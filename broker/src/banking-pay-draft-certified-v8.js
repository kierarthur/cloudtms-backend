// PostgreSQL's uuid type does not impose an RFC version/variant restriction.
// Match that existing database contract instead of adding a new Worker policy.
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_RE = /^[0-9a-f]{64}$/;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export const BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_INPUT_KEY =
  'workbench_settled_certificate_reference_v8';

export const BANKING_PAY_DRAFT_CERTIFIED_INPUT_CONTRACT =
  'BANKING_PAY_DRAFT_CERTIFIED_OPERATION_INPUT_V8';

export const BANKING_PAY_DRAFT_CERTIFIED_OPERATION_PROJECTION_CONTRACT =
  'WORKBENCH_SETTLED_CERTIFICATE_OPERATION_PROJECTION_V1';

const CERTIFICATE_REFERENCE_KEYS = Object.freeze([
  'candidate_filter_id',
  'certification_id',
  'client_filter_id',
  'filter_context_digest_sha256',
  'idempotency_key',
  'manifest_digest_sha256',
  'overall_digest_sha256',
  'pay_channel_scope',
  'same_week_paye_override'
]);

const PRE_ADMISSION_SCOPE_FACT_KEYS = Object.freeze([
  'authority_fence_generation',
  'certificate_uuid',
  'certification_id',
  'manifest_digest_sha256',
  'overall_digest_sha256',
  'pay_channel_scope',
  'pay_date',
  'pay_week_end',
  'pay_week_start',
  'progress_counter_version',
  'scope_facts_contract',
  'selected_ready_for_request',
  'selected_ready_paye',
  'selected_ready_total',
  'selected_ready_umbrella',
  'session_version',
  'week_ending_cutoff',
  'workbench_session_id'
]);

const SAME_WEEK_OVERRIDE_KEYS = Object.freeze([
  'continue',
  'guardrail_code',
  'pay_date',
  'pay_week_end',
  'pay_week_start',
  'reason',
  'reauth_purpose',
  'used',
  'verified',
  'verified_at_utc',
  'verified_by_user_id'
]);

const EXPANDED_SELECTION_KEYS = new Set([
  'all_selected_preview_row_ids',
  'draft_selected_economic_keys',
  'draft_selected_preview_row_contracts',
  'draft_selected_preview_row_ids',
  'expected_workbench_selected_preview_row_ids',
  'scoped_selected_economic_keys',
  'scoped_selected_preview_row_contracts',
  'scoped_selected_preview_row_ids',
  'selected_economic_keys',
  'selected_preview_row_contracts',
  'selected_preview_row_ids'
]);

function isPlainObject(value) {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function exactKeys(value, expected) {
  if (!isPlainObject(value)) return false;
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  return actual.length === wanted.length
    && actual.every((key, index) => key === wanted[index]);
}

function stringValue(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function integerInRange(value, minimum, maximum) {
  return Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function isoDateToUtcMs(value) {
  if (!DATE_RE.test(value)) return NaN;
  const parsed = Date.parse(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed)
      && new Date(parsed).toISOString().slice(0, 10) === value
    ? parsed
    : NaN;
}

function scopeCount(facts, scope) {
  if (scope === 'PAYE') return facts.selected_ready_paye;
  if (scope === 'UMBRELLA') return facts.selected_ready_umbrella;
  return facts.selected_ready_total;
}

function fail(code, detail = {}) {
  return { ok: false, code, ...detail };
}

export function findExpandedSelectionKeys(value) {
  const found = new Set();
  const seen = new Set();
  const visit = (node) => {
    if (!node || typeof node !== 'object' || seen.has(node)) return;
    seen.add(node);
    if (Array.isArray(node)) {
      node.forEach(visit);
      return;
    }
    for (const [key, child] of Object.entries(node)) {
      if (EXPANDED_SELECTION_KEYS.has(key)) found.add(key);
      visit(child);
    }
  };
  visit(value);
  return [...found].sort();
}

export function validateCurrentCertificateIssuerEnvelopeV8(value) {
  if (!exactKeys(value, ['certificate_reference', 'pre_admission_scope_facts'])) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_ISSUER_ENVELOPE_INVALID');
  }

  const reference = value.certificate_reference;
  const facts = value.pre_admission_scope_facts;
  if (!exactKeys(reference, CERTIFICATE_REFERENCE_KEYS)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_SHAPE_INVALID');
  }
  if (!exactKeys(facts, PRE_ADMISSION_SCOPE_FACT_KEYS)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_SCOPE_FACTS_SHAPE_INVALID');
  }

  const certificationId = stringValue(reference.certification_id);
  const overallDigest = stringValue(reference.overall_digest_sha256).toLowerCase();
  const manifestDigest = stringValue(reference.manifest_digest_sha256).toLowerCase();
  const filterDigest = stringValue(reference.filter_context_digest_sha256).toLowerCase();
  const idempotencyKey = stringValue(reference.idempotency_key);
  const scope = stringValue(reference.pay_channel_scope).toUpperCase();

  if (!SHA256_RE.test(overallDigest)
      || certificationId !== `WORKBENCH_SETTLED_CERTIFICATION_V2:${overallDigest}`
      || !SHA256_RE.test(manifestDigest)
      || !SHA256_RE.test(filterDigest)
      || !idempotencyKey
      || idempotencyKey.length > 200
      || !['ALL', 'PAYE', 'UMBRELLA'].includes(scope)
      || !isPlainObject(reference.same_week_paye_override)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_INVALID');
  }

  const override = reference.same_week_paye_override;
  if (!exactKeys(override, SAME_WEEK_OVERRIDE_KEYS)
      || typeof override.continue !== 'boolean'
      || typeof override.verified !== 'boolean'
      || typeof override.used !== 'boolean') {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_OVERRIDE_CONTEXT_INVALID');
  }

  if (reference.candidate_filter_id !== null
      && !UUID_RE.test(stringValue(reference.candidate_filter_id))) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_CANDIDATE_FILTER_INVALID');
  }
  if (reference.client_filter_id !== null
      && !UUID_RE.test(stringValue(reference.client_filter_id))) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_CLIENT_FILTER_INVALID');
  }

  const certificateUuid = stringValue(facts.certificate_uuid).toLowerCase();
  const sessionId = stringValue(facts.workbench_session_id).toLowerCase();
  if (!UUID_RE.test(certificateUuid) || !UUID_RE.test(sessionId)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_AUTHORITY_IDENTITY_INVALID');
  }
  if (facts.scope_facts_contract !== 'WORKBENCH_SETTLED_CERTIFICATE_PRE_ADMISSION_SCOPE_FACTS_V1'
      || facts.certification_id !== certificationId
      || stringValue(facts.overall_digest_sha256).toLowerCase() !== overallDigest
      || stringValue(facts.manifest_digest_sha256).toLowerCase() !== manifestDigest
      || stringValue(facts.pay_channel_scope).toUpperCase() !== scope) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_ISSUER_CROSS_BINDING_MISMATCH');
  }

  for (const key of [
    'selected_ready_total',
    'selected_ready_for_request',
    'selected_ready_paye',
    'selected_ready_umbrella'
  ]) {
    if (!integerInRange(facts[key], 0, 50000)) {
      return fail('BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_INVALID', { field: key });
    }
  }
  if (facts.selected_ready_total !== facts.selected_ready_paye + facts.selected_ready_umbrella
      || facts.selected_ready_for_request !== scopeCount(facts, scope)
      || facts.selected_ready_for_request < 1) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_MISMATCH');
  }
  if (!integerInRange(facts.session_version, 1, Number.MAX_SAFE_INTEGER)
      || !integerInRange(facts.progress_counter_version, 1, Number.MAX_SAFE_INTEGER)
      || !integerInRange(facts.authority_fence_generation, 0, Number.MAX_SAFE_INTEGER)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_SESSION_VERSION_INVALID');
  }

  const payDate = stringValue(facts.pay_date);
  const weekStart = stringValue(facts.pay_week_start);
  const weekEnd = stringValue(facts.pay_week_end);
  const cutoff = stringValue(facts.week_ending_cutoff);
  const payDateMs = isoDateToUtcMs(payDate);
  const weekStartMs = isoDateToUtcMs(weekStart);
  const weekEndMs = isoDateToUtcMs(weekEnd);
  const cutoffMs = isoDateToUtcMs(cutoff);
  if (![payDateMs, weekStartMs, weekEndMs, cutoffMs].every(Number.isFinite)
      || weekEndMs - weekStartMs !== 6 * 86400000
      || new Date(weekStartMs).getUTCDay() !== 1) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_PAY_PERIOD_INVALID');
  }

  if (stringValue(override.pay_date) !== payDate
      || stringValue(override.pay_week_start) !== weekStart
      || stringValue(override.pay_week_end) !== weekEnd) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_OVERRIDE_CONTEXT_INVALID');
  }
  if (override.used === false) {
    if (override.continue !== false
        || override.verified !== false
        || override.reason !== null
        || override.verified_by_user_id !== null
        || override.verified_at_utc !== null
        || override.reauth_purpose !== null
        || override.guardrail_code !== null) {
      return fail('BANKING_PAY_DRAFT_CERTIFIED_OVERRIDE_CONTEXT_INVALID');
    }
  } else if (override.continue !== true
      || override.verified !== true
      || !stringValue(override.reason)
      || stringValue(override.reason).length > 2000
      || !UUID_RE.test(stringValue(override.verified_by_user_id))
      || !Number.isFinite(Date.parse(stringValue(override.verified_at_utc)))
      || override.reauth_purpose !== 'PAYE_SAME_WEEK_OVERRIDE'
      || override.guardrail_code !== 'PAYE_SAME_WEEK_OVERRIDE_REQUIRED') {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_OVERRIDE_CONTEXT_INVALID');
  }

  const expandedKeys = findExpandedSelectionKeys(value);
  if (expandedKeys.length > 0) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_EXPANDED_SELECTION_FORBIDDEN', {
      forbidden_keys: expandedKeys
    });
  }

  return {
    ok: true,
    certificate_reference: reference,
    pre_admission_scope_facts: facts,
    certificate_uuid: certificateUuid,
    workbench_session_id: sessionId,
    certification_id: certificationId,
    overall_digest_sha256: overallDigest,
    manifest_digest_sha256: manifestDigest,
    pay_channel_scope: scope,
    selected_ready_for_request: facts.selected_ready_for_request,
    pay_date: payDate,
    week_ending_cutoff: cutoff,
    session_version: facts.session_version,
    progress_counter_version: facts.progress_counter_version,
    authority_fence_generation: facts.authority_fence_generation,
    idempotency_key: idempotencyKey
  };
}

export function buildCertifiedDraftOperationInputV8(validatedEnvelope) {
  if (!validatedEnvelope || validatedEnvelope.ok !== true) {
    throw new TypeError('A validated Workbench certificate issuer envelope is required.');
  }
  const input = {
    certified_draft_contract: BANKING_PAY_DRAFT_CERTIFIED_INPUT_CONTRACT,
    [BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_INPUT_KEY]: validatedEnvelope.certificate_reference,
    certificate_uuid: validatedEnvelope.certificate_uuid,
    workbench_session_id: validatedEnvelope.workbench_session_id,
    session_id: validatedEnvelope.workbench_session_id,
    session_version: validatedEnvelope.session_version,
    progress_counter_version: validatedEnvelope.progress_counter_version,
    authority_fence_generation: validatedEnvelope.authority_fence_generation,
    pay_channel_scope: validatedEnvelope.pay_channel_scope,
    draft_scope: validatedEnvelope.pay_channel_scope,
    pay_date: validatedEnvelope.pay_date,
    week_ending_cutoff: validatedEnvelope.week_ending_cutoff,
    selected_rows_total: validatedEnvelope.selected_ready_for_request,
    selected_preview_row_count: validatedEnvelope.selected_ready_for_request,
    certificate_manifest_digest_sha256: validatedEnvelope.manifest_digest_sha256,
    policy_x_authority: 'PRE_DRAFT_CERTIFICATE_TO_POST_DRAFT_FROZEN_ARTEFACTS',
    backend_runner_owned: true,
    frontend_completion_required: false
  };
  const forbidden = findExpandedSelectionKeys(input);
  if (forbidden.length > 0) {
    throw new TypeError(`Expanded Create Draft selection is forbidden: ${forbidden.join(', ')}`);
  }
  return input;
}

export function isCertifiedDraftOperationInputV8(input) {
  if (!isPlainObject(input)
      || input.certified_draft_contract !== BANKING_PAY_DRAFT_CERTIFIED_INPUT_CONTRACT
      || !isPlainObject(input[BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_INPUT_KEY])) {
    return false;
  }
  return findExpandedSelectionKeys(input).length === 0;
}

// The database admission owner deliberately stores a smaller projection than
// buildCertifiedDraftOperationInputV8.  It is the persisted operation shape
// consumed by the queue runner and must be recognised without manufacturing a
// second selection/economic authority in the Worker.
export function validateCertifiedDraftOperationProjectionV8(input) {
  if (!isPlainObject(input)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_OPERATION_PROJECTION_INVALID');
  }
  const reference = input[BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_INPUT_KEY];
  if (!exactKeys(reference, CERTIFICATE_REFERENCE_KEYS)) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_REFERENCE_SHAPE_INVALID');
  }
  if (findExpandedSelectionKeys(input).length > 0) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_EXPANDED_SELECTION_FORBIDDEN');
  }

  const scope = stringValue(reference.pay_channel_scope).toUpperCase();
  const certificationId = stringValue(reference.certification_id);
  const overallDigest = stringValue(reference.overall_digest_sha256).toLowerCase();
  const manifestDigest = stringValue(reference.manifest_digest_sha256).toLowerCase();
  const filterDigest = stringValue(reference.filter_context_digest_sha256).toLowerCase();
  const idempotencyKey = stringValue(reference.idempotency_key);
  const railProviderSnapshot = stringValue(input.rail_provider_snapshot).toUpperCase();
  const railEnvSnapshot = stringValue(input.rail_env_snapshot).toUpperCase();
  if (!['ALL', 'PAYE', 'UMBRELLA'].includes(scope)
      || stringValue(input.pay_channel_scope).toUpperCase() !== scope
      || stringValue(input.draft_scope).toUpperCase() !== scope
      || !railProviderSnapshot
      || !railEnvSnapshot
      || railProviderSnapshot.length > 100
      || railEnvSnapshot.length > 100
      || !SHA256_RE.test(overallDigest)
      || certificationId !== `WORKBENCH_SETTLED_CERTIFICATION_V2:${overallDigest}`
      || !SHA256_RE.test(manifestDigest)
      || !SHA256_RE.test(filterDigest)
      || !idempotencyKey
      || idempotencyKey.length > 200) {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_OPERATION_PROJECTION_MISMATCH');
  }

  const override = reference.same_week_paye_override;
  const projectedOverride = input.same_week_paye_override;
  if (!exactKeys(override, SAME_WEEK_OVERRIDE_KEYS)
      || !exactKeys(projectedOverride, SAME_WEEK_OVERRIDE_KEYS)
      || SAME_WEEK_OVERRIDE_KEYS.some((key) => projectedOverride[key] !== override[key])
      || typeof override.continue !== 'boolean'
      || typeof override.verified !== 'boolean'
      || typeof override.used !== 'boolean') {
    return fail('BANKING_PAY_DRAFT_CERTIFIED_OVERRIDE_CONTEXT_INVALID');
  }

  return {
    ok: true,
    certificate_reference: reference,
    certification_id: certificationId,
    overall_digest_sha256: overallDigest,
    manifest_digest_sha256: manifestDigest,
    pay_channel_scope: scope,
    same_week_paye_override: override,
    pay_date: stringValue(override.pay_date),
    pay_week_start: stringValue(override.pay_week_start),
    pay_week_end: stringValue(override.pay_week_end),
    rail_provider_snapshot: railProviderSnapshot,
    rail_env_snapshot: railEnvSnapshot,
    idempotency_key: idempotencyKey
  };
}

export function isCertifiedDraftOperationProjectionV8(input) {
  return validateCertifiedDraftOperationProjectionV8(input).ok === true;
}

function safeObject(value) {
  return isPlainObject(value) ? value : {};
}

function uniqueUuidValues(values) {
  const result = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const uuid = stringValue(value).toLowerCase();
    if (!UUID_RE.test(uuid) || seen.has(uuid)) continue;
    seen.add(uuid);
    result.push(uuid);
  }
  return result;
}

function patchNestedResult(value) {
  const patch = safeObject(value);
  return safeObject(
    patch.post_action_refresh || patch.postActionRefresh || patch.refresh
      || patch.post_create_refresh || patch.postCreateRefresh
  );
}

function sumPatchCount(patchResults, key) {
  return patchResults.reduce((sum, value) => {
    const patch = safeObject(value);
    const nested = patchNestedResult(patch);
    const direct = Number(patch[key]);
    const nestedValue = Number(nested[key]);
    if (Number.isFinite(direct)) return sum + Math.max(0, Math.trunc(direct));
    if (Number.isFinite(nestedValue)) return sum + Math.max(0, Math.trunc(nestedValue));
    return sum;
  }, 0);
}

function collectPatchUuidValues(patchResults, ...keys) {
  const values = [];
  for (const value of patchResults) {
    const patch = safeObject(value);
    const nested = patchNestedResult(patch);
    for (const key of keys) {
      if (Array.isArray(patch[key])) values.push(...patch[key]);
      if (Array.isArray(nested[key])) values.push(...nested[key]);
    }
  }
  return uniqueUuidValues(values);
}

function collectPatchEconomicKeys(patchResults) {
  const result = [];
  const seen = new Set();
  for (const value of patchResults) {
    const patch = safeObject(value);
    const nested = patchNestedResult(patch);
    for (const key of ['affected_economic_keys', 'affectedEconomicKeys', 'economic_keys', 'economicKeys']) {
      for (const economicKey of [
        ...(Array.isArray(patch[key]) ? patch[key] : []),
        ...(Array.isArray(nested[key]) ? nested[key] : [])
      ]) {
        let identity = '';
        try { identity = JSON.stringify(economicKey); } catch { identity = String(economicKey || ''); }
        if (!identity || seen.has(identity)) continue;
        seen.add(identity);
        result.push(economicKey);
      }
    }
  }
  return result;
}

export function validateCertifiedDraftTerminalContextV8(value) {
  if (!isPlainObject(value)
      || value.contract !== 'BANKING_PAY_DRAFT_TERMINAL_CONTEXT_V8'
      || value.ok !== true) {
    return fail('BANKING_PAY_DRAFT_TERMINAL_CONTEXT_INVALID');
  }
  const operationId = stringValue(value.operation_id).toLowerCase();
  const workbenchSessionId = stringValue(value.workbench_session_id).toLowerCase();
  const sourceSessionId = stringValue(value.source_session_id).toLowerCase();
  const sourceSnapshotRunId = stringValue(value.source_snapshot_run_id).toLowerCase();
  const sourceSessionSignature = stringValue(value.source_session_signature);
  const batchIds = uniqueUuidValues(value.pay_batch_ids);
  const createdBatchIds = uniqueUuidValues(value.created_pay_batch_ids);
  const skippedBatchIds = uniqueUuidValues(value.skipped_empty_pay_batch_ids);
  const cancelledBatchIds = uniqueUuidValues(value.cancelled_empty_pay_batch_ids);
  const createdBatches = Array.isArray(value.created_batches) ? value.created_batches : [];
  const patchResults = Array.isArray(value.patch_results) ? value.patch_results : [];
  const sourceSessionVersion = Number(value.source_session_version);
  const candidateCount = Number(value.candidate_count);
  if (!UUID_RE.test(operationId)
      || !UUID_RE.test(workbenchSessionId)
      || sourceSessionId !== workbenchSessionId
      || !UUID_RE.test(sourceSnapshotRunId)
      || !sourceSessionSignature
      || !integerInRange(sourceSessionVersion, 1, Number.MAX_SAFE_INTEGER)
      || !integerInRange(candidateCount, 1, 50000)
      || batchIds.length < 1
      || batchIds.length > 2
      || JSON.stringify(batchIds) !== JSON.stringify(createdBatchIds)
      || createdBatches.length !== batchIds.length
      || patchResults.length !== batchIds.length
      || !isPlainObject(value.reservation_availability)
      || !stringValue(value.replacement_idempotency_key)) {
    return fail('BANKING_PAY_DRAFT_TERMINAL_CONTEXT_MISMATCH');
  }
  for (let index = 0; index < createdBatches.length; index += 1) {
    const batch = safeObject(createdBatches[index]);
    if (stringValue(batch.pay_batch_id).toLowerCase() !== batchIds[index]
        || !['PAYE', 'UMBRELLA'].includes(stringValue(batch.pay_channel).toUpperCase())) {
      return fail('BANKING_PAY_DRAFT_TERMINAL_BATCH_CONTEXT_MISMATCH');
    }
  }
  return {
    ok: true,
    ...value,
    operation_id: operationId,
    workbench_session_id: workbenchSessionId,
    source_session_id: sourceSessionId,
    source_snapshot_run_id: sourceSnapshotRunId,
    source_session_signature: sourceSessionSignature,
    source_session_version: sourceSessionVersion,
    candidate_count: candidateCount,
    pay_batch_ids: batchIds,
    created_pay_batch_ids: createdBatchIds,
    skipped_empty_pay_batch_ids: skippedBatchIds,
    cancelled_empty_pay_batch_ids: cancelledBatchIds,
    created_batches: createdBatches.map((batch) => ({
      pay_batch_id: stringValue(batch.pay_batch_id).toLowerCase(),
      pay_channel: stringValue(batch.pay_channel).toUpperCase()
    })),
    patch_results: patchResults,
    replacement_session_required: value.replacement_session_required === true
  };
}

export function buildCertifiedDraftTerminalResultV8(
  validatedContext,
  replacementResult = null,
  workerWake = null
) {
  if (!validatedContext || validatedContext.ok !== true) {
    throw new TypeError('A validated certified Draft terminal context is required.');
  }
  const context = validatedContext;
  const patchResults = context.patch_results;
  const affectedCandidateIds = collectPatchUuidValues(
    patchResults, 'affected_candidate_ids', 'affectedCandidateIds', 'candidate_ids', 'candidateIds'
  );
  const patchedRowIds = collectPatchUuidValues(patchResults, 'patched_row_ids', 'patchedRowIds');
  const affectedRowIds = collectPatchUuidValues(
    patchResults, 'affected_row_ids', 'affectedRowIds', 'row_ids', 'rowIds'
  );
  const affectedTimesheetIds = collectPatchUuidValues(
    patchResults, 'affected_timesheet_ids', 'affectedTimesheetIds', 'timesheet_ids', 'timesheetIds'
  );
  const complexCandidateIds = collectPatchUuidValues(
    patchResults, 'complex_refresh_candidate_ids', 'complexRefreshCandidateIds'
  );
  const targetedCandidateIds = collectPatchUuidValues(
    patchResults, 'targeted_refresh_candidate_ids', 'targetedRefreshCandidateIds',
    'refresh_candidate_ids', 'refreshCandidateIds',
    'complex_refresh_candidate_ids', 'complexRefreshCandidateIds'
  );
  const targetedCount = Math.max(sumPatchCount(patchResults, 'targeted_refresh_enqueued_count'), targetedCandidateIds.length);
  const affectedCandidateCount = Math.max(sumPatchCount(patchResults, 'affected_candidate_count'), affectedCandidateIds.length);
  const affectedRowCount = Math.max(sumPatchCount(patchResults, 'affected_row_count'), affectedRowIds.length, patchedRowIds.length);
  const patchedRowCount = Math.max(sumPatchCount(patchResults, 'patched_row_count'), patchedRowIds.length);
  const targeted = targetedCount > 0 || patchResults.some((value) => {
    const patch = safeObject(value);
    const nested = patchNestedResult(patch);
    return patch.targeted_refresh_enqueued === true || nested.targeted_refresh_enqueued === true;
  });
  const wake = isPlainObject(workerWake) ? workerWake : null;
  const batchIds = context.created_pay_batch_ids;
  const reservationAvailability = context.reservation_availability;

  const baseRefresh = {
    ok: true,
    mode: 'PATCH_EXISTING_SESSION',
    patch_applied: true,
    operation_type: 'DRAFT_CREATE',
    operation_id: context.operation_id,
    session_id: context.workbench_session_id,
    source_session_id: context.workbench_session_id,
    current_workbench_session_id: context.workbench_session_id,
    refreshed_session_id: context.workbench_session_id,
    source_session_version: context.source_session_version,
    source_snapshot_run_id: context.source_snapshot_run_id,
    source_session_signature: context.source_session_signature,
    replacement_session_required: context.replacement_session_required,
    replacement_available: false,
    replacement_session_id: null,
    replacement_session_version: null,
    source_session_discarded: false,
    source_session_obsolete: false,
    requires_new_session: false,
    preview_reopen_required: false,
    targeted_refresh_enqueued: targeted,
    targeted_refresh_enqueued_count: targetedCount,
    affected_candidate_count: affectedCandidateCount,
    affected_candidate_ids: affectedCandidateIds,
    affected_row_count: affectedRowCount,
    affected_row_ids: affectedRowIds,
    patched_row_count: patchedRowCount,
    patched_row_ids: patchedRowIds,
    affected_timesheet_ids: affectedTimesheetIds,
    affected_economic_keys: collectPatchEconomicKeys(patchResults),
    targeted_refresh_candidate_ids: targetedCandidateIds,
    complex_refresh_candidate_ids: complexCandidateIds,
    pay_batch_ids: batchIds,
    created_pay_batch_ids: batchIds,
    patch_results: patchResults,
    reservation_availability: reservationAvailability,
    draft_availability: reservationAvailability,
    skipped_preview_row_count: reservationAvailability.skipped_preview_row_count,
    clipped_preview_row_count: reservationAvailability.clipped_preview_row_count
  };

  let postCreateRefresh;
  let terminalSession;
  let replacementFields;
  if (context.replacement_session_required) {
    const replacement = safeObject(replacementResult);
    const replacementSession = safeObject(replacement.replacement_session);
    const replacementSessionId = stringValue(
      replacement.replacement_session_id || replacement.session_id
        || replacementSession.session_id || replacementSession.id
    ).toLowerCase();
    const replacementVersion = Number(
      replacement.replacement_session_version ?? replacement.session_version
        ?? replacementSession.session_version ?? replacementSession.version
    );
    const replacementSignature = stringValue(
      replacement.replacement_session_signature || replacement.session_signature
        || replacementSession.session_signature
    ) || null;
    const replacementSnapshotRunId = stringValue(
      replacement.replacement_snapshot_run_id || replacement.snapshot_run_id
        || replacement.source_snapshot_run_id || replacementSession.snapshot_run_id
        || replacementSession.source_snapshot_run_id
    ).toLowerCase() || null;
    if (replacement.ok === false
        || !UUID_RE.test(replacementSessionId)
        || !integerInRange(replacementVersion, 1, Number.MAX_SAFE_INTEGER)
        || (replacementSnapshotRunId !== null && !UUID_RE.test(replacementSnapshotRunId))) {
      throw new TypeError('The replacement Workbench session result is invalid.');
    }
    const adoption = {
      ok: true,
      action: 'ADOPT_REPLACEMENT_SESSION',
      next_recommended_action: 'ADOPT_REPLACEMENT_SESSION',
      source_session_id: context.workbench_session_id,
      source_session_version: replacement.source_session_version ?? context.source_session_version,
      source_session_signature: context.source_session_signature,
      source_session_discarded: true,
      source_session_obsolete: true,
      replacement_available: true,
      replacement_session_id: replacementSessionId,
      replacement_session_version: replacementVersion,
      replacement_session_signature: replacementSignature,
      replacement_snapshot_run_id: replacementSnapshotRunId,
      replacement_idempotency_key: replacement.replacement_idempotency_key || context.replacement_idempotency_key,
      replacement_pay_date: stringValue(replacement.replacement_pay_date || replacement.pay_date || replacementSession.pay_date) || null,
      replacement_week_ending_cutoff: stringValue(
        replacement.replacement_week_ending_cutoff || replacement.week_ending_cutoff
          || replacement.week_ending_cutoff_date || replacementSession.week_ending_cutoff
          || replacementSession.week_ending_cutoff_date
      ) || null,
      current_workbench_session_id: replacementSessionId,
      refreshed_session_id: replacementSessionId,
      refreshed_session_signature: replacementSignature,
      refreshed_session_version: replacementVersion,
      preview_reopen_required: false,
      requires_new_session: false
    };
    postCreateRefresh = {
      ...baseRefresh,
      source_session_version: replacement.source_session_version ?? context.source_session_version,
      source_session_discarded: true,
      source_session_obsolete: true,
      source_discarded_at_utc: replacement.source_discarded_at_utc || null,
      replacement_available: true,
      replacement_session_id: replacementSessionId,
      replacement_session_version: replacementVersion,
      replacement_session_signature: replacementSignature,
      replacement_snapshot_run_id: replacementSnapshotRunId,
      replacement_pay_date: adoption.replacement_pay_date,
      replacement_week_ending_cutoff: adoption.replacement_week_ending_cutoff,
      replacement_session_status: replacement.replacement_session_status || replacementSession.status || null,
      replacement_progress_state: replacement.replacement_progress_state || replacementSession.progress_state || null,
      replacement_created: replacement.replacement_created === true,
      replacement_reused: replacement.replacement_reused === true || replacement.idempotency_reused === true,
      idempotency_reused: replacement.idempotency_reused === true,
      replacement_idempotency_key: adoption.replacement_idempotency_key,
      replacement_session: Object.keys(replacementSession).length ? replacementSession : replacement,
      replacement_adoption_contract: adoption,
      adoption_contract: adoption,
      next_recommended_action: 'ADOPT_REPLACEMENT_SESSION',
      action: 'ADOPT_REPLACEMENT_SESSION',
      adopt_replacement_session: true,
      current_workbench_session_id: replacementSessionId,
      refreshed_session_id: replacementSessionId,
      refreshed_session_signature: replacementSignature,
      refreshed_session_version: replacementVersion,
      work_queued: replacement.work_queued === true,
      old_rows_retained: replacement.old_rows_retained !== false,
      atomic_replacement: replacement.atomic_replacement !== false,
      dirty_candidate_ids: [],
      refresh_job_ids: [],
      snapshot_refresh_job_ids: [],
      skipped_empty_pay_batch_ids: context.skipped_empty_pay_batch_ids,
      cancelled_empty_pay_batch_ids: context.cancelled_empty_pay_batch_ids,
      worker_wake: wake,
      worker_wake_scheduled: wake?.scheduled === true || wake?.already_running === true,
      worker_wake_session_scoped: stringValue(wake?.worker_scope).toUpperCase() === 'SESSION' || wake?.session_scoped === true,
      worker_wake_wait_until_available: wake?.wait_until_available === true || wake?.wait_until_used === true,
      scheduled_worker_is_durable_fallback: true,
      warning: null
    };
    terminalSession = {
      session_id: replacementSessionId,
      snapshot_run_id: replacementSnapshotRunId,
      source_snapshot_run_id: replacementSnapshotRunId,
      session_version: replacementVersion,
      session_signature: replacementSignature,
      source_session_id: context.workbench_session_id,
      source_session_version: postCreateRefresh.source_session_version,
      source_session_signature: context.source_session_signature,
      source_session_discarded: true,
      source_session_obsolete: true,
      requires_new_session: false,
      replacement_available: true,
      replacement_session_id: replacementSessionId,
      replacement_session_signature: replacementSignature,
      replacement_session_version: replacementVersion,
      replacement_adoption_contract: adoption,
      adoption_contract: adoption,
      next_recommended_action: 'ADOPT_REPLACEMENT_SESSION',
      action: 'ADOPT_REPLACEMENT_SESSION',
      adopted_replacement_session: true,
      consumed: false,
      preview_reopen_required: false
    };
    replacementFields = {
      source_session_discarded: true,
      source_session_obsolete: true,
      replacement_available: true,
      replacement_session_id: replacementSessionId,
      replacement_session_signature: replacementSignature,
      replacement_session_version: replacementVersion,
      replacement_snapshot_run_id: replacementSnapshotRunId,
      replacement_idempotency_key: adoption.replacement_idempotency_key,
      replacement_session: postCreateRefresh.replacement_session,
      replacement_adoption_contract: adoption,
      adoption_contract: adoption,
      next_recommended_action: 'ADOPT_REPLACEMENT_SESSION',
      action: 'ADOPT_REPLACEMENT_SESSION',
      adopt_replacement_session: true,
      current_workbench_session_id: replacementSessionId,
      refreshed_session_id: replacementSessionId,
      refreshed_session_signature: replacementSignature,
      refreshed_session_version: replacementVersion,
      snapshot_run_id: replacementSnapshotRunId,
      session_signature: replacementSignature,
      session_version: replacementVersion,
      dirty_candidate_ids: [],
      refresh_candidate_ids: []
    };
  } else {
    postCreateRefresh = {
      ...baseRefresh,
      target_selected_after_patch: false,
      replacement_session_required: false,
      replacement_available: false,
      replacement_created: false,
      replacement_reused: false,
      replacement_session_id: null,
      replacement_session_version: null,
      replacement_session_signature: null,
      replacement_snapshot_run_id: null,
      action: targeted ? 'WAIT_FOR_WORKER' : 'PATCH_EXISTING_SESSION',
      next_recommended_action: targeted ? 'WAIT_FOR_WORKER' : null,
      warning: null,
      skipped_empty_pay_batch_ids: context.skipped_empty_pay_batch_ids,
      cancelled_empty_pay_batch_ids: context.cancelled_empty_pay_batch_ids
    };
    if (wake) {
      postCreateRefresh.worker_wake = wake;
      postCreateRefresh.worker_wake_scheduled = wake.scheduled === true || wake.already_running === true;
      postCreateRefresh.worker_wake_session_scoped = stringValue(wake.worker_scope).toUpperCase() === 'SESSION' || wake.session_scoped === true;
      postCreateRefresh.worker_wake_wait_until_available = wake.wait_until_available === true || wake.wait_until_used === true;
      postCreateRefresh.scheduled_worker_is_durable_fallback = true;
    }
    terminalSession = {
      session_id: context.workbench_session_id,
      snapshot_run_id: context.source_snapshot_run_id,
      source_snapshot_run_id: context.source_snapshot_run_id,
      session_version: context.source_session_version,
      session_signature: context.source_session_signature,
      source_session_id: context.workbench_session_id,
      source_session_version: context.source_session_version,
      source_session_signature: context.source_session_signature,
      source_session_discarded: false,
      source_session_obsolete: false,
      requires_new_session: false,
      replacement_available: false,
      replacement_session_id: null,
      replacement_session_signature: null,
      replacement_session_version: null,
      replacement_adoption_contract: null,
      adoption_contract: null,
      next_recommended_action: postCreateRefresh.next_recommended_action,
      action: postCreateRefresh.action,
      adopted_replacement_session: false,
      consumed: false,
      preview_reopen_required: false
    };
    replacementFields = {
      source_session_discarded: false,
      source_session_obsolete: false,
      replacement_available: false,
      replacement_session_id: null,
      replacement_session_signature: null,
      replacement_session_version: null,
      replacement_snapshot_run_id: null,
      replacement_idempotency_key: null,
      replacement_session: null,
      replacement_adoption_contract: null,
      adoption_contract: null,
      next_recommended_action: postCreateRefresh.next_recommended_action,
      action: postCreateRefresh.action,
      adopt_replacement_session: false,
      current_workbench_session_id: context.workbench_session_id,
      refreshed_session_id: context.workbench_session_id,
      refreshed_session_signature: context.source_session_signature,
      refreshed_session_version: context.source_session_version,
      snapshot_run_id: context.source_snapshot_run_id,
      session_signature: context.source_session_signature,
      session_version: context.source_session_version,
      dirty_candidate_ids: complexCandidateIds.length ? complexCandidateIds : targetedCandidateIds,
      refresh_candidate_ids: targetedCandidateIds.length ? targetedCandidateIds : complexCandidateIds
    };
  }

  return {
    ok: true,
    operation_id: context.operation_id,
    workbench_session_id: context.workbench_session_id,
    session_id: context.workbench_session_id,
    source_session_id: context.workbench_session_id,
    source_snapshot_run_id: context.source_snapshot_run_id,
    source_session_version: context.source_session_version,
    source_session_signature: context.source_session_signature,
    requires_new_session: false,
    ...replacementFields,
    pay_batch_ids: batchIds,
    created_pay_batch_ids: batchIds,
    skipped_empty_pay_batch_ids: context.skipped_empty_pay_batch_ids,
    cancelled_empty_pay_batch_ids: context.cancelled_empty_pay_batch_ids,
    operation_type: 'DRAFT_CREATE',
    primary_pay_batch_id: batchIds[0],
    pay_batch_id: batchIds[0],
    created_batch_count: batchIds.length,
    backend_runner_owned: true,
    frontend_completion_required: false,
    paye_pay_batch_id: context.paye_pay_batch_id || null,
    umbrella_pay_batch_id: context.umbrella_pay_batch_id || null,
    created_batches: context.created_batches,
    reservation_availability: reservationAvailability,
    draft_availability: reservationAvailability,
    skipped_preview_row_count: reservationAvailability.skipped_preview_row_count,
    clipped_preview_row_count: reservationAvailability.clipped_preview_row_count,
    candidate_count: context.candidate_count,
    refresh_job_ids: [],
    snapshot_refresh_job_ids: [],
    post_action_refresh: context.replacement_session_required ? undefined : postCreateRefresh,
    post_create_refresh: postCreateRefresh,
    refresh: postCreateRefresh,
    preview_refresh_warning: null,
    worker_wake: postCreateRefresh.worker_wake || null,
    worker_wake_scheduled: postCreateRefresh.worker_wake_scheduled === true,
    session: terminalSession
  };
}
