const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/i;

const INITIAL_PREVIEW_REQUEST_KEYS = new Set([
  'actor_user_id',
  'authority_scope_kind',
  'expected_current_final_revision_id',
  'expected_final_manifest_hash',
  'idempotency_key',
  'replacement_source',
  'report_scope_id',
  'source_cycle_id',
]);

const RECHECK_PREVIEW_REQUEST_KEYS = new Set([
  'actor_user_id',
  'correction_context',
  'idempotency_key',
]);

const APPLY_REQUEST_KEYS = new Set([
  'actor_user_id',
  'confirmation_text',
  'correction_context',
  'idempotency_key',
  'reason',
]);

const CORRECTION_CONTEXT_KEYS = new Set([
  'authority_scope_kind',
  'correction_session_id',
  'expected_authority_scope_version',
  'expected_comparison_manifest_hash',
  'expected_current_final_revision_id',
  'expected_final_manifest_hash',
  'expected_issue_set_hash',
  'expected_preview_hash',
  'expected_row_manifest_hash',
  'expected_session_version',
  'review_expected_session_version',
  'review_idempotency_key',
  'replacement_projection_publication_id',
  'replacement_upload_id',
  'report_scope_id',
  'source_cycle_id',
]);

const SERVICE_SNAPSHOT_KEYS = Object.freeze([
  'calculator_owner',
  'schema_version',
  'source_actual_schedule_json',
  'tsfin_snapshot_json',
]);

export class WeeklyCorrectFinalSourceError extends Error {
  constructor(code, message, status = 409, details = {}) {
    super(message);
    this.name = 'WeeklyCorrectFinalSourceError';
    this.code = code;
    this.status = status;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, status = 409, details = {}) {
  throw new WeeklyCorrectFinalSourceError(code, message, status, details);
}

function plainObject(value, code, label, status = 400) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(code, `${label} is invalid.`, status);
  }
  return value;
}

function text(value, code, label, status = 400) {
  const result = String(value ?? '').trim();
  if (!result) fail(code, `${label} is invalid.`, status);
  return result;
}

function uuid(value, code, label, status = 400) {
  const result = text(value, code, label, status).toLowerCase();
  if (!UUID_PATTERN.test(result)) fail(code, `${label} is invalid.`, status);
  return result;
}

function sha256(value, code, label, status = 400) {
  const result = text(value, code, label, status).toLowerCase();
  if (!SHA256_PATTERN.test(result)) fail(code, `${label} is invalid.`, status);
  return result;
}

function integer(value, code, label, status = 409, minimum = 0) {
  const token = String(value ?? '').trim();
  if (!/^\d+$/.test(token)) fail(code, `${label} is invalid.`, status);
  const result = Number(token);
  if (!Number.isSafeInteger(result) || result < minimum) {
    fail(code, `${label} is invalid.`, status);
  }
  return result;
}

function jsonClone(value, code, label) {
  try {
    const encoded = JSON.stringify(value);
    if (encoded == null) throw new TypeError('not JSON');
    return JSON.parse(encoded);
  } catch {
    fail(code, `${label} is invalid.`, 400);
  }
}

function sortedJson(value, seen = new Set()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError('Non-finite JSON number');
    return value;
  }
  if (typeof value !== 'object' || seen.has(value)) throw new TypeError('Non-JSON value');
  seen.add(value);
  const result = Array.isArray(value)
    ? value.map((entry) => sortedJson(entry, seen))
    : Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortedJson(value[key], seen)]));
  seen.delete(value);
  return result;
}

function sameJson(left, right) {
  try {
    return JSON.stringify(sortedJson(left)) === JSON.stringify(sortedJson(right));
  } catch {
    return false;
  }
}

function requireDependencies(dependencies) {
  const value = plainObject(
    dependencies,
    'WEEKLY_SOURCE_CORRECTION_DEPENDENCY_UNAVAILABLE',
    'Correct-final dependencies',
    503,
  );
  for (const name of [
    'dataRpc',
    'stageReplacementSource',
    'rebuildReplacementProjection',
    'buildOrdinaryServiceSnapshot',
  ]) {
    if (typeof value[name] !== 'function') {
      fail(
        'WEEKLY_SOURCE_CORRECTION_DEPENDENCY_UNAVAILABLE',
        'The final source cannot be corrected right now.',
        503,
        { dependency: name },
      );
    }
  }
  return value;
}

function rejectUnknownKeys(request, allowed, code) {
  for (const key of Object.keys(request)) {
    if (!allowed.has(key)) fail(code, `Correct-final field ${key} is not permitted.`, 400);
  }
}

function normaliseScope(request) {
  const sourceCycleId = uuid(
    request.source_cycle_id,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Source cycle',
  );
  const scopeKind = text(
    request.authority_scope_kind,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Authority scope',
  ).toUpperCase();
  if (!['CYCLE', 'NHSP_REPORT_SCOPE'].includes(scopeKind)) {
    fail('WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID', 'Authority scope is invalid.', 400);
  }
  const reportScopeId = request.report_scope_id == null || request.report_scope_id === ''
    ? null
    : uuid(
      request.report_scope_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Report scope',
    );
  if ((scopeKind === 'NHSP_REPORT_SCOPE') !== (reportScopeId !== null)) {
    fail('WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID', 'Report scope is invalid.', 400);
  }
  return { sourceCycleId, scopeKind, reportScopeId };
}

function normaliseCorrectionContext(value) {
  const context = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction review',
  );
  rejectUnknownKeys(context, CORRECTION_CONTEXT_KEYS, 'WEEKLY_SOURCE_CORRECTION_UNKNOWN_FIELD');
  const { sourceCycleId, scopeKind, reportScopeId } = normaliseScope(context);
  const reviewIdempotencyKey = text(
    context.review_idempotency_key,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction review reference',
  );
  if (reviewIdempotencyKey.length < 16 || reviewIdempotencyKey.length > 200) {
    fail('WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID', 'Correction review reference is invalid.', 400);
  }
  return Object.freeze({
    sourceCycleId,
    scopeKind,
    reportScopeId,
    correctionSessionId: uuid(
      context.correction_session_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Correction session',
    ),
    expectedCurrentFinalRevisionId: uuid(
      context.expected_current_final_revision_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Current final source',
    ),
    expectedFinalManifestHash: sha256(
      context.expected_final_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Current final-source proof',
    ),
    replacementUploadId: uuid(
      context.replacement_upload_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Replacement upload',
    ),
    replacementProjectionPublicationId: uuid(
      context.replacement_projection_publication_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Replacement comparison',
    ),
    expectedAuthorityScopeVersion: integer(
      context.expected_authority_scope_version,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Authority version',
      400,
      1,
    ),
    expectedRowManifestHash: sha256(
      context.expected_row_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Replacement row proof',
    ),
    expectedComparisonManifestHash: sha256(
      context.expected_comparison_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Replacement comparison proof',
    ),
    expectedIssueSetHash: sha256(
      context.expected_issue_set_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Replacement issue proof',
    ),
    expectedPreviewHash: sha256(
      context.expected_preview_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Correction review proof',
    ),
    expectedSessionVersion: integer(
      context.expected_session_version,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Correction version',
      400,
      1,
    ),
    reviewExpectedSessionVersion: integer(
      context.review_expected_session_version,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Correction review version',
      400,
      1,
    ),
    reviewIdempotencyKey,
  });
}

function normalisePreviewRequest(input) {
  const request = plainObject(
    input,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correct-final request',
  );
  const actorUserId = uuid(
    request.actor_user_id,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Office user',
  );
  const idempotencyKey = text(
    request.idempotency_key,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction reference',
  );
  if (idempotencyKey.length < 16 || idempotencyKey.length > 180) {
    fail('WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID', 'Correction reference is invalid.', 400);
  }
  if (request.correction_context != null) {
    rejectUnknownKeys(request, RECHECK_PREVIEW_REQUEST_KEYS, 'WEEKLY_SOURCE_CORRECTION_UNKNOWN_FIELD');
    return Object.freeze({
      actorUserId,
      idempotencyKey,
      recheck: true,
      context: normaliseCorrectionContext(request.correction_context),
    });
  }
  rejectUnknownKeys(request, INITIAL_PREVIEW_REQUEST_KEYS, 'WEEKLY_SOURCE_CORRECTION_UNKNOWN_FIELD');
  const { sourceCycleId, scopeKind, reportScopeId } = normaliseScope(request);
  return Object.freeze({
    actorUserId,
    sourceCycleId,
    scopeKind,
    reportScopeId,
    expectedCurrentFinalRevisionId: uuid(
      request.expected_current_final_revision_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Current final source',
    ),
    expectedFinalManifestHash: sha256(
      request.expected_final_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Current final-source proof',
    ),
    idempotencyKey,
    recheck: false,
    replacementSource: Object.freeze(jsonClone(
      plainObject(
        request.replacement_source,
        'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
        'Replacement source',
      ),
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
      'Replacement source',
    )),
  });
}

function normaliseApplyRequest(input) {
  const request = plainObject(
    input,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correct-final request',
  );
  rejectUnknownKeys(request, APPLY_REQUEST_KEYS, 'WEEKLY_SOURCE_CORRECTION_UNKNOWN_FIELD');
  const actorUserId = uuid(
    request.actor_user_id,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Office user',
  );
  const reason = text(
    request.reason,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction reason',
  );
  const confirmationText = text(
    request.confirmation_text,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction confirmation',
  );
  const idempotencyKey = text(
    request.idempotency_key,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction reference',
  );
  if (reason.length > 1_000 || confirmationText.length > 500
      || idempotencyKey.length < 16 || idempotencyKey.length > 180) {
    fail('WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID', 'Correction details are invalid.', 400);
  }
  const context = plainObject(
    request.correction_context,
    'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID',
    'Correction review',
  );
  return Object.freeze({
    actorUserId,
    reason,
    confirmationText,
    idempotencyKey,
    context: normaliseCorrectionContext(context),
  });
}

async function rpc(dependencies, name, pRequest) {
  return dependencies.dataRpc(name, { p_request: pRequest });
}

function validateOpened(value, expectedActor) {
  const opened = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_OPEN_RESULT_INVALID',
    'Correct-final OPEN result',
    502,
  );
  if (opened.ok !== true
      || !['DRAFT', 'READY', 'REVIEWED', 'PREPARED', 'APPLIED'].includes(String(opened.status))) {
    fail('WEEKLY_SOURCE_CORRECTION_OPEN_RESULT_INVALID', 'Correct-final OPEN did not seal a session.', 502);
  }
  return Object.freeze({
    ...opened,
    actor_user_id: expectedActor,
    correction_session_id: uuid(
      opened.correction_session_id,
      'WEEKLY_SOURCE_CORRECTION_OPEN_RESULT_INVALID',
      'Correction session',
      502,
    ),
    version: integer(
      opened.version,
      'WEEKLY_SOURCE_CORRECTION_OPEN_RESULT_INVALID',
      'Correction session version',
      502,
      1,
    ),
  });
}

function validateStaged(value, correctionSessionId) {
  const staged = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
    'Replacement source result',
    502,
  );
  if (staged.ok !== true || String(staged.status) !== 'CORRECTION_READY') {
    fail('WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID', 'The replacement source is not ready.', 502);
  }
  const returnedSession = uuid(
    staged.correction_session_id,
    'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
    'Correction session',
    502,
  );
  if (returnedSession !== correctionSessionId) {
    fail('WEEKLY_SOURCE_CORRECTION_STAGE_IDENTITY_MISMATCH', 'The replacement source belongs to another correction.', 409);
  }
  return Object.freeze({
    ...staged,
    correction_session_id: returnedSession,
    replacement_upload_id: uuid(
      staged.replacement_upload_id,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Replacement upload',
      502,
    ),
    replacement_projection_publication_id: uuid(
      staged.replacement_projection_publication_id,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Replacement comparison',
      502,
    ),
    expected_authority_scope_version: integer(
      staged.expected_authority_scope_version,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Authority scope version',
      502,
      1,
    ),
    expected_row_manifest_hash: sha256(
      staged.expected_row_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Replacement row proof',
      502,
    ),
    expected_comparison_manifest_hash: sha256(
      staged.expected_comparison_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Replacement comparison proof',
      502,
    ),
    expected_issue_set_hash: sha256(
      staged.expected_issue_set_hash,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Replacement issue proof',
      502,
    ),
    version: integer(
      staged.version,
      'WEEKLY_SOURCE_CORRECTION_STAGE_RESULT_INVALID',
      'Staged correction version',
      502,
      1,
    ),
  });
}

function validatePrepared(value, normalized, opened, staged) {
  const prepared = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
    'Correct-final PREPARE result',
    502,
  );
  const correctionSessionId = uuid(
    prepared.correction_session_id,
    'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
    'Correction session',
    502,
  );
  if (prepared.ok !== true || prepared.status !== 'PREPARED'
      || correctionSessionId !== opened.correction_session_id
      || uuid(
        prepared.prior_final_revision_id,
        'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
        'Prior final source',
        502,
      ) !== normalized.expectedCurrentFinalRevisionId
      || uuid(
        prepared.upload_id,
        'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
        'Prepared upload',
        502,
      ) !== staged.replacement_upload_id) {
    fail('WEEKLY_SOURCE_CORRECTION_PREPARE_IDENTITY_MISMATCH', 'The prepared source identity changed.', 409);
  }
  const rootContexts = Array.isArray(prepared.root_contexts) ? prepared.root_contexts : null;
  if (!rootContexts) {
    fail('WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID', 'Prepared root context is invalid.', 502);
  }
  const seenRoots = new Set();
  const normalisedRoots = rootContexts.map((entry) => {
    const root = plainObject(
      entry,
      'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
      'Prepared root',
      502,
    );
    const rootId = uuid(
      root.root_timesheet_id,
      'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
      'Prepared root Timesheet',
      502,
    );
    if (seenRoots.has(rootId)) {
      fail('WEEKLY_SOURCE_CORRECTION_ROOT_CONTEXT_DUPLICATE', 'A prepared Timesheet was repeated.', 502);
    }
    seenRoots.add(rootId);
    for (const field of ['expected_segments', 'expected_actual_schedule', 'expected_source_expenses']) {
      if (!Array.isArray(root[field])) {
        fail('WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID', `Prepared root ${field} is invalid.`, 502);
      }
    }
    plainObject(
      root.expected_rate_source_refs,
      'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
      'Prepared rate authority',
      502,
    );
    return Object.freeze({
      ...jsonClone(root, 'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID', 'Prepared root'),
      root_timesheet_id: rootId,
      prepared_context_hash: sha256(
        root.prepared_context_hash,
        'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
        'Prepared root proof',
        502,
      ),
    });
  });
  return Object.freeze({
    ...prepared,
    correction_session_id: correctionSessionId,
    prior_final_revision_id: normalized.expectedCurrentFinalRevisionId,
    final_revision_id: uuid(
      prepared.final_revision_id,
      'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
      'Prepared final source',
      502,
    ),
    version: integer(
      prepared.version,
      'WEEKLY_SOURCE_CORRECTION_PREPARE_RESULT_INVALID',
      'Prepared correction version',
      502,
      1,
    ),
    root_contexts: Object.freeze(normalisedRoots),
    office_preview: normaliseOfficePreview(prepared.office_preview),
  });
}

function plainPreviewRow(value, fields, label) {
  const row = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
    label,
    502,
  );
  if (!sameJson(Object.keys(row).sort(), [...fields].sort())) {
    fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID', `${label} is invalid.`, 502);
  }
  return Object.freeze(Object.fromEntries(fields.map((field) => [field, text(
    row[field],
    'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
    label,
    502,
  )])));
}

function normaliseOfficePreview(value) {
  const preview = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
    'Correction review',
    502,
  );
  if (!Array.isArray(preview.changes) || !Array.isArray(preview.blockers)) {
    fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID', 'Correction review is invalid.', 502);
  }
  return Object.freeze({
    changes: Object.freeze(preview.changes.map((row) => plainPreviewRow(
      row,
      ['candidate', 'day_date', 'current_final', 'replacement', 'result'],
      'Correction change',
    ))),
    blockers: Object.freeze(preview.blockers.map((row) => plainPreviewRow(
      row,
      ['candidate', 'day_date', 'problem', 'action'],
      'Correction blocker',
    ))),
    confirmation_text: text(
      preview.confirmation_text,
      'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
      'Correction confirmation',
      502,
    ),
    preview_hash: sha256(
      preview.preview_hash,
      'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
      'Correction review proof',
      502,
    ),
  });
}

function validateReviewed(value, identity, expectedRequestVersion) {
  const reviewed = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
    'Correction review',
    502,
  );
  const status = String(reviewed.status ?? '');
  if (reviewed.ok !== true || !['BLOCKED', 'READY_FOR_CONFIRMATION'].includes(status)
      || uuid(
        reviewed.correction_session_id,
        'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
        'Correction session',
        502,
      ) !== identity.correctionSessionId
      || uuid(
        reviewed.prior_final_revision_id,
        'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
        'Prior final source',
        502,
      ) !== identity.expectedCurrentFinalRevisionId
      || uuid(
        reviewed.upload_id,
        'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
        'Replacement upload',
        502,
      ) !== identity.replacementUploadId) {
    fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID', 'The correction review changed.', 409);
  }
  const version = integer(
    reviewed.version,
    'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID',
    'Correction version',
    502,
    1,
  );
  if (version !== expectedRequestVersion + 1) {
    fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID', 'The correction review version changed.', 409);
  }
  const officePreview = normaliseOfficePreview(reviewed.office_preview);
  if ((status === 'BLOCKED') !== (officePreview.blockers.length > 0)) {
    fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID', 'The correction review status is invalid.', 502);
  }
  return Object.freeze({ ...reviewed, status, version, office_preview: officePreview });
}

function validateServiceSnapshot(value, root) {
  const snapshot = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_SERVICE_SNAPSHOT_INVALID',
    'Ordinary Timesheet calculation',
    502,
  );
  const keys = Object.keys(snapshot).sort();
  if (!sameJson(keys, [...SERVICE_SNAPSHOT_KEYS].sort())
      || snapshot.schema_version !== 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1'
      || snapshot.calculator_owner !== 'buildWeeklyScheduleSegmentsSnapshot'
      || !Array.isArray(snapshot.source_actual_schedule_json)
      || !plainObject(
        snapshot.tsfin_snapshot_json,
        'WEEKLY_SOURCE_CORRECTION_SERVICE_SNAPSHOT_INVALID',
        'Ordinary financial calculation',
        502,
      )
      || !sameJson(snapshot.source_actual_schedule_json, root.expected_actual_schedule)
      || String(snapshot.tsfin_snapshot_json.timesheet_id ?? '').toLowerCase()
          !== root.root_timesheet_id
      || !sameJson(
        snapshot.tsfin_snapshot_json.rate_source_refs_json,
        root.expected_rate_source_refs,
      )
      || !sameJson(
        snapshot.tsfin_snapshot_json.invoice_breakdown_json?.segments,
        root.expected_segments,
      )) {
    fail(
      'WEEKLY_SOURCE_CORRECTION_SERVICE_SNAPSHOT_INVALID',
      'The ordinary Timesheet calculation does not match the prepared source.',
      502,
      { root_timesheet_id: root.root_timesheet_id },
    );
  }
  return Object.freeze(jsonClone(
    snapshot,
    'WEEKLY_SOURCE_CORRECTION_SERVICE_SNAPSHOT_INVALID',
    'Ordinary Timesheet calculation',
  ));
}

function validateApplied(value, correctionSessionId, prepared) {
  const applied = plainObject(
    value,
    'WEEKLY_SOURCE_CORRECTION_APPLY_RESULT_INVALID',
    'Correct-final APPLY result',
    502,
  );
  if (applied.ok !== true || applied.status !== 'CORRECTED'
      || uuid(
        applied.correction_session_id,
        'WEEKLY_SOURCE_CORRECTION_APPLY_RESULT_INVALID',
        'Correction session',
        502,
      ) !== correctionSessionId
      || uuid(
        applied.prior_final_revision_id,
        'WEEKLY_SOURCE_CORRECTION_APPLY_RESULT_INVALID',
        'Prior final source',
        502,
      ) !== prepared.prior_final_revision_id
      || integer(
        applied.affected_root_count,
        'WEEKLY_SOURCE_CORRECTION_APPLY_RESULT_INVALID',
        'Affected Timesheet count',
        502,
      ) !== prepared.root_contexts.length) {
    fail('WEEKLY_SOURCE_CORRECTION_APPLY_IDENTITY_MISMATCH', 'The corrected source result is invalid.', 502);
  }
  uuid(
    applied.final_revision_id,
    'WEEKLY_SOURCE_CORRECTION_APPLY_RESULT_INVALID',
    'Corrected final source',
    502,
  );
  if (typeof applied.idempotent_replay !== 'boolean') {
    fail('WEEKLY_SOURCE_CORRECTION_APPLY_RESULT_INVALID', 'The corrected source replay state is invalid.', 502);
  }
  return applied;
}

function assertActor(input, expectedActor) {
  const actorId = uuid(
    input.actor?.id,
    'WEEKLY_SOURCE_CORRECTION_ACTOR_INVALID',
    'Office user',
    401,
  );
  if (actorId !== expectedActor) {
    fail('WEEKLY_SOURCE_CORRECTION_ACTOR_MISMATCH', 'The Office user changed.', 401);
  }
}

function reviewRequest(actorUserId, identity, expectedSessionVersion, idempotencyKey) {
  return {
    schema_version: 'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1',
    actor_user_id: actorUserId,
    correction_session_id: identity.correctionSessionId,
    replacement_upload_id: identity.replacementUploadId,
    replacement_projection_publication_id: identity.replacementProjectionPublicationId,
    expected_session_version: expectedSessionVersion,
    expected_authority_scope_version: identity.expectedAuthorityScopeVersion,
    expected_row_manifest_hash: identity.expectedRowManifestHash,
    expected_comparison_manifest_hash: identity.expectedComparisonManifestHash,
    expected_issue_set_hash: identity.expectedIssueSetHash,
    idempotency_key: idempotencyKey,
  };
}

function prepareRequest(normalized, context) {
  return {
    schema_version: 'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_V1',
    actor_user_id: normalized.actorUserId,
    correction_session_id: context.correctionSessionId,
    replacement_upload_id: context.replacementUploadId,
    replacement_projection_publication_id: context.replacementProjectionPublicationId,
    expected_session_version: context.expectedSessionVersion,
    expected_authority_scope_version: context.expectedAuthorityScopeVersion,
    expected_row_manifest_hash: context.expectedRowManifestHash,
    expected_comparison_manifest_hash: context.expectedComparisonManifestHash,
    expected_issue_set_hash: context.expectedIssueSetHash,
    expected_preview_hash: context.expectedPreviewHash,
    reason: normalized.reason,
    confirmation_text: normalized.confirmationText,
    idempotency_key: `${normalized.idempotencyKey}:prepare`,
  };
}

function correctionContext(identity, reviewExpectedSessionVersion, reviewIdempotencyKey, reviewed) {
  return Object.freeze({
    source_cycle_id: identity.sourceCycleId,
    authority_scope_kind: identity.scopeKind,
    report_scope_id: identity.reportScopeId,
    expected_current_final_revision_id: identity.expectedCurrentFinalRevisionId,
    expected_final_manifest_hash: identity.expectedFinalManifestHash,
    correction_session_id: identity.correctionSessionId,
    replacement_upload_id: identity.replacementUploadId,
    replacement_projection_publication_id: identity.replacementProjectionPublicationId,
    expected_authority_scope_version: identity.expectedAuthorityScopeVersion,
    expected_row_manifest_hash: identity.expectedRowManifestHash,
    expected_comparison_manifest_hash: identity.expectedComparisonManifestHash,
    expected_issue_set_hash: identity.expectedIssueSetHash,
    review_expected_session_version: reviewExpectedSessionVersion,
    expected_session_version: reviewed.version,
    expected_preview_hash: reviewed.office_preview.preview_hash,
    review_idempotency_key: reviewIdempotencyKey,
  });
}

async function buildServiceSnapshots(dependencies, input, actorUserId, correctionSessionId, prepared) {
  const rootServiceSnapshots = [];
  for (const rootContext of prepared.root_contexts) {
    const built = await dependencies.buildOrdinaryServiceSnapshot({
      actor_user_id: actorUserId,
      correction_session_id: correctionSessionId,
      prepared_final_revision_id: prepared.final_revision_id,
      root_context: rootContext,
      env: input.env,
      ctx: input.ctx,
    });
    const serviceSnapshot = validateServiceSnapshot(
      built?.service_snapshot ?? built,
      rootContext,
    );
    rootServiceSnapshots.push(Object.freeze({
      root_timesheet_id: rootContext.root_timesheet_id,
      prepared_context_hash: rootContext.prepared_context_hash,
      service_snapshot: serviceSnapshot,
    }));
  }
  return rootServiceSnapshots;
}

/**
 * Upload and review an inactive replacement, returning only a plain-English
 * Changes/Blocked review plus an opaque server-issued apply context. No source
 * authority, Timesheet, pay, invoice, query or public-token state is changed.
 */
export async function orchestrateWeeklyCorrectFinalPreview(input = {}) {
  const dependencies = requireDependencies(input.dependencies);
  const normalized = normalisePreviewRequest(input.request);
  assertActor(input, normalized.actorUserId);

  if (normalized.recheck) {
    const priorIdentity = normalized.context;
    const rebuilt = validateStaged(await dependencies.rebuildReplacementProjection({
      actor_user_id: normalized.actorUserId,
      correction_session_id: priorIdentity.correctionSessionId,
      expected_session_version: priorIdentity.expectedSessionVersion,
      source_cycle_id: priorIdentity.sourceCycleId,
      authority_scope_kind: priorIdentity.scopeKind,
      report_scope_id: priorIdentity.reportScopeId,
      replacement_upload_id: priorIdentity.replacementUploadId,
      replacement_projection_publication_id: priorIdentity.replacementProjectionPublicationId,
      expected_authority_scope_version: priorIdentity.expectedAuthorityScopeVersion,
      expected_row_manifest_hash: priorIdentity.expectedRowManifestHash,
      expected_comparison_manifest_hash: priorIdentity.expectedComparisonManifestHash,
      expected_issue_set_hash: priorIdentity.expectedIssueSetHash,
      idempotency_key: `${normalized.idempotencyKey}:rebuild`,
      env: input.env,
      ctx: input.ctx,
    }), priorIdentity.correctionSessionId);
    if (rebuilt.replacement_upload_id !== priorIdentity.replacementUploadId
        || rebuilt.expected_authority_scope_version !== priorIdentity.expectedAuthorityScopeVersion
        || rebuilt.expected_row_manifest_hash !== priorIdentity.expectedRowManifestHash
        || rebuilt.replacement_projection_publication_id
          === priorIdentity.replacementProjectionPublicationId) {
      fail(
        'WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH',
        'The rebuilt replacement source identity changed.',
        409,
      );
    }
    const identity = Object.freeze({
      ...priorIdentity,
      replacementProjectionPublicationId: rebuilt.replacement_projection_publication_id,
      expectedComparisonManifestHash: rebuilt.expected_comparison_manifest_hash,
      expectedIssueSetHash: rebuilt.expected_issue_set_hash,
    });
    const reviewIdempotencyKey = `${normalized.idempotencyKey}:review`;
    const reviewed = validateReviewed(await rpc(
      dependencies,
      'weekly_source_correct_final_review_atomic_v1',
      reviewRequest(
        normalized.actorUserId,
        identity,
        rebuilt.version,
        reviewIdempotencyKey,
      ),
    ), identity, rebuilt.version);
    const reviewContext = correctionContext(
      identity,
      rebuilt.version,
      reviewIdempotencyKey,
      reviewed,
    );
    return Object.freeze({
      ok: true,
      status: reviewed.status,
      correction_session_id: identity.correctionSessionId,
      changes: reviewed.office_preview.changes,
      blockers: reviewed.office_preview.blockers,
      confirmation_text: reviewed.office_preview.confirmation_text,
      review_context: reviewContext,
      apply_context: reviewed.status === 'READY_FOR_CONFIRMATION' ? reviewContext : null,
    });
  }

  const opened = validateOpened(await rpc(
    dependencies,
    'weekly_source_correct_final_open_atomic_v1',
    {
      schema_version: 'WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
      actor_user_id: normalized.actorUserId,
      source_cycle_id: normalized.sourceCycleId,
      authority_scope_kind: normalized.scopeKind,
      report_scope_id: normalized.reportScopeId,
      expected_current_final_revision_id: normalized.expectedCurrentFinalRevisionId,
      expected_final_manifest_hash: normalized.expectedFinalManifestHash,
      reason: 'Reviewing a replacement source before Office confirmation.',
      idempotency_key: `${normalized.idempotencyKey}:open`,
    },
  ), normalized.actorUserId);

  const staged = validateStaged(await dependencies.stageReplacementSource({
    actor_user_id: normalized.actorUserId,
    correction_session_id: opened.correction_session_id,
    expected_session_version: opened.version,
    source_cycle_id: normalized.sourceCycleId,
    authority_scope_kind: normalized.scopeKind,
    report_scope_id: normalized.reportScopeId,
    replacement_source: normalized.replacementSource,
    idempotency_key: `${normalized.idempotencyKey}:stage`,
    env: input.env,
    ctx: input.ctx,
  }), opened.correction_session_id);

  const identity = Object.freeze({
    sourceCycleId: normalized.sourceCycleId,
    scopeKind: normalized.scopeKind,
    reportScopeId: normalized.reportScopeId,
    expectedCurrentFinalRevisionId: normalized.expectedCurrentFinalRevisionId,
    expectedFinalManifestHash: normalized.expectedFinalManifestHash,
    correctionSessionId: opened.correction_session_id,
    replacementUploadId: staged.replacement_upload_id,
    replacementProjectionPublicationId: staged.replacement_projection_publication_id,
    expectedAuthorityScopeVersion: staged.expected_authority_scope_version,
    expectedRowManifestHash: staged.expected_row_manifest_hash,
    expectedComparisonManifestHash: staged.expected_comparison_manifest_hash,
    expectedIssueSetHash: staged.expected_issue_set_hash,
  });
  const reviewIdempotencyKey = `${normalized.idempotencyKey}:review`;
  const reviewed = validateReviewed(await rpc(
    dependencies,
    'weekly_source_correct_final_review_atomic_v1',
    reviewRequest(
      normalized.actorUserId,
      identity,
      staged.version,
      reviewIdempotencyKey,
    ),
  ), identity, staged.version);
  const reviewContext = correctionContext(
    identity,
    staged.version,
    reviewIdempotencyKey,
    reviewed,
  );

  return Object.freeze({
    ok: true,
    status: reviewed.status,
    correction_session_id: opened.correction_session_id,
    changes: reviewed.office_preview.changes,
    blockers: reviewed.office_preview.blockers,
    confirmation_text: reviewed.office_preview.confirmation_text,
    review_context: reviewContext,
    apply_context: reviewed.status === 'READY_FOR_CONFIRMATION' ? reviewContext : null,
  });
}

/** Apply the exact reviewed replacement after Office supplies its reason and exact confirmation. */
export async function orchestrateWeeklyCorrectFinalApply(input = {}) {
  const dependencies = requireDependencies(input.dependencies);
  const normalized = normaliseApplyRequest(input.request);
  assertActor(input, normalized.actorUserId);
  const context = normalized.context;

  // Re-read the exact stored REVIEW result first. This is an idempotent replay,
  // not a new review and not a materialisation step.
  const reviewed = validateReviewed(await rpc(
    dependencies,
    'weekly_source_correct_final_review_atomic_v1',
    reviewRequest(
      normalized.actorUserId,
      context,
      context.reviewExpectedSessionVersion,
      context.reviewIdempotencyKey,
    ),
  ), context, context.reviewExpectedSessionVersion);
  if (reviewed.version !== context.expectedSessionVersion
      || reviewed.office_preview.preview_hash !== context.expectedPreviewHash
      || reviewed.office_preview.confirmation_text !== normalized.confirmationText
      || reviewed.office_preview.blockers.length
      || reviewed.status !== 'READY_FOR_CONFIRMATION') {
    fail(
      'WEEKLY_SOURCE_CORRECTION_PREVIEW_STALE',
      'The correction review has changed. Review the replacement again.',
      409,
    );
  }

  // Materialisation begins only after the Office reason and exact confirmation
  // have reached the server. PREPARE remains inactive and APPLY publishes last.
  const replayNormalized = Object.freeze({
    actorUserId: normalized.actorUserId,
    expectedCurrentFinalRevisionId: context.expectedCurrentFinalRevisionId,
  });
  const opened = Object.freeze({ correction_session_id: context.correctionSessionId });
  const staged = Object.freeze({
    replacement_upload_id: context.replacementUploadId,
    replacement_projection_publication_id: context.replacementProjectionPublicationId,
    expected_authority_scope_version: context.expectedAuthorityScopeVersion,
    expected_row_manifest_hash: context.expectedRowManifestHash,
    expected_comparison_manifest_hash: context.expectedComparisonManifestHash,
    expected_issue_set_hash: context.expectedIssueSetHash,
    version: context.expectedSessionVersion,
  });
  const prepared = validatePrepared(await rpc(
    dependencies,
    'weekly_source_correct_final_prepare_atomic_v1',
    prepareRequest(normalized, context),
  ), replayNormalized, opened, staged);
  if (prepared.version !== context.expectedSessionVersion + 2
      || prepared.office_preview.preview_hash !== context.expectedPreviewHash
      || prepared.office_preview.confirmation_text !== normalized.confirmationText
      || prepared.office_preview.blockers.length) {
    fail(
      'WEEKLY_SOURCE_CORRECTION_PREVIEW_STALE',
      'The correction review has changed. Review the replacement again.',
      409,
    );
  }

  const rootServiceSnapshots = await buildServiceSnapshots(
    dependencies,
    input,
    normalized.actorUserId,
    context.correctionSessionId,
    prepared,
  );

  const applied = await rpc(
    dependencies,
    'weekly_source_correct_final_apply_atomic_v1',
    {
      schema_version: 'WEEKLY_SOURCE_CORRECT_FINAL_APPLY_V1',
      actor_user_id: normalized.actorUserId,
      correction_session_id: context.correctionSessionId,
      replacement_upload_id: context.replacementUploadId,
      replacement_projection_publication_id: context.replacementProjectionPublicationId,
      expected_session_version: prepared.version,
      expected_authority_scope_version: context.expectedAuthorityScopeVersion,
      expected_row_manifest_hash: context.expectedRowManifestHash,
      expected_comparison_manifest_hash: context.expectedComparisonManifestHash,
      expected_issue_set_hash: context.expectedIssueSetHash,
      expected_preview_hash: context.expectedPreviewHash,
      reason: normalized.reason,
      confirmation_text: normalized.confirmationText,
      idempotency_key: `${normalized.idempotencyKey}:apply`,
      root_service_snapshots: rootServiceSnapshots,
    },
  );
  return validateApplied(applied, context.correctionSessionId, prepared);
}

export const WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT = Object.freeze({
  version: 'WEEKLY_CORRECT_FINAL_SOURCE_WORKER_V3',
  previewStages: Object.freeze(['OPEN', 'STAGE_REPLACEMENT', 'REVIEW_STAGED_SOURCE', 'RETURN_REVIEW']),
  recheckStages: Object.freeze(['REBUILD_STAGED_PROJECTION', 'REVIEW_STAGED_SOURCE', 'RETURN_REVIEW']),
  applyStages: Object.freeze(['RELOAD_REVIEW', 'MATERIALISE_INACTIVE', 'BUILD_SERVICE_SNAPSHOTS', 'APPLY', 'VERIFY_RESULT']),
  acceptsBrowserFinancialSnapshots: false,
  acceptsSavedReplacementUpload: false,
  previewChangesAuthority: false,
  mutatesBankingPay: false,
  mutatesInvoices: false,
});
