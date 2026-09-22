const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/i;

const FINALISE_REQUEST_KEYS = new Set([
  'actor_user_id',
  'authority_scope_kind',
  'expected_authority_scope_version',
  'expected_comparison_manifest_hash',
  'expected_issue_set_hash',
  'expected_row_manifest_hash',
  'projection_publication_id',
  'report_scope_id',
  'source_cycle_id',
  'upload_id',
]);

const RECOVERY_REQUEST_KEYS = new Set([
  'actor_user_id',
  'confirm_retry',
  'expected_task_version',
  'final_revision_id',
  'run_id',
  'task_id',
]);

const SERVICE_SNAPSHOT_KEYS = Object.freeze([
  'calculator_owner',
  'schema_version',
  'source_actual_schedule_json',
  'tsfin_snapshot_json',
]);

// Gate 2: the projection no longer publishes an entitlement and no longer
// refuses a locked root.  A never-authorised root is PREPARED_FOR_AUTHORISATION
// and awaits the ordinary Office Authorise; an authorised root is PROPOSED and
// awaits an Office decision.  REFUSED_LOCKED is gone, not re-routed.
const TERMINAL_TASK_STATES = new Set([
  'PREPARED_FOR_AUTHORISATION',
  'PROPOSED',
  'NO_OP_FIRST_NEGATIVE',
  'TARGET_MANAGED_SUPPRESSED',
  'FAILED',
]);

const RECEIPT_OUTCOMES = new Set([
  'PREPARED_FOR_AUTHORISATION',
  'PROPOSED',
  'NO_OP_FIRST_NEGATIVE',
  'TARGET_MANAGED_SUPPRESSED',
]);

export class WeeklySourceFinalisationPayError extends Error {
  constructor(code, message, status = 409, details = {}) {
    super(message);
    this.name = 'WeeklySourceFinalisationPayError';
    this.code = code;
    this.status = status;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, status = 409, details = {}) {
  throw new WeeklySourceFinalisationPayError(code, message, status, details);
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

function jsonClone(value, code, label, status = 502) {
  try {
    const encoded = JSON.stringify(value);
    if (encoded == null) throw new TypeError('not JSON');
    return JSON.parse(encoded);
  } catch {
    fail(code, `${label} is invalid.`, status);
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
    'WEEKLY_SOURCE_FINALISATION_PAY_DEPENDENCY_UNAVAILABLE',
    'Finalisation dependencies',
    503,
  );
  for (const name of ['dataRpc', 'buildOrdinaryServiceSnapshot']) {
    if (typeof value[name] !== 'function') {
      fail(
        'WEEKLY_SOURCE_FINALISATION_PAY_DEPENDENCY_UNAVAILABLE',
        'The final source cannot be completed right now.',
        503,
        { dependency: name },
      );
    }
  }
  return value;
}

function assertOnlyKeys(value, permitted, code, label) {
  for (const key of Object.keys(value)) {
    if (!permitted.has(key)) fail(code, `${label} field ${key} is not permitted.`, 400);
  }
}

function normaliseFinaliseRequest(input) {
  const request = plainObject(
    input,
    'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
    'Finalise request',
  );
  assertOnlyKeys(
    request,
    FINALISE_REQUEST_KEYS,
    'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_FIELD',
    'Finalise',
  );
  const scopeKind = text(
    request.authority_scope_kind,
    'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
    'Authority scope',
  ).toUpperCase();
  if (!['CYCLE', 'NHSP_REPORT_SCOPE'].includes(scopeKind)) {
    fail('WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID', 'Authority scope is invalid.', 400);
  }
  const reportScopeId = request.report_scope_id == null || request.report_scope_id === ''
    ? null
    : uuid(
      request.report_scope_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Report scope',
    );
  if ((scopeKind === 'NHSP_REPORT_SCOPE') !== (reportScopeId !== null)) {
    fail('WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID', 'Report scope is invalid.', 400);
  }
  return Object.freeze({
    actor_user_id: uuid(
      request.actor_user_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Office user',
    ),
    source_cycle_id: uuid(
      request.source_cycle_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Source cycle',
    ),
    authority_scope_kind: scopeKind,
    report_scope_id: reportScopeId,
    upload_id: uuid(
      request.upload_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Source upload',
    ),
    projection_publication_id: uuid(
      request.projection_publication_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Comparison publication',
    ),
    expected_authority_scope_version: integer(
      request.expected_authority_scope_version,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Authority scope version',
      400,
      1,
    ),
    expected_row_manifest_hash: sha256(
      request.expected_row_manifest_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Source row proof',
    ),
    expected_comparison_manifest_hash: sha256(
      request.expected_comparison_manifest_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Comparison proof',
    ),
    expected_issue_set_hash: sha256(
      request.expected_issue_set_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_REQUEST_INVALID',
      'Issue proof',
    ),
  });
}

function normaliseRecoveryRequest(input) {
  const request = plainObject(
    input,
    'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
    'Pay recovery request',
  );
  assertOnlyKeys(
    request,
    RECOVERY_REQUEST_KEYS,
    'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_UNKNOWN_FIELD',
    'Pay recovery',
  );
  if (typeof request.confirm_retry !== 'boolean') {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
      'Pay recovery confirmation is invalid.',
      400,
    );
  }
  return Object.freeze({
    actor_user_id: uuid(
      request.actor_user_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
      'Office user',
    ),
    final_revision_id: uuid(
      request.final_revision_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
      'Final source',
    ),
    run_id: uuid(
      request.run_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
      'Pay publication run',
    ),
    task_id: uuid(
      request.task_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
      'Pay publication item',
    ),
    expected_task_version: integer(
      request.expected_task_version,
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUEST_INVALID',
      'Pay publication item version',
      400,
      1,
    ),
    confirm_retry: request.confirm_retry,
  });
}

function unwrapRpc(value, functionName) {
  let payload = value;
  if (Array.isArray(payload) && payload.length === 1) [payload] = payload;
  if (payload && typeof payload === 'object' && !Array.isArray(payload)
      && Object.hasOwn(payload, functionName)) payload = payload[functionName];
  if (Array.isArray(payload) && payload.length === 1) [payload] = payload;
  return payload;
}

async function rpc(dependencies, name, pRequest) {
  const value = await dependencies.dataRpc(name, { p_request: pRequest });
  return unwrapRpc(value, name);
}

function validateFinalisation(value, request) {
  const result = plainObject(
    value,
    'WEEKLY_SOURCE_FINALISATION_PAY_FINALISE_RESULT_INVALID',
    'Final source result',
    502,
  );
  if (result.ok !== true || result.status !== 'FINALISED'
      || uuid(
        result.source_cycle_id,
        'WEEKLY_SOURCE_FINALISATION_PAY_FINALISE_RESULT_INVALID',
        'Finalised source cycle',
        502,
      ) !== request.source_cycle_id) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_FINALISE_RESULT_INVALID',
      'The source finalisation result is invalid.',
      502,
    );
  }
  return Object.freeze({
    ...result,
    final_revision_id: uuid(
      result.final_revision_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_FINALISE_RESULT_INVALID',
      'Final source',
      502,
    ),
  });
}

function validateTask(value, runId) {
  const task = plainObject(
    value,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Pay publication item',
    502,
  );
  const state = text(
    task.state,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Pay publication item state',
    502,
  ).toUpperCase();
  if (![...TERMINAL_TASK_STATES, 'READY', 'SUBMISSION_STARTED', 'RECOVERY_REQUIRED'].includes(state)) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication item state is invalid.',
      502,
    );
  }
  const rootContext = plainObject(
    task.root_context,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Prepared Timesheet context',
    502,
  );
  const rootId = uuid(
    task.root_timesheet_id,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Root Timesheet',
    502,
  );
  if (uuid(
    rootContext.root_timesheet_id,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Prepared root Timesheet',
    502,
  ) !== rootId
      || !Array.isArray(rootContext.expected_segments)
      || !Array.isArray(rootContext.expected_actual_schedule)
      || !Array.isArray(rootContext.expected_source_expenses)) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Prepared Timesheet context is invalid.',
      502,
    );
  }
  plainObject(
    rootContext.expected_rate_source_refs,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Prepared rate authority',
    502,
  );
  return Object.freeze({
    ...task,
    run_id: runId,
    task_id: uuid(
      task.task_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication item',
      502,
    ),
    root_timesheet_id: rootId,
    client_manifest_id: uuid(
      task.client_manifest_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Client source manifest',
      502,
    ),
    prepared_context_hash: sha256(
      task.prepared_context_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Prepared Timesheet proof',
      502,
    ),
    projection_idempotency_key: text(
      task.projection_idempotency_key,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication reference',
      502,
    ),
    task_ordinal: integer(
      task.task_ordinal,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication order',
      502,
      1,
    ),
    attempt_count: integer(
      task.attempt_count,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication attempt count',
      502,
    ),
    version: integer(
      task.version,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication item version',
      502,
      1,
    ),
    state,
    root_context: Object.freeze(jsonClone(
      rootContext,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Prepared Timesheet context',
    )),
  });
}

function validateRun(value, expectedFinalRevisionId) {
  const run = plainObject(
    value,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Pay publication run',
    502,
  );
  const runId = uuid(
    run.run_id,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Pay publication run',
    502,
  );
  const revisionId = uuid(
    run.final_revision_id,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Final source',
    502,
  );
  const state = text(
    run.state,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Pay publication run state',
    502,
  ).toUpperCase();
  if (revisionId !== expectedFinalRevisionId
      || !['READY', 'RUNNING', 'RECOVERY_REQUIRED', 'ACTION_REQUIRED', 'COMPLETE'].includes(state)
      || !Array.isArray(run.tasks)) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication run identity is invalid.',
      502,
    );
  }
  const tasks = run.tasks.map((task) => validateTask(task, runId));
  const taskCount = integer(
    run.task_count,
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
    'Pay publication item count',
    502,
  );
  if (taskCount !== tasks.length
      || new Set(tasks.map((task) => task.task_id)).size !== tasks.length
      || new Set(tasks.map((task) => task.root_timesheet_id)).size !== tasks.length) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication item manifest is invalid.',
      502,
    );
  }
  return Object.freeze({
    ...run,
    run_id: runId,
    final_revision_id: revisionId,
    source_cycle_id: uuid(
      run.source_cycle_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Source cycle',
      502,
    ),
    task_manifest_hash: sha256(
      run.task_manifest_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication manifest proof',
      502,
    ),
    run_hash: sha256(
      run.run_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Pay publication run proof',
      502,
    ),
    state,
    task_count: taskCount,
    terminal_task_count: integer(
      run.terminal_task_count,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Completed pay publication item count',
      502,
    ),
    action_required_task_count: integer(
      run.action_required_task_count,
      'WEEKLY_SOURCE_FINALISATION_PAY_RUN_RESULT_INVALID',
      'Action-required pay publication item count',
      502,
    ),
    tasks: Object.freeze(tasks),
  });
}

function validateServiceSnapshot(value, root) {
  const candidate = value?.service_snapshot ?? value;
  const snapshot = plainObject(
    candidate,
    'WEEKLY_SOURCE_FINALISATION_PAY_SERVICE_SNAPSHOT_INVALID',
    'Ordinary Timesheet calculation',
    502,
  );
  const keys = Object.keys(snapshot).sort();
  const tsfin = plainObject(
    snapshot.tsfin_snapshot_json,
    'WEEKLY_SOURCE_FINALISATION_PAY_SERVICE_SNAPSHOT_INVALID',
    'Ordinary financial calculation',
    502,
  );
  if (!sameJson(keys, [...SERVICE_SNAPSHOT_KEYS].sort())
      || snapshot.schema_version !== 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1'
      || snapshot.calculator_owner !== 'buildWeeklyScheduleSegmentsSnapshot'
      || !Array.isArray(snapshot.source_actual_schedule_json)
      || !sameJson(snapshot.source_actual_schedule_json, root.expected_actual_schedule)
      || String(tsfin.timesheet_id ?? '').toLowerCase() !== root.root_timesheet_id
      || !sameJson(tsfin.rate_source_refs_json, root.expected_rate_source_refs)
      || !sameJson(tsfin.invoice_breakdown_json?.segments, root.expected_segments)) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_SERVICE_SNAPSHOT_INVALID',
      'The ordinary Timesheet calculation does not match the final source.',
      502,
      { root_timesheet_id: root.root_timesheet_id },
    );
  }
  return Object.freeze(jsonClone(
    snapshot,
    'WEEKLY_SOURCE_FINALISATION_PAY_SERVICE_SNAPSHOT_INVALID',
    'Ordinary Timesheet calculation',
  ));
}

function validateStart(value, task, finalRevisionId) {
  const result = plainObject(
    value,
    'WEEKLY_SOURCE_FINALISATION_PAY_START_RESULT_INVALID',
    'Pay publication start',
    502,
  );
  const returnedTask = validateTask(result.task, task.run_id);
  if (returnedTask.task_id !== task.task_id
      || returnedTask.root_timesheet_id !== task.root_timesheet_id) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_START_IDENTITY_MISMATCH',
      'Pay publication item identity changed.',
      409,
    );
  }
  if (result.status === 'RECOVERY_REQUIRED' || TERMINAL_TASK_STATES.has(returnedTask.state)) {
    return Object.freeze({ ...result, task: returnedTask, projection_request: null });
  }
  const projectionRequest = plainObject(
    result.projection_request,
    'WEEKLY_SOURCE_FINALISATION_PAY_START_RESULT_INVALID',
    'Ordinary pay projection request',
    502,
  );
  const expectedKeys = [
    'actor_user_id',
    'final_revision_id',
    'idempotency_key',
    'root_timesheet_id',
    'schema_version',
  ].sort();
  if (result.ok !== true || result.status !== 'SUBMISSION_STARTED'
      || returnedTask.state !== 'SUBMISSION_STARTED'
      || !sameJson(Object.keys(projectionRequest).sort(), expectedKeys)
      || projectionRequest.schema_version !== 'WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1'
      || uuid(
        projectionRequest.final_revision_id,
        'WEEKLY_SOURCE_FINALISATION_PAY_START_RESULT_INVALID',
        'Projection final source',
        502,
      ) !== finalRevisionId
      || uuid(
        projectionRequest.root_timesheet_id,
        'WEEKLY_SOURCE_FINALISATION_PAY_START_RESULT_INVALID',
        'Projection root Timesheet',
        502,
      ) !== task.root_timesheet_id
      || projectionRequest.idempotency_key !== task.projection_idempotency_key) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_START_RESULT_INVALID',
      'Pay publication start result is invalid.',
      502,
    );
  }
  return Object.freeze({
    ...result,
    task: returnedTask,
    projection_request: Object.freeze(jsonClone(
      projectionRequest,
      'WEEKLY_SOURCE_FINALISATION_PAY_START_RESULT_INVALID',
      'Ordinary pay projection request',
    )),
  });
}

function validateReceipt(value, task, finalRevisionId) {
  const receipt = plainObject(
    value,
    'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
    'Ordinary pay projection result',
    502,
  );
  const outcome = text(
    receipt.outcome,
    'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
    'Ordinary pay projection outcome',
    502,
  ).toUpperCase();
  if (!RECEIPT_OUTCOMES.has(outcome)
      || receipt.ok !== true
      || uuid(
        receipt.final_revision_id,
        'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
        'Projection final source',
        502,
      ) !== finalRevisionId
      || uuid(
        receipt.root_timesheet_id,
        'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
        'Projection root Timesheet',
        502,
      ) !== task.root_timesheet_id) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
      'The ordinary pay projection result cannot be proved.',
      502,
    );
  }
  return Object.freeze({
    ...receipt,
    outcome,
    receipt_id: uuid(
      receipt.receipt_id,
      'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
      'Projection receipt',
      502,
    ),
    receipt_hash: sha256(
      receipt.receipt_hash,
      'WEEKLY_SOURCE_FINALISATION_PAY_PROJECTION_RESULT_UNKNOWN',
      'Projection receipt proof',
      502,
    ),
  });
}

function publicRun(run) {
  return Object.freeze({
    run_id: run.run_id,
    final_revision_id: run.final_revision_id,
    source_cycle_id: run.source_cycle_id,
    state: run.state,
    task_count: run.task_count,
    terminal_task_count: run.terminal_task_count,
    action_required_task_count: run.action_required_task_count,
    tasks: Object.freeze(run.tasks.map((task) => Object.freeze({
      task_id: task.task_id,
      task_ordinal: task.task_ordinal,
      client_manifest_id: task.client_manifest_id,
      root_timesheet_id: task.root_timesheet_id,
      state: task.state,
      attempt_count: task.attempt_count,
      version: task.version,
      requires_recovery: task.state === 'SUBMISSION_STARTED' || task.state === 'RECOVERY_REQUIRED',
      action_required: task.state === 'FAILED',
    }))),
  });
}

function sourceFinalisedResult(status, sourceFinalisation, run, extra = {}) {
  return Object.freeze({
    ok: true,
    status,
    source_finalised: true,
    invoice_authority_committed: true,
    source_finalisation: sourceFinalisation,
    ordinary_pay_projection: run ? publicRun(run) : null,
    ...extra,
  });
}

function resultAfterUnknown(sourceFinalisation, run, task) {
  if (run.state === 'COMPLETE') {
    return sourceFinalisedResult('FINALISED', sourceFinalisation, run);
  }
  if (run.state === 'ACTION_REQUIRED') {
    return sourceFinalisedResult('FINALISED_PAY_ACTION_REQUIRED', sourceFinalisation, run);
  }
  return sourceFinalisedResult(
    'FINALISED_PAY_RECOVERY_REQUIRED',
    sourceFinalisation,
    run,
    {
      recovery: Object.freeze({
        run_id: run.run_id,
        task_id: task.task_id,
        expected_task_version: run.tasks.find((entry) => entry.task_id === task.task_id)?.version
          ?? task.version,
      }),
    },
  );
}

function boundedErrorCode(error, fallback) {
  const raw = String(error?.code || error?.name || fallback).trim().toUpperCase();
  const safe = raw.replace(/[^A-Z0-9_]/g, '_').replace(/_+/g, '_').slice(0, 120);
  return safe || fallback;
}

async function markUnknown(dependencies, actorUserId, run, task, error) {
  try {
    const value = await rpc(
      dependencies,
      'weekly_source_finalisation_pay_task_unknown_atomic_v1',
      {
        schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_V1',
        actor_user_id: actorUserId,
        run_id: run.run_id,
        task_id: task.task_id,
        expected_task_version: task.version,
        error_code: boundedErrorCode(
          error,
          'WEEKLY_SOURCE_FINALISATION_PAY_RESULT_UNKNOWN',
        ),
      },
    );
    return validateRun(value, run.final_revision_id);
  } catch {
    return Object.freeze({
      ...run,
      state: 'RECOVERY_REQUIRED',
      tasks: Object.freeze(run.tasks.map((entry) => entry.task_id === task.task_id
        ? Object.freeze({ ...entry, state: 'SUBMISSION_STARTED' })
        : entry)),
    });
  }
}

async function processReadyTasks({
  actorUserId,
  sourceFinalisation,
  initialRun,
  dependencies,
  env,
  ctx,
}) {
  let run = initialRun;
  for (const listedTask of run.tasks) {
    let task = run.tasks.find((entry) => entry.task_id === listedTask.task_id);
    if (!task || TERMINAL_TASK_STATES.has(task.state)) continue;
    if (task.state === 'SUBMISSION_STARTED' || task.state === 'RECOVERY_REQUIRED') {
      return sourceFinalisedResult(
        'FINALISED_PAY_RECOVERY_REQUIRED',
        sourceFinalisation,
        run,
        {
          recovery: Object.freeze({
            run_id: run.run_id,
            task_id: task.task_id,
            expected_task_version: task.version,
          }),
        },
      );
    }

    let serviceSnapshot;
    try {
      const built = await dependencies.buildOrdinaryServiceSnapshot({
        actor_user_id: actorUserId,
        final_revision_id: run.final_revision_id,
        root_context: task.root_context,
        env,
        ctx,
      });
      serviceSnapshot = validateServiceSnapshot(built, task.root_context);
    } catch (error) {
      const errorCode = boundedErrorCode(
        error,
        'WEEKLY_SOURCE_FINALISATION_PAY_PREPARATION_FAILED',
      );
      const errorDetailCode = boundedErrorCode(
        { code: error?.message },
        'WEEKLY_SOURCE_FINALISATION_PAY_PREPARATION_DETAIL_UNAVAILABLE',
      );
      const databaseErrorCode = boundedErrorCode(
        { code: error?.json?.code },
        'DATABASE_ERROR_CODE_UNAVAILABLE',
      );
      const databaseErrorMessageCode = boundedErrorCode(
        { code: error?.json?.message },
        'DATABASE_ERROR_MESSAGE_UNAVAILABLE',
      );
      try {
        console.warn(JSON.stringify({
          event: 'weekly_source_finalisation_pay_preparation_required',
          error_code: errorCode,
          error_detail_code: errorDetailCode,
          database_error_code: databaseErrorCode,
          database_error_message_code: databaseErrorMessageCode,
          task_id: task.task_id,
          final_revision_id: run.final_revision_id,
        }));
      } catch {}
      return sourceFinalisedResult(
        'FINALISED_PAY_PREPARATION_REQUIRED',
        sourceFinalisation,
        run,
        {
          pay_projection_error_code: errorCode,
          pay_projection_task_id: task.task_id,
        },
      );
    }

    let started;
    try {
      started = validateStart(await rpc(
        dependencies,
        'weekly_source_finalisation_pay_task_start_atomic_v1',
        {
          schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_V1',
          actor_user_id: actorUserId,
          run_id: run.run_id,
          task_id: task.task_id,
          expected_task_version: task.version,
          expected_context_hash: task.prepared_context_hash,
        },
      ), task, run.final_revision_id);
    } catch (error) {
      return sourceFinalisedResult(
        'FINALISED_PAY_RECOVERY_REQUIRED',
        sourceFinalisation,
        run,
        {
          pay_projection_error_code: boundedErrorCode(
            error,
            'WEEKLY_SOURCE_FINALISATION_PAY_START_FAILED',
          ),
          pay_projection_task_id: task.task_id,
        },
      );
    }

    task = started.task;
    if (!started.projection_request) {
      const syntheticRun = Object.freeze({
        ...run,
        state: task.state === 'RECOVERY_REQUIRED' || task.state === 'SUBMISSION_STARTED'
          ? 'RECOVERY_REQUIRED'
          : run.state,
        tasks: Object.freeze(run.tasks.map((entry) => entry.task_id === task.task_id ? task : entry)),
      });
      return sourceFinalisedResult(
        task.state === 'RECOVERY_REQUIRED' || task.state === 'SUBMISSION_STARTED'
          ? 'FINALISED_PAY_RECOVERY_REQUIRED'
          : 'FINALISED_PAY_CHECKPOINT_REQUIRED',
        sourceFinalisation,
        syntheticRun,
      );
    }

    let receipt;
    try {
      receipt = validateReceipt(await rpc(
        dependencies,
        'weekly_source_ordinary_pay_projection_apply_atomic_v1',
        {
          ...started.projection_request,
          service_snapshot: serviceSnapshot,
        },
      ), task, run.final_revision_id);
    } catch (error) {
      run = await markUnknown(dependencies, actorUserId, run, task, error);
      return resultAfterUnknown(sourceFinalisation, run, task);
    }

    try {
      run = validateRun(await rpc(
        dependencies,
        'weekly_source_finalisation_pay_task_finish_atomic_v1',
        {
          schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_V1',
          actor_user_id: actorUserId,
          run_id: run.run_id,
          task_id: task.task_id,
          expected_task_version: task.version,
          projection_receipt_id: receipt.receipt_id,
          projection_receipt_hash: receipt.receipt_hash,
        },
      ), run.final_revision_id);
    } catch (error) {
      run = await markUnknown(dependencies, actorUserId, run, task, error);
      return resultAfterUnknown(sourceFinalisation, run, task);
    }
  }

  if (run.state === 'COMPLETE') {
    return sourceFinalisedResult('FINALISED', sourceFinalisation, run);
  }
  if (run.state === 'ACTION_REQUIRED') {
    return sourceFinalisedResult('FINALISED_PAY_ACTION_REQUIRED', sourceFinalisation, run);
  }
  if (run.state === 'RECOVERY_REQUIRED') {
    return sourceFinalisedResult('FINALISED_PAY_RECOVERY_REQUIRED', sourceFinalisation, run);
  }
  return sourceFinalisedResult('FINALISED_PAY_CHECKPOINT_REQUIRED', sourceFinalisation, run);
}

/**
 * Finalises source first, then publishes each immutable per-root manifest through
 * the existing ordinary Timesheet/TSFIN owner. Source invoice authority remains
 * committed even when this second phase needs action or explicit recovery.
 */
export async function orchestrateWeeklySourceFinalisation(input = {}) {
  const dependencies = requireDependencies(input.dependencies);
  const request = normaliseFinaliseRequest(input.request);
  const actorUserId = uuid(
    input.actor?.id,
    'WEEKLY_SOURCE_FINALISATION_PAY_ACTOR_INVALID',
    'Office user',
    401,
  );
  if (actorUserId !== request.actor_user_id) {
    fail('WEEKLY_SOURCE_FINALISATION_PAY_ACTOR_MISMATCH', 'The Office user changed.', 401);
  }

  // Deliberately not caught: an indeterminate finalisation response may only be
  // resolved by replaying the already-idempotent finalisation command.
  const sourceFinalisation = validateFinalisation(await rpc(
    dependencies,
    'weekly_source_finalise_atomic_v1',
    request,
  ), request);

  let run;
  try {
    run = validateRun(await rpc(
      dependencies,
      'weekly_source_finalisation_pay_open_atomic_v1',
      {
        schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_V1',
        actor_user_id: request.actor_user_id,
        final_revision_id: sourceFinalisation.final_revision_id,
      },
    ), sourceFinalisation.final_revision_id);
  } catch (error) {
    return sourceFinalisedResult(
      'FINALISED_PAY_CHECKPOINT_REQUIRED',
      sourceFinalisation,
      null,
      {
        pay_projection_error_code: boundedErrorCode(
          error,
          'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_FAILED',
        ),
      },
    );
  }

  return processReadyTasks({
    actorUserId,
    sourceFinalisation,
    initialRun: run,
    dependencies,
    env: input.env,
    ctx: input.ctx,
  });
}

/**
 * Explicit recovery only. It first asks the database for a durable receipt. A
 * retry is re-armed only when the caller separately confirms it and no receipt
 * exists; the original idempotency key is always retained.
 */
export async function recoverWeeklySourceFinalisationPayProjection(input = {}) {
  const dependencies = requireDependencies(input.dependencies);
  const request = normaliseRecoveryRequest(input.request);
  const actorUserId = uuid(
    input.actor?.id,
    'WEEKLY_SOURCE_FINALISATION_PAY_ACTOR_INVALID',
    'Office user',
    401,
  );
  if (actorUserId !== request.actor_user_id) {
    fail('WEEKLY_SOURCE_FINALISATION_PAY_ACTOR_MISMATCH', 'The Office user changed.', 401);
  }

  const opened = validateRun(await rpc(
    dependencies,
    'weekly_source_finalisation_pay_open_atomic_v1',
    {
      schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_V1',
      actor_user_id: request.actor_user_id,
      final_revision_id: request.final_revision_id,
    },
  ), request.final_revision_id);
  const selected = opened.tasks.find((task) => task.task_id === request.task_id);
  if (!selected || opened.run_id !== request.run_id || selected.version !== request.expected_task_version) {
    fail(
      'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_STALE',
      'The pay publication recovery details changed.',
      409,
    );
  }

  const recovered = validateRun(await rpc(
    dependencies,
    'weekly_source_finalisation_pay_task_recover_atomic_v1',
    {
      schema_version: 'WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_V1',
      actor_user_id: request.actor_user_id,
      run_id: request.run_id,
      task_id: request.task_id,
      expected_task_version: request.expected_task_version,
      confirm_retry: request.confirm_retry,
    },
  ), request.final_revision_id);

  const sourceFinalisation = Object.freeze({
    ok: true,
    status: 'FINALISED',
    final_revision_id: request.final_revision_id,
    source_cycle_id: recovered.source_cycle_id,
    idempotent: true,
  });
  const recoveredTask = recovered.tasks.find((task) => task.task_id === request.task_id);
  if (!request.confirm_retry || recoveredTask?.state !== 'READY') {
    if (recovered.state === 'COMPLETE') {
      return sourceFinalisedResult('FINALISED', sourceFinalisation, recovered);
    }
    if (recovered.state === 'ACTION_REQUIRED') {
      return sourceFinalisedResult('FINALISED_PAY_ACTION_REQUIRED', sourceFinalisation, recovered);
    }
    return sourceFinalisedResult(
      'FINALISED_PAY_RECOVERY_REQUIRED',
      sourceFinalisation,
      recovered,
      {
        recovery: recoveredTask && !TERMINAL_TASK_STATES.has(recoveredTask.state)
          ? Object.freeze({
            run_id: recovered.run_id,
            task_id: recoveredTask.task_id,
            expected_task_version: recoveredTask.version,
          })
          : null,
      },
    );
  }

  return processReadyTasks({
    actorUserId,
    sourceFinalisation,
    initialRun: recovered,
    dependencies,
    env: input.env,
    ctx: input.ctx,
  });
}

export const WEEKLY_SOURCE_FINALISATION_PAY_ORCHESTRATION_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_FINALISATION_PAY_WORKER_V1',
  stages: Object.freeze([
    'FINALISE_SOURCE',
    'OPEN_DURABLE_MANIFEST',
    'BUILD_SERVER_SNAPSHOT',
    'CHECKPOINT_SUBMISSION',
    'APPLY_EXISTING_ORDINARY_PROJECTION_ONCE',
    'CHECKPOINT_DURABLE_RECEIPT',
  ]),
  sourceFinalisationCommitsBeforePayProjection: true,
  invoiceAuthorityDependsOnPayProjection: false,
  automaticRetryAfterUnknownOutcome: false,
  acceptsBrowserFinancialSnapshots: false,
  usesExistingOrdinaryTimesheetTsfinLifecycle: true,
  bypassesWorkbench: false,
  mutatesWorkbenchOrBankingPay: false,
  mutatesInvoices: false,
});
