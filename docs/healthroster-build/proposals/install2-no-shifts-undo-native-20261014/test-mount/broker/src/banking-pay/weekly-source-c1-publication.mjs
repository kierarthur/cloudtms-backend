import {
  bindWeeklySourceC1Start,
} from './weekly-source-c1-authoring.mjs';

const NON_PROGRESS_STATUSES = new Set([
  'PENDING',
  'BUSY',
  'STALE',
  'REFUSED',
  'ABORTED',
  'RETIRED',
  'COMPACTED',
]);

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function object(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(code, `${label} is unavailable.`);
  }
  return value;
}

function positiveInteger(value, label) {
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < 1) {
    fail('C1_PUBLICATION_LIMIT_INVALID', `${label} is invalid.`);
  }
  return result;
}

function cursor(result) {
  object(result, 'C1_PUBLICATION_RESULT_INVALID', 'C1 result');
  if (!result.operation_id || result.owner_epoch == null || result.next_sequence == null || !result.receipt_sha256) {
    fail('C1_PUBLICATION_CURSOR_INVALID', 'C1 did not return a complete operation cursor.');
  }
  return Object.freeze({
    operation_id: result.operation_id,
    owner_epoch: result.owner_epoch,
    next_sequence: result.next_sequence,
    receipt_sha256: result.receipt_sha256,
  });
}

function recordList(prepared) {
  const sources = Array.isArray(prepared?.stream?.sources) ? prepared.stream.sources : null;
  const components = Array.isArray(prepared?.stream?.components) ? prepared.stream.components : null;
  if (!sources || !components) {
    fail('C1_PUBLICATION_STREAM_INVALID', 'The prepared C1 stream is incomplete.');
  }
  const records = [];
  for (const source of sources) {
    const parts = Array.isArray(source.parts) ? source.parts : null;
    if (!parts) fail('C1_PUBLICATION_STREAM_INVALID', 'A prepared source record has no parts.');
    const { parts: ignored, ...sourceRecord } = source;
    void ignored;
    records.push(sourceRecord, ...parts);
  }
  records.push(...components);
  return Object.freeze(records.map((record) => Object.freeze({ ...record })));
}

function isTerminal(result) {
  return result.status === 'PUBLISHED' || NON_PROGRESS_STATUSES.has(result.status);
}

function requireForwardProgress(before, after, operationName) {
  if (
    before
    && String(before.next_sequence) === String(after.next_sequence)
    && before.receipt_sha256 === after.receipt_sha256
    && after.status !== 'PUBLISHED'
    && !NON_PROGRESS_STATUSES.has(after.status)
  ) {
    fail(
      'C1_PUBLICATION_NO_PROGRESS',
      `C1 made no durable progress during ${operationName}.`,
      { operationName, status: after.status, phase: after.phase },
    );
  }
}

async function checkpoint(callback, phase, result, extra = {}) {
  if (typeof callback === 'function') {
    await callback(Object.freeze({ phase, result, ...extra }));
  }
}

async function invokeWithUnknownCheckpoint(callback, phase, operation, extra = {}) {
  try {
    return await operation();
  } catch (error) {
    if (error?.recoveryCall && typeof callback === 'function') {
      await callback(Object.freeze({
        phase,
        error_code: String(error.code ?? 'C1_OUTCOME_UNKNOWN'),
        recovery_call: error.recoveryCall,
        ...extra,
      }));
    }
    throw error;
  }
}

function resumeCheckpoint(value, recordCount) {
  if (value == null) return null;
  const row = object(value, 'C1_PUBLICATION_RESUME_INVALID', 'C1 publication checkpoint');
  const phase = String(row.phase ?? '').trim().toUpperCase();
  if (!['START', 'STAGE', 'VALIDATE', 'CERTIFY', 'PUBLISH'].includes(phase)) {
    fail('C1_PUBLICATION_RESUME_INVALID', 'The C1 publication checkpoint phase is invalid.');
  }
  const result = object(row.result, 'C1_PUBLICATION_RESUME_INVALID', 'C1 checkpoint result');
  const recordOffset = Number(row.next_record_offset ?? row.record_offset ?? 0);
  if (!Number.isSafeInteger(recordOffset) || recordOffset < 0 || recordOffset > recordCount) {
    fail('C1_PUBLICATION_RESUME_INVALID', 'The C1 publication checkpoint record offset is invalid.');
  }
  if (!isTerminal(result)) cursor(result);
  return Object.freeze({ phase, result, recordOffset });
}

/**
 * Convert one explicit C1 unknown-outcome recovery into the only safe resume
 * cursor.  A recovered STAGE call advances by the exact stored
 * input_records_consumed value; it never infers progress from status cursors.
 */
export function resumeWeeklySourceC1CheckpointFromRecovery(input = {}) {
  const unknown = object(
    input.unknown_checkpoint,
    'C1_PUBLICATION_RECOVERY_INVALID',
    'Saved unknown-outcome checkpoint',
  );
  const recovery = object(
    input.recovery,
    'C1_PUBLICATION_RECOVERY_INVALID',
    'C1 recovery result',
  );
  if (recovery.contract !== 'WEEKLY_SOURCE_C1_RECOVERY_V1' || recovery.ok !== true) {
    fail('C1_PUBLICATION_RECOVERY_REFUSED', 'C1 did not prove a recoverable publication result.');
  }
  const phase = String(unknown.phase ?? '').trim().toUpperCase();
  if (!['START', 'STAGE', 'VALIDATE', 'CERTIFY', 'PUBLISH'].includes(phase)) {
    fail('C1_PUBLICATION_RECOVERY_INVALID', 'The saved recovery phase is invalid.');
  }
  const recordOffset = Number(unknown.record_offset ?? unknown.next_record_offset ?? 0);
  const recordsSubmitted = Number(unknown.records_submitted ?? 0);
  if (!Number.isSafeInteger(recordOffset) || recordOffset < 0
      || !Number.isSafeInteger(recordsSubmitted) || recordsSubmitted < 0) {
    fail('C1_PUBLICATION_RECOVERY_INVALID', 'The saved recovery offsets are invalid.');
  }
  const result = object(
    recovery.result,
    'C1_PUBLICATION_RECOVERY_INVALID',
    'Recovered C1 result',
  );
  let nextRecordOffset = recordOffset;
  if (phase === 'STAGE') {
    if (
      recovery.decision !== 'REPLAYED'
      || recovery.code !== 'C1_RECOVERY_STAGE_COMMITTED_EXACT_REPLAY'
      || recovery.replay_attempted !== true
    ) {
      fail(
        'C1_PUBLICATION_STAGE_RECOVERY_UNPROVED',
        'The uncertain C1 stage was not recovered by its exact sealed replay.',
      );
    }
    const consumed = Number(result.input_records_consumed);
    if (!Number.isSafeInteger(consumed) || consumed < 0 || consumed > recordsSubmitted) {
      fail(
        'C1_PUBLICATION_CONSUMED_INVALID',
        'The recovered C1 stage count is invalid.',
      );
    }
    nextRecordOffset += consumed;
  }
  return Object.freeze({
    phase,
    result,
    record_offset: recordOffset,
    next_record_offset: nextRecordOffset,
    recovered: true,
  });
}

/**
 * Bind an already normalized, complete C1 evidence stream to the exact C1
 * start request.  This is deliberately separate from the financial owner:
 * callers must supply a stream built from the established Weekly calculator.
 */
export async function prepareWeeklySourceC1Publication(input = {}) {
  const prepared = object(input.prepared_stream, 'C1_PUBLICATION_STREAM_INVALID', 'Prepared C1 stream');
  const stream = object(prepared.stream, 'C1_PUBLICATION_STREAM_INVALID', 'Prepared C1 stream');
  const startFacts = object(input.start_facts, 'C1_PUBLICATION_START_INVALID', 'C1 start facts');
  const bound = await bindWeeklySourceC1Start({
    ...startFacts,
    agency_id: prepared.agency_id,
    request_id: prepared.request_id,
    actor_user_id: prepared.actor_user_id,
    candidate_id: prepared.candidate_id,
    contract_id: prepared.contract_id,
    root_timesheet_id: prepared.root_timesheet_id,
    week_ending_date: prepared.week_ending_date,
    source_mode: prepared.source_mode,
    expected_source_count: stream.expected_source_count,
    expected_component_count: stream.expected_component_count,
    expected_payload_bytes: stream.expected_payload_bytes,
    source_manifest_sha256: stream.source_manifest_sha256,
    entitlement_sha256: stream.entitlement_sha256,
    approval_sha256: prepared.office_approval_intent_sha256,
    is_zero_entitlement: stream.is_zero_entitlement,
  });
  return Object.freeze({
    request: bound.start,
    request_sha256: bound.request_sha256,
    records: recordList(prepared),
    stream,
  });
}

/**
 * Publish one prepared C1 request without automatic retry.  Every committed
 * cursor is surfaced to the caller before the next RPC so it can be persisted.
 * Unknown outcomes are never guessed or replayed here; the caller must retain
 * the adapter's recovery envelope and invoke its explicit recovery path.
 */
export async function publishWeeklySourceC1Prepared(input = {}) {
  const adapter = object(input.adapter, 'C1_PUBLICATION_ADAPTER_INVALID', 'C1 adapter');
  const publication = object(input.publication, 'C1_PUBLICATION_INPUT_INVALID', 'Prepared publication');
  if (!Array.isArray(publication.records)) {
    fail('C1_PUBLICATION_INPUT_INVALID', 'Prepared publication records are unavailable.');
  }
  const maxCalls = positiveInteger(input.max_calls ?? 10_000, 'C1 call limit');
  const maxStageRecords = Math.min(256, positiveInteger(input.max_stage_records ?? 128, 'C1 stage size'));
  const resume = resumeCheckpoint(input.resume_checkpoint, publication.records.length);
  let calls = 0;
  const countCall = () => {
    calls += 1;
    if (calls > maxCalls) fail('C1_PUBLICATION_CALL_LIMIT', 'C1 publication exceeded its bounded call limit.');
  };

  let result;
  let phase;
  let operation;
  let offset;
  if (resume) {
    result = resume.result;
    phase = resume.phase;
    offset = resume.recordOffset;
    if (isTerminal(result)) return Object.freeze({ result, calls, records_consumed: offset, resumed: true });
    operation = cursor(result);
  } else {
    countCall();
    result = await invokeWithUnknownCheckpoint(
      input.on_unknown,
      'START',
      () => adapter.start(publication.request),
      { record_offset: 0, next_record_offset: 0 },
    );
    await checkpoint(input.on_checkpoint, 'START', result, {
      record_offset: 0,
      next_record_offset: 0,
    });
    if (isTerminal(result)) return Object.freeze({ result, calls, records_consumed: 0, resumed: false });
    phase = 'START';
    operation = cursor(result);
    offset = 0;
  }

  // A definite STAGE checkpoint advances only by the exact number the C1
  // owner says it consumed.  A restarted Worker therefore submits the suffix,
  // never the whole page and never a guessed cursor.
  while (offset < publication.records.length) {
    const page = publication.records.slice(offset, Math.min(offset + maxStageRecords, publication.records.length));
    countCall();
    const before = operation;
    result = await invokeWithUnknownCheckpoint(
      input.on_unknown,
      'STAGE',
      () => adapter.stage(operation, page),
      { record_offset: offset, next_record_offset: offset, records_submitted: page.length },
    );
    const consumed = Number(result.input_records_consumed);
    if (!Number.isSafeInteger(consumed) || consumed < 0 || consumed > page.length) {
      fail('C1_PUBLICATION_CONSUMED_INVALID', 'C1 returned an invalid staged-record count.');
    }
    await checkpoint(input.on_checkpoint, 'STAGE', result, {
      record_offset: offset,
      next_record_offset: offset + consumed,
      records_submitted: page.length,
      records_consumed: consumed,
    });
    if (isTerminal(result)) return Object.freeze({ result, calls, records_consumed: offset + consumed });
    operation = cursor(result);
    requireForwardProgress(before, operation, 'STAGE');
    if (consumed === 0) {
      fail('C1_PUBLICATION_NO_PROGRESS', 'C1 accepted no records from a non-empty stage page.');
    }
    offset += consumed;
    phase = 'STAGE';
  }

  if (!resume || !['VALIDATE', 'CERTIFY', 'PUBLISH'].includes(phase) || result.has_more === true) {
    do {
      countCall();
      const before = operation;
      result = await invokeWithUnknownCheckpoint(
        input.on_unknown,
        'VALIDATE',
        () => adapter.continueValidation(operation),
        { record_offset: offset, next_record_offset: offset },
      );
      await checkpoint(input.on_checkpoint, 'VALIDATE', result, {
        record_offset: offset,
        next_record_offset: offset,
      });
      if (isTerminal(result)) return Object.freeze({ result, calls, records_consumed: offset, resumed: !!resume });
      operation = cursor(result);
      requireForwardProgress(before, operation, 'VALIDATE');
      phase = 'VALIDATE';
    } while (result.has_more === true);
  }

  if (!resume || !['CERTIFY', 'PUBLISH'].includes(phase) || result.has_more === true) {
    do {
      countCall();
      const before = operation;
      result = await invokeWithUnknownCheckpoint(
        input.on_unknown,
        'CERTIFY',
        () => adapter.certify(operation),
        { record_offset: offset, next_record_offset: offset },
      );
      await checkpoint(input.on_checkpoint, 'CERTIFY', result, {
        record_offset: offset,
        next_record_offset: offset,
      });
      if (isTerminal(result)) return Object.freeze({ result, calls, records_consumed: offset, resumed: !!resume });
      operation = cursor(result);
      requireForwardProgress(before, operation, 'CERTIFY');
      phase = 'CERTIFY';
    } while (result.has_more === true);
  }

  do {
    countCall();
    const before = operation;
    result = await invokeWithUnknownCheckpoint(
      input.on_unknown,
      'PUBLISH',
      () => adapter.publish(operation),
      { record_offset: offset, next_record_offset: offset },
    );
    await checkpoint(input.on_checkpoint, 'PUBLISH', result, {
      record_offset: offset,
      next_record_offset: offset,
    });
    if (isTerminal(result)) return Object.freeze({ result, calls, records_consumed: offset, resumed: !!resume });
    operation = cursor(result);
    requireForwardProgress(before, operation, 'PUBLISH');
    phase = 'PUBLISH';
  } while (result.has_more === true);

  fail(
    'C1_PUBLICATION_TERMINAL_MISSING',
    'C1 publication ended without a published or explicitly non-progress result.',
    { status: result.status, phase: result.phase },
  );
}

export const WEEKLY_SOURCE_C1_PUBLICATION_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_C1_PUBLICATION_V1',
  automaticRetry: false,
  completeEntitlementOnly: true,
  calculatesResidual: false,
  ordinaryRootOnly: true,
  durableResume: true,
  unknownOutcomeRequiresExplicitRecovery: true,
});
