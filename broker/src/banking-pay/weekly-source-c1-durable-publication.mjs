import {
  createWeeklySourceC1Adapter,
} from './weekly-source-c1-adapter.mjs';
import {
  publishWeeklySourceC1Prepared,
} from './weekly-source-c1-publication.mjs';

const DECIMAL_INTEGER = /^-?(?:0|[1-9][0-9]*)$/;

const DATA_RPC = Object.freeze({
  stage: 'weekly_exceptional_pay_stage_c1_request_v1',
  read: 'weekly_exceptional_pay_read_c1_request_v1',
  checkpoint: 'weekly_exceptional_pay_record_c1_checkpoint_v1',
  unknown: 'weekly_exceptional_pay_record_c1_unknown_v1',
  recovery: 'weekly_exceptional_pay_record_c1_recovery_v1',
  complete: 'weekly_exceptional_pay_complete_c1_publication_v1',
});

const REQUEST_INTEGER_FIELDS = Object.freeze([
  'request_sequence',
  'expected_head_revision',
  'expected_source_count',
  'expected_component_count',
  'expected_payload_bytes',
]);

const CONTROL_INTEGER_FIELDS = Object.freeze([
  'owner_epoch',
  'sequence',
  'next_sequence',
  'source_cursor',
  'component_cursor',
  'verify_cursor',
  'rows_read',
  'rows_written',
  'work_used',
  'processed_bytes',
  'retry_after_ms',
  'input_records_consumed',
  'part_cursor',
  'verify_part_cursor',
  'verify_byte_offset',
  'head_revision',
  'retained_sequence_low',
  'retained_sequence_high',
]);

export class WeeklySourceC1DurablePublicationError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'WeeklySourceC1DurablePublicationError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, details) {
  throw new WeeklySourceC1DurablePublicationError(code, message, details);
}

function plainObject(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(code, `${label} is unavailable.`);
  }
  return value;
}

function nonEmptyText(value, code, label) {
  if (typeof value !== 'string' || value.trim() === '') fail(code, `${label} is unavailable.`);
  return value;
}

function integer(value, path) {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number' && Number.isSafeInteger(value) && !Object.is(value, -0)) {
    return BigInt(value);
  }
  if (typeof value === 'string' && DECIMAL_INTEGER.test(value)) return BigInt(value);
  fail('C1_DURABLE_INTEGER_INVALID', `${path} is not a lossless integer.`);
}

function withIntegerFields(value, fields, path) {
  const copy = { ...plainObject(value, 'C1_DURABLE_PAYLOAD_INVALID', path) };
  for (const field of fields) {
    if (copy[field] !== null && copy[field] !== undefined) {
      copy[field] = integer(copy[field], `${path}.${field}`);
    }
  }
  return Object.freeze(copy);
}

function normalizeRecord(record, index) {
  const path = `publication.records[${index}]`;
  const value = plainObject(record, 'C1_DURABLE_STREAM_INVALID', path);
  if (value.record_type === 'SOURCE') {
    return withIntegerFields(value, ['source_ordinal', 'payload_bytes', 'part_count'], path);
  }
  if (value.record_type === 'PART') {
    return withIntegerFields(value, ['source_ordinal', 'part_ordinal'], path);
  }
  if (value.record_type === 'COMPONENT') {
    return withIntegerFields(value, ['component_ordinal', 'source_ordinal'], path);
  }
  fail('C1_DURABLE_STREAM_INVALID', `${path}.record_type is unsupported.`);
}

function normalizeControlResult(result, path) {
  return withIntegerFields(result, CONTROL_INTEGER_FIELDS, path);
}

function normalizeReadEnvelope(value) {
  const envelope = plainObject(value, 'C1_DURABLE_READ_INVALID', 'C1 durable read');
  if (envelope.ok !== true || envelope.contract !== 'WEEKLY_PROTECTED_C1_READ_V1') {
    fail('C1_DURABLE_READ_INVALID', 'The durable C1 read contract is invalid.');
  }
  const publication = plainObject(
    envelope.publication,
    'C1_DURABLE_READ_INVALID',
    'C1 durable publication',
  );
  if (!Array.isArray(publication.records)) {
    fail('C1_DURABLE_STREAM_INVALID', 'The durable C1 record stream is unavailable.');
  }
  const normalized = {
    ...envelope,
    publication: Object.freeze({
      ...publication,
      request: withIntegerFields(
        publication.request,
        REQUEST_INTEGER_FIELDS,
        'publication.request',
      ),
      records: Object.freeze(publication.records.map(normalizeRecord)),
    }),
  };
  if (envelope.resume_checkpoint !== null && envelope.resume_checkpoint !== undefined) {
    const checkpoint = plainObject(
      envelope.resume_checkpoint,
      'C1_DURABLE_READ_INVALID',
      'C1 durable checkpoint',
    );
    normalized.resume_checkpoint = Object.freeze({
      ...checkpoint,
      result: normalizeControlResult(checkpoint.result, 'resume_checkpoint.result'),
    });
  }
  return Object.freeze(normalized);
}

function requireDependencies(input) {
  if (typeof input.data_rpc !== 'function') {
    fail('C1_DURABLE_DATA_RPC_INVALID', 'The CloudTMS durable RPC transport is unavailable.');
  }
  if (typeof input.c1_raw_rpc !== 'function') {
    fail('C1_DURABLE_C1_RPC_INVALID', 'The C1 lossless RPC transport is unavailable.');
  }
}

function checkpointIdempotencyKey(publicationRequestId, phase, result, offset) {
  return [
    publicationRequestId,
    'c1-checkpoint',
    phase,
    String(offset),
    String(result.next_sequence),
    result.receipt_sha256,
  ].join(':');
}

function unknownIdempotencyKey(publicationRequestId, phase, recoveryCall, offset) {
  return [
    publicationRequestId,
    'c1-unknown',
    phase,
    String(offset),
    recoveryCall.parameters_sha256,
  ].join(':');
}

async function readDurable(input) {
  const response = await input.data_rpc(DATA_RPC.read, {
    p_request: {
      schema_version: 'WEEKLY_PROTECTED_C1_READ_V1',
      actor_user_id: input.actor_user_id,
      publication_request_id: input.publication_request_id,
      expected_request_sha256: input.expected_request_sha256,
    },
  });
  return normalizeReadEnvelope(response);
}

/**
 * Continue one already-staged complete entitlement through C1.  Every known
 * C1 result is durably checkpointed before the next call.  An unknown result
 * is durably stopped and is never replayed by this function.
 */
export async function publishDurableWeeklySourceC1(input = {}) {
  requireDependencies(input);
  const actorUserId = nonEmptyText(input.actor_user_id, 'C1_DURABLE_SCOPE_INVALID', 'Office user');
  const publicationRequestId = nonEmptyText(
    input.publication_request_id,
    'C1_DURABLE_SCOPE_INVALID',
    'Publication request',
  );
  const expectedRequestSha256 = nonEmptyText(
    input.expected_request_sha256,
    'C1_DURABLE_SCOPE_INVALID',
    'Expected request digest',
  );
  const durable = await readDurable({
    ...input,
    actor_user_id: actorUserId,
    publication_request_id: publicationRequestId,
    expected_request_sha256: expectedRequestSha256,
  });
  if (durable.state === 'PUBLISHED') {
    return Object.freeze({ ok: true, outcome: 'PUBLISHED', idempotent_replay: true });
  }
  if (durable.unknown_checkpoint) {
    fail(
      'C1_DURABLE_RECOVERY_REQUIRED',
      'The previous C1 result must be resolved before publication can continue.',
      { unknown_outcome_id: durable.unknown_checkpoint.unknown_outcome_id },
    );
  }

  const adapter = createWeeklySourceC1Adapter(input.c1_raw_rpc);
  let publicationResult;
  try {
    publicationResult = await publishWeeklySourceC1Prepared({
      adapter,
      publication: durable.publication,
      resume_checkpoint: durable.resume_checkpoint ?? null,
      max_calls: input.max_calls,
      max_stage_records: input.max_stage_records,
      on_checkpoint: async (checkpoint) => input.data_rpc(DATA_RPC.checkpoint, {
        p_request: {
          schema_version: 'WEEKLY_PROTECTED_C1_CHECKPOINT_V1',
          actor_user_id: actorUserId,
          publication_request_id: publicationRequestId,
          phase: checkpoint.phase,
          record_offset: checkpoint.record_offset,
          next_record_offset: checkpoint.next_record_offset,
          result: checkpoint.result,
          idempotency_key: checkpointIdempotencyKey(
            publicationRequestId,
            checkpoint.phase,
            checkpoint.result,
            checkpoint.record_offset,
          ),
        },
      }),
      on_unknown: async (unknown) => input.data_rpc(DATA_RPC.unknown, {
        p_request: {
          schema_version: 'WEEKLY_PROTECTED_C1_UNKNOWN_V1',
          actor_user_id: actorUserId,
          publication_request_id: publicationRequestId,
          phase: unknown.phase,
          record_offset: unknown.record_offset,
          next_record_offset: unknown.next_record_offset,
          records_submitted: unknown.records_submitted ?? 0,
          error_code: unknown.error_code,
          recovery_call: unknown.recovery_call,
          idempotency_key: unknownIdempotencyKey(
            publicationRequestId,
            unknown.phase,
            unknown.recovery_call,
            unknown.record_offset,
          ),
        },
      }),
    });
  } catch (error) {
    if (error?.code === 'C1_OUTCOME_UNKNOWN') {
      fail(
        'C1_DURABLE_RECOVERY_REQUIRED',
        'The C1 result is unknown and has been saved for explicit recovery.',
        { publication_request_id: publicationRequestId },
      );
    }
    throw error;
  }

  if (publicationResult.result.status !== 'PUBLISHED') {
    return Object.freeze({
      ok: false,
      outcome: publicationResult.result.status,
      publication_request_id: publicationRequestId,
      calls: publicationResult.calls,
    });
  }
  const complete = await input.data_rpc(DATA_RPC.complete, {
    p_request: {
      schema_version: 'WEEKLY_PROTECTED_C1_COMPLETE_V1',
      actor_user_id: actorUserId,
      publication_request_id: publicationRequestId,
      expected_request_sha256: expectedRequestSha256,
      idempotency_key: [
        publicationRequestId,
        'c1-complete',
        publicationResult.result.receipt_sha256,
      ].join(':'),
    },
  });
  return Object.freeze({
    ...complete,
    calls: publicationResult.calls,
    records_consumed: publicationResult.records_consumed,
  });
}

/** Stage one complete Office-approved entitlement and publish that exact seal. */
export async function stageAndPublishDurableWeeklySourceC1(input = {}) {
  requireDependencies(input);
  const stageRequest = plainObject(
    input.stage_request,
    'C1_DURABLE_STAGE_INVALID',
    'Protected-hours approval',
  );
  const stage = await input.data_rpc(DATA_RPC.stage, { p_request: stageRequest });
  if (stage?.ok !== true || stage?.outcome !== 'STAGED') {
    fail('C1_DURABLE_STAGE_INVALID', 'The protected-hours entitlement was not staged.');
  }
  return publishDurableWeeklySourceC1({
    ...input,
    actor_user_id: stageRequest.actor_user_id,
    publication_request_id: stage.publication_request_id,
    expected_request_sha256: stage.request_sha256,
  });
}

/**
 * Perform the one explicit C1 recovery allowed by its sealed call envelope,
 * persist the decision, then continue only when that recovery was proved.
 */
export async function recoverDurableWeeklySourceC1(input = {}) {
  requireDependencies(input);
  const actorUserId = nonEmptyText(input.actor_user_id, 'C1_DURABLE_SCOPE_INVALID', 'Office user');
  const durable = await readDurable(input);
  if (!durable.unknown_checkpoint) {
    fail('C1_DURABLE_RECOVERY_NOT_REQUIRED', 'There is no unresolved C1 result to recover.');
  }
  const adapter = createWeeklySourceC1Adapter(input.c1_raw_rpc);
  const recovery = await adapter.recoverUnknown(durable.unknown_checkpoint.recovery_call);
  const recorded = await input.data_rpc(DATA_RPC.recovery, {
    p_request: {
      schema_version: 'WEEKLY_PROTECTED_C1_RECOVERY_RESULT_V1',
      actor_user_id: actorUserId,
      unknown_outcome_id: durable.unknown_checkpoint.unknown_outcome_id,
      recovery,
      idempotency_key: [
        durable.unknown_checkpoint.unknown_outcome_id,
        'c1-recovery',
        recovery.code,
      ].join(':'),
    },
  });
  if (recovery.ok !== true) return Object.freeze({ ...recorded, recovery });
  return publishDurableWeeklySourceC1(input);
}

export const WEEKLY_SOURCE_C1_DURABLE_PUBLICATION_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_C1_DURABLE_PUBLICATION_V1',
  completeEntitlementOnly: true,
  automaticRetry: false,
  unknownOutcomeRequiresExplicitRecovery: true,
  calculatesResidual: false,
  createsDraft: false,
  changesInvoice: false,
});

export const WEEKLY_SOURCE_C1_DATA_RPC_NAMES = DATA_RPC;
