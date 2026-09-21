import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  createWeeklySourceC1Adapter,
  parseWeeklySourceC1Json,
  stringifyWeeklySourceC1Json,
  WEEKLY_SOURCE_C1_RPC_NAMES,
  WeeklySourceC1AdapterInputError,
  WeeklySourceC1AdapterProtocolError,
  WeeklySourceC1UnknownOutcomeError
} from '../broker/src/banking-pay/weekly-source-c1-adapter.mjs';

const UUID = Object.freeze({
  request: '00000000-0000-4000-8000-000000000001',
  actor: '00000000-0000-4000-8000-000000000002',
  candidate: '00000000-0000-4000-8000-000000000003',
  contract: '00000000-0000-4000-8000-000000000004',
  root: '00000000-0000-4000-8000-000000000005',
  financial: '00000000-0000-4000-8000-000000000006',
  operation: '00000000-0000-4000-8000-000000000007',
  scope: '00000000-0000-4000-8000-000000000008',
  batch: '00000000-0000-4000-8000-000000000009',
  generation: '00000000-0000-4000-8000-00000000000a',
  source: '00000000-0000-4000-8000-00000000000b',
  component: '00000000-0000-4000-8000-00000000000c',
  job: '00000000-0000-4000-8000-00000000000d',
  build: '00000000-0000-4000-8000-00000000000e',
  attempt: '00000000-0000-4000-8000-00000000000f',
  nonce: '00000000-0000-4000-8000-000000000010',
  terminal: '00000000-0000-4000-8000-000000000011'
});

const H = Object.freeze({
  a: 'a'.repeat(64),
  b: 'b'.repeat(64),
  c: 'c'.repeat(64),
  d: 'd'.repeat(64),
  e: 'e'.repeat(64),
  f: 'f'.repeat(64)
});

const LARGE = 9_007_199_254_740_993n;

function startRequest(sourceMode = 'HEALTHROSTER_WEEKLY') {
  return {
    request_id: UUID.request,
    request_sequence: LARGE,
    actor_user_id: UUID.actor,
    candidate_id: UUID.candidate,
    contract_id: UUID.contract,
    root_timesheet_id: UUID.root,
    week_ending_date: '2026-09-20',
    source_mode: sourceMode,
    expected_head_revision: LARGE + 1n,
    expected_source_count: 2n,
    expected_component_count: 1n,
    expected_payload_bytes: 128n,
    source_manifest_sha256: H.a,
    entitlement_sha256: H.b,
    approval_sha256: H.c,
    is_zero_entitlement: false,
    financial_row_id: UUID.financial
  };
}

function operationCursor() {
  return {
    operation_id: UUID.operation,
    owner_epoch: LARGE,
    next_sequence: LARGE + 1n,
    receipt_sha256: H.d
  };
}

function scopeCursor() {
  return {
    scope_id: UUID.scope,
    owner_epoch: LARGE,
    next_sequence: LARGE + 1n,
    receipt_sha256: H.d
  };
}

function scopeRequest() {
  return {
    scope_id: UUID.scope,
    r01_operation_id: UUID.operation,
    r01_request_id: UUID.request,
    pay_batch_id: UUID.batch,
    actor_user_id: UUID.actor,
    base_source_identity_sha256: H.a,
    selection_sha256: H.b,
    source_build_count: LARGE,
    source_root_count: LARGE + 1n,
    request_sha256: H.c
  };
}

function sourceRecord() {
  return {
    source_ordinal: 1n,
    source_id: UUID.source,
    authority_kind: 'CLIENT_SOURCE',
    source_system: 'MAGNIT',
    external_identity: 'source-row-1',
    external_revision: '1',
    source_document_sha256: H.a,
    payload_bytes: 2n,
    part_count: 1n,
    work_date: '2026-09-14',
    root_timesheet_id: UUID.root,
    candidate_id: UUID.candidate,
    contract_id: UUID.contract,
    source_row_sha256: H.b,
    record_type: 'SOURCE'
  };
}

function partRecord() {
  return {
    source_ordinal: 1n,
    part_ordinal: 1n,
    payload_utf8: '7b7d',
    fragment_sha256: H.c,
    record_type: 'PART'
  };
}

function componentRecord() {
  return {
    component_ordinal: 1n,
    component_id: UUID.component,
    source_ordinal: 1n,
    source_key: '01:worked-time',
    component_kind: 'WORKED_TIME',
    economic_key_type: 'TS_DAY',
    economic_key_value: '2026-09-14',
    component_member_identity: 'member-1',
    segment_id: null,
    segment_key: null,
    segment_stable_key: null,
    work_date: '2026-09-14',
    reference_number: null,
    hours_day: '7.500000',
    hours_night: '0.000000',
    hours_sat: '0.000000',
    hours_sun: '0.000000',
    hours_bh: '0.000000',
    additional_code_raw: null,
    unit_count: '7.500000',
    unit_pay_rate: '20.000000',
    unit_charge_rate: '30.000000',
    expense_code: null,
    pay_ex_vat: '150.00',
    charge_ex_vat: '225.00',
    exclude_from_pay: false,
    origin: 'SOURCE',
    component_sha256: H.e,
    record_type: 'COMPONENT'
  };
}

function control(overrides = {}) {
  return {
    contract: 'WEEKLY_SOURCE_C1_V1',
    ok: true,
    status: 'PROGRESS',
    code: 'C1_OK',
    operation_id: UUID.operation,
    scope_id: UUID.scope,
    owner_epoch: LARGE,
    sequence: LARGE,
    next_sequence: LARGE + 1n,
    phase: 'SOURCES',
    source_cursor: 0n,
    component_cursor: 0n,
    verify_cursor: 0n,
    rows_read: 0n,
    rows_written: 1n,
    work_used: 1n,
    processed_bytes: 128n,
    has_more: true,
    replayed: false,
    retry_after_ms: 0n,
    request_sha256: H.a,
    checkpoint_sha256: H.b,
    receipt_sha256: H.c,
    publication_id: null,
    head_revision: null,
    source_identity_sha256: null,
    operation_created: true,
    input_records_consumed: 1n,
    part_cursor: 0n,
    verify_part_cursor: 0n,
    verify_byte_offset: 0n,
    ...overrides
  };
}

function statusResult(overrides = {}) {
  const base = control();
  delete base.input_records_consumed;
  delete base.part_cursor;
  delete base.verify_part_cursor;
  delete base.verify_byte_offset;
  return {
    ...base,
    state: 'STAGING',
    terminal_receipt_sha256: null,
    retained_sequence_low: LARGE,
    retained_sequence_high: LARGE,
    ...overrides
  };
}

function scopeStatus(overrides = {}) {
  return {
    scope_id: UUID.scope,
    state: 'BUILDING',
    owner_epoch: LARGE,
    next_sequence: LARGE + 1n,
    visited_root_count: 1n,
    source_root_count: 1n,
    protected_root_count: 1n,
    held_freeze_count: 1n,
    held_evidence_count: 1n,
    r01_generation_id: null,
    begin_receipt_sha256: null,
    last_receipt_sha256: `\\x${H.d}`,
    release_phase: 'NONE',
    release_member_cursor: 0n,
    ...overrides
  };
}

function recorder(handler = null) {
  const calls = [];
  const rpc = async (name, parametersJson, options) => {
    calls.push({ name, parametersJson, options });
    if (handler) return handler({ name, parametersJson, options, calls });
    if (name === WEEKLY_SOURCE_C1_RPC_NAMES.status) return stringifyWeeklySourceC1Json(statusResult());
    if (name === WEEKLY_SOURCE_C1_RPC_NAMES.sourceScopeStatus) return stringifyWeeklySourceC1Json(scopeStatus());
    if (name === WEEKLY_SOURCE_C1_RPC_NAMES.beginR01) {
      return stringifyWeeklySourceC1Json({ contract: 'R01', generation_id: UUID.generation, unit_sequence: LARGE });
    }
    if (name === WEEKLY_SOURCE_C1_RPC_NAMES.executeWorkbenchAttempt) {
      return stringifyWeeklySourceC1Json({ ok: true, has_more: false, attempt_number: LARGE });
    }
    return stringifyWeeklySourceC1Json(control());
  };
  return { rpc, calls };
}

test('C1 adapter maps every sealed method to its exact public RPC and preserves bigint tokens', async () => {
  const captured = recorder();
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  const op = operationCursor();
  const scope = scopeCursor();

  await adapter.start(startRequest());
  await adapter.stage(op, [sourceRecord(), partRecord(), componentRecord()]);
  await adapter.continueValidation(op);
  await adapter.certify(op);
  await adapter.publish(op);
  await adapter.setPending(op, 'OFFICE_WAIT');
  await adapter.abort(op, 'OFFICE_CANCELLED');
  await adapter.retire(op, H.e);
  await adapter.repair(op, H.f);
  await adapter.status(UUID.operation);
  await adapter.prepareSourceScope(scopeRequest());
  await adapter.continueSourceScope(scope);
  await adapter.beginR01({
    p_operation_id: UUID.operation,
    p_request_id: UUID.request,
    p_pay_batch_id: UUID.batch,
    p_actor_user_id: UUID.actor,
    p_base_generation_id: UUID.generation,
    p_base_revision: LARGE,
    p_kind: 'DRAFT_CREATE',
    p_declared_count: LARGE + 1n,
    p_policy_sha256: H.a,
    p_source_identity_sha256: H.b,
    p_selection_sha256: H.c
  });
  await adapter.sourceScopeStatus(UUID.scope);
  await adapter.abortSourceScope(scope, H.e);
  await adapter.releaseSourceScope(scope, UUID.terminal, H.f);
  await adapter.executeWorkbenchAttempt({
    p_job_id: UUID.job,
    p_build_id: UUID.build,
    p_private_stage: 'RECONCILE',
    p_attempt_id: UUID.attempt,
    p_attempt_nonce: UUID.nonce,
    p_worker_id: 'worker-1',
    p_lane_identity: 'lane-1'
  });
  await adapter.cleanup({
    stream_id: 1n,
    owner_epoch: LARGE,
    next_sequence: LARGE + 1n,
    receipt_sha256: H.d
  });

  assert.deepEqual(captured.calls.map((entry) => entry.name), [
    'weekly_source_start_c1',
    'weekly_source_stage_c1',
    'weekly_source_continue_c1',
    'weekly_source_certify_c1',
    'weekly_source_publish_c1',
    'weekly_source_pending_c1',
    'weekly_source_abort_c1',
    'weekly_source_retire_c1',
    'weekly_source_repair_c1',
    'weekly_source_status_c1',
    'weekly_source_scope_start_c1',
    'weekly_source_scope_continue_c1',
    'weekly_source_begin_r01_c1',
    'weekly_source_scope_status_c1',
    'weekly_source_scope_abort_c1',
    'weekly_source_scope_release_c1',
    'weekly_source_workbench_execute_c1',
    'weekly_source_cleanup_c1'
  ]);
  assert.match(captured.calls[0].parametersJson, /"request_sequence":9007199254740993/);
  assert.doesNotMatch(captured.calls[0].parametersJson, /"request_sequence":"9007199254740993"/);
  assert.match(captured.calls[1].parametersJson, /"p_sequence":9007199254740994/);
  assert.match(captured.calls[1].parametersJson, /"source_system":"MAGNIT"/);
  assert.match(captured.calls[12].parametersJson, /"p_policy_sha256":"\\\\x[a-f0-9]{64}"/);
  assert(captured.calls.every((entry) => entry.options.automaticRetry === false));
});

test('C1 adapter public surface retains every exact contract-08 method name and arity', () => {
  const prototype = Object.getPrototypeOf(createWeeklySourceC1Adapter(async () => '{}'));
  const expectedArities = {
    abort: 2,
    abortSourceScope: 2,
    beginR01: 1,
    certify: 1,
    cleanup: 1,
    continueSourceScope: 1,
    continueValidation: 1,
    executeWorkbenchAttempt: 1,
    prepareSourceScope: 1,
    publish: 1,
    recoverUnknown: 1,
    releaseSourceScope: 3,
    repair: 2,
    retire: 2,
    setPending: 2,
    sourceScopeStatus: 1,
    stage: 2,
    start: 1,
    status: 1
  };
  const actualNames = Object.getOwnPropertyNames(prototype)
    .filter((name) => name !== 'constructor')
    .sort();
  assert.deepEqual(actualNames, Object.keys(expectedArities).sort());
  for (const [name, arity] of Object.entries(expectedArities)) assert.equal(prototype[name].length, arity, name);
});

test('C1 adapter returns exact large integer values as BigInt', async () => {
  const captured = recorder();
  const result = await createWeeklySourceC1Adapter(captured.rpc).start(startRequest());
  assert.equal(result.owner_epoch, LARGE);
  assert.equal(result.next_sequence, LARGE + 1n);
  assert.equal(typeof result.owner_epoch, 'bigint');
});

test('C1 adapter accepts Magnit only through generic HEALTHROSTER_WEEKLY mode', async () => {
  const captured = recorder();
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  await adapter.start(startRequest('HEALTHROSTER_WEEKLY'));
  await assert.rejects(
    () => adapter.start(startRequest('MAGNIT_WEEKLY')),
    (error) => error instanceof WeeklySourceC1AdapterInputError && error.code === 'C1_INPUT_INVALID'
  );
  assert.equal(captured.calls.length, 1);
});

test('C1 adapter fails closed on unknown fields, unsafe Numbers and malformed stage rows before RPC', async () => {
  const captured = recorder();
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  await assert.rejects(() => adapter.start({ ...startRequest(), unexpected: true }), WeeklySourceC1AdapterInputError);
  await assert.rejects(
    () => adapter.start({ ...startRequest(), request_sequence: Number(LARGE) }),
    WeeklySourceC1AdapterInputError
  );
  await assert.rejects(
    () => adapter.stage(operationCursor(), [{ ...sourceRecord(), payload_bytes: 0n }]),
    WeeklySourceC1AdapterInputError
  );
  await assert.rejects(
    () => adapter.stage(operationCursor(), [{ ...componentRecord(), pay_ex_vat: '150.0' }]),
    WeeklySourceC1AdapterInputError
  );
  await assert.rejects(
    () => adapter.continueValidation({ ...operationCursor(), candidate_id: UUID.candidate }),
    WeeklySourceC1AdapterInputError
  );
  assert.equal(captured.calls.length, 0);
});

test('C1 adapter refuses a parsed RPC response or a non-closed control envelope', async () => {
  const parsedAdapter = createWeeklySourceC1Adapter(async () => control());
  await assert.rejects(() => parsedAdapter.start(startRequest()), WeeklySourceC1AdapterProtocolError);

  const extraAdapter = createWeeklySourceC1Adapter(async () => stringifyWeeklySourceC1Json({ ...control(), extra: true }));
  await assert.rejects(() => extraAdapter.start(startRequest()), WeeklySourceC1AdapterProtocolError);

  const nonCanonicalIntegerText = stringifyWeeklySourceC1Json(control()).replace(
    '"owner_epoch":9007199254740993',
    '"owner_epoch":9007199254740993.0'
  );
  const nonCanonicalAdapter = createWeeklySourceC1Adapter(async () => nonCanonicalIntegerText);
  await assert.rejects(() => nonCanonicalAdapter.start(startRequest()), WeeklySourceC1AdapterProtocolError);
});

test('a response-backed known RPC refusal is propagated and is never relabelled UNKNOWN', async () => {
  const refusal = Object.assign(new Error('C1_ACTOR_NOT_AUTHORISED'), {
    code: 'C1_ACTOR_NOT_AUTHORISED',
    outcomeKnown: true,
    status: 403
  });
  const adapter = createWeeklySourceC1Adapter(async () => { throw refusal; });
  await assert.rejects(() => adapter.start(startRequest()), (error) => error === refusal);
});

test('C1 adapter makes no hidden retry and provides a digest-bound UNKNOWN recovery record', async () => {
  const failure = Object.assign(new Error('connection lost'), { unknownOutcome: true });
  const captured = recorder(async () => { throw failure; });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  await assert.rejects(
    () => adapter.stage(operationCursor(), [sourceRecord()]),
    (error) => {
      assert(error instanceof WeeklySourceC1UnknownOutcomeError);
      assert.equal(error.recoveryCall.method, 'stage');
      assert.equal(error.recoveryCall.stream_kind, 'OPERATION');
      assert.equal(error.recoveryCall.stream_id, UUID.operation);
      assert.match(error.recoveryCall.parameters_sha256, /^[a-f0-9]{64}$/);
      assert.equal(error.recoveryCall.recovery_attempted, false);
      return true;
    }
  );
  assert.equal(captured.calls.length, 1);
});

test('UNKNOWN operation recovery checks status first and returns a committed receipt without replay', async () => {
  let savedCall;
  const captured = recorder(async ({ name, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    assert.equal(name, 'weekly_source_status_c1');
    return stringifyWeeklySourceC1Json(statusResult({
      next_sequence: LARGE + 2n,
      sequence: LARGE + 1n,
      receipt_sha256: H.e
    }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.continueValidation(operationCursor());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.decision, 'COMMITTED');
  assert.equal(recovered.replay_attempted, false);
  assert.equal(captured.calls.length, 2);
  assert.deepEqual(captured.calls.map((call) => call.name), [
    'weekly_source_continue_c1',
    'weekly_source_status_c1'
  ]);
});

test('UNKNOWN partial STAGE recovery exact-replays the committed request to recover its accepted prefix', async () => {
  let savedCall;
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    if (calls.length === 1) {
      assert.equal(name, 'weekly_source_stage_c1');
      throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    }
    if (calls.length === 2) {
      assert.equal(name, 'weekly_source_status_c1');
      return stringifyWeeklySourceC1Json(statusResult({
        next_sequence: LARGE + 2n,
        sequence: LARGE + 1n,
        receipt_sha256: H.e
      }));
    }
    assert.equal(name, 'weekly_source_stage_c1');
    assert.equal(parametersJson, savedCall.parameters_json);
    return stringifyWeeklySourceC1Json(control({
      sequence: LARGE + 1n,
      next_sequence: LARGE + 2n,
      input_records_consumed: 1n,
      replayed: true,
      receipt_sha256: H.e
    }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.stage(operationCursor(), [sourceRecord(), partRecord()]);
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.decision, 'REPLAYED');
  assert.equal(recovered.code, 'C1_RECOVERY_STAGE_COMMITTED_EXACT_REPLAY');
  assert.equal(recovered.replay_attempted, true);
  assert.equal(recovered.result.input_records_consumed, 1n);
  assert.equal(recovered.result.replayed, true);
  assert.deepEqual(captured.calls.map((call) => call.name), [
    'weekly_source_stage_c1',
    'weekly_source_status_c1',
    'weekly_source_stage_c1'
  ]);
});

test('UNKNOWN partial STAGE replay can become unknown only once and cannot enter a recovery loop', async () => {
  let savedCall;
  const captured = recorder(async ({ name, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('initial timeout'), { unknownOutcome: true });
    if (calls.length === 2) {
      assert.equal(name, 'weekly_source_status_c1');
      return stringifyWeeklySourceC1Json(statusResult({
        next_sequence: LARGE + 2n,
        sequence: LARGE + 1n,
        receipt_sha256: H.e
      }));
    }
    assert.equal(name, 'weekly_source_stage_c1');
    throw Object.assign(new Error('replay timeout'), { unknownOutcome: true });
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.stage(operationCursor(), [sourceRecord()]);
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  let attemptedCall;
  await assert.rejects(
    () => adapter.recoverUnknown(savedCall),
    (error) => {
      assert(error instanceof WeeklySourceC1UnknownOutcomeError);
      attemptedCall = error.recoveryCall;
      assert.equal(attemptedCall.recovery_attempted, true);
      return true;
    }
  );
  const refused = await adapter.recoverUnknown(attemptedCall);
  assert.equal(refused.decision, 'REFUSED');
  assert.equal(refused.code, 'C1_RECOVERY_ALREADY_ATTEMPTED');
  assert.equal(refused.replay_attempted, false);
  assert.equal(captured.calls.length, 3);
});

test('UNKNOWN operation recovery replays the identical request once only after status proves it absent', async () => {
  let savedCall;
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    if (name === 'weekly_source_status_c1') {
      return stringifyWeeklySourceC1Json(statusResult({
        next_sequence: LARGE + 1n,
        sequence: LARGE,
        receipt_sha256: H.d
      }));
    }
    assert.equal(name, 'weekly_source_continue_c1');
    assert.equal(parametersJson, savedCall.parameters_json);
    return stringifyWeeklySourceC1Json(control({ sequence: LARGE + 1n, next_sequence: LARGE + 2n }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.continueValidation(operationCursor());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.decision, 'REPLAYED');
  assert.equal(recovered.replay_attempted, true);
  assert.deepEqual(captured.calls.map((call) => call.name), [
    'weekly_source_continue_c1',
    'weekly_source_status_c1',
    'weekly_source_continue_c1'
  ]);
});

test('UNKNOWN start recovery uses the sealed exact-request replay exception and never invents another identity', async () => {
  let savedCall;
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    assert.equal(name, 'weekly_source_start_c1');
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    assert.equal(parametersJson, savedCall.parameters_json);
    return stringifyWeeklySourceC1Json(control({ replayed: true }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.start(startRequest());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.code, 'C1_RECOVERY_START_EXACT_REPLAY');
  assert.equal(recovered.result.replayed, true);
  assert.equal(captured.calls.length, 2);
});

test('UNKNOWN source-scope start recovery reads status then exact-replays so SQL proves the saved digest', async () => {
  let savedCall;
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    if (calls.length === 2) {
      assert.equal(name, 'weekly_source_scope_status_c1');
      return stringifyWeeklySourceC1Json(scopeStatus());
    }
    assert.equal(name, 'weekly_source_scope_start_c1');
    assert.equal(parametersJson, savedCall.parameters_json);
    return stringifyWeeklySourceC1Json(control({ operation_id: null, scope_id: UUID.scope, replayed: true }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.prepareSourceScope(scopeRequest());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.code, 'C1_RECOVERY_SCOPE_START_VERIFIED_REPLAY');
  assert.equal(recovered.replay_attempted, true);
  assert.equal(recovered.result.replayed, true);
  assert.deepEqual(captured.calls.map((call) => call.name), [
    'weekly_source_scope_start_c1',
    'weekly_source_scope_status_c1',
    'weekly_source_scope_start_c1'
  ]);
});

test('UNKNOWN source-scope start recovery replays once only after a known not-found status', async () => {
  let savedCall;
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    if (calls.length === 2) {
      assert.equal(name, 'weekly_source_scope_status_c1');
      throw Object.assign(new Error('not found'), { outcomeKnown: true, status: 404 });
    }
    assert.equal(name, 'weekly_source_scope_start_c1');
    assert.equal(parametersJson, savedCall.parameters_json);
    return stringifyWeeklySourceC1Json(control({ operation_id: null, scope_id: UUID.scope, replayed: true }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.prepareSourceScope(scopeRequest());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.code, 'C1_RECOVERY_SCOPE_START_EXACT_REPLAY');
  assert.equal(recovered.replay_attempted, true);
  assert.equal(captured.calls.length, 3);
});

test('UNKNOWN source-scope start recovery surfaces UUID collision refusal after its one exact replay', async () => {
  let savedCall;
  const collision = Object.assign(new Error('C1_REQUEST_COLLISION'), {
    code: 'C1_REQUEST_COLLISION',
    outcomeKnown: true,
    status: 409
  });
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    if (calls.length === 2) {
      assert.equal(name, 'weekly_source_scope_status_c1');
      return stringifyWeeklySourceC1Json(scopeStatus());
    }
    assert.equal(name, 'weekly_source_scope_start_c1');
    assert.equal(parametersJson, savedCall.parameters_json);
    throw collision;
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.prepareSourceScope(scopeRequest());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  await assert.rejects(() => adapter.recoverUnknown(savedCall), (error) => error === collision);
  assert.deepEqual(captured.calls.map((call) => call.name), [
    'weekly_source_scope_start_c1',
    'weekly_source_scope_status_c1',
    'weekly_source_scope_start_c1'
  ]);
});

test('UNKNOWN source-scope start recovery never performs a second replay after its exact replay is UNKNOWN', async () => {
  let savedCall;
  let recoveryCall;
  const captured = recorder(async ({ name, parametersJson, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('initial timeout'), { unknownOutcome: true });
    if (calls.length === 2) {
      assert.equal(name, 'weekly_source_scope_status_c1');
      return stringifyWeeklySourceC1Json(scopeStatus());
    }
    assert.equal(name, 'weekly_source_scope_start_c1');
    assert.equal(parametersJson, savedCall.parameters_json);
    throw Object.assign(new Error('replay timeout'), { unknownOutcome: true });
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.prepareSourceScope(scopeRequest());
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  await assert.rejects(
    () => adapter.recoverUnknown(savedCall),
    (error) => {
      assert(error instanceof WeeklySourceC1UnknownOutcomeError);
      assert.equal(error.recoveryCall.recovery_attempted, true);
      recoveryCall = error.recoveryCall;
      return true;
    }
  );
  assert.equal(captured.calls.length, 3);
  const refused = await adapter.recoverUnknown(recoveryCall);
  assert.equal(refused.decision, 'REFUSED');
  assert.equal(refused.code, 'C1_RECOVERY_ALREADY_ATTEMPTED');
  assert.equal(refused.replay_attempted, false);
  assert.equal(captured.calls.length, 3);
});

test('UNKNOWN recovery refuses body tampering, checkpoint conflict and a second replay attempt', async () => {
  let savedCall;
  const captured = recorder(async ({ name, calls }) => {
    if (calls.length === 1) throw Object.assign(new Error('timeout'), { unknownOutcome: true });
    assert.equal(name, 'weekly_source_status_c1');
    return stringifyWeeklySourceC1Json(statusResult({
      next_sequence: LARGE + 1n,
      receipt_sha256: H.e
    }));
  });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.continueValidation(operationCursor());
  } catch (error) {
    savedCall = error.recoveryCall;
  }

  await assert.rejects(
    () => adapter.recoverUnknown({ ...savedCall, parameters_json: `${savedCall.parameters_json} ` }),
    WeeklySourceC1AdapterInputError
  );
  await assert.rejects(
    () => adapter.recoverUnknown({ ...savedCall, stream_id: UUID.scope }),
    WeeklySourceC1AdapterInputError
  );
  const conflict = await adapter.recoverUnknown(savedCall);
  assert.equal(conflict.decision, 'REFUSED');
  assert.equal(conflict.code, 'C1_RECOVERY_CHECKPOINT_CONFLICT');
  const repeated = await adapter.recoverUnknown({ ...savedCall, recovery_attempted: true });
  assert.equal(repeated.code, 'C1_RECOVERY_ALREADY_ATTEMPTED');
});

test('UNKNOWN passthrough outcome refuses recovery instead of bypassing its owning workflow', async () => {
  let savedCall;
  const captured = recorder(async () => { throw Object.assign(new Error('timeout'), { unknownOutcome: true }); });
  const adapter = createWeeklySourceC1Adapter(captured.rpc);
  try {
    await adapter.executeWorkbenchAttempt({
      p_job_id: UUID.job,
      p_build_id: UUID.build,
      p_private_stage: 'RECONCILE',
      p_attempt_id: UUID.attempt,
      p_attempt_nonce: UUID.nonce,
      p_worker_id: 'worker-1',
      p_lane_identity: 'lane-1'
    });
  } catch (error) {
    savedCall = error.recoveryCall;
  }
  const recovered = await adapter.recoverUnknown(savedCall);
  assert.equal(recovered.decision, 'REFUSED');
  assert.equal(recovered.code, 'C1_RECOVERY_STATUS_ID_UNAVAILABLE');
  assert.equal(captured.calls.length, 1);
});

test('passthrough Workbench result is not subjected to the C1 32 KiB control limit or financial rewriting', async () => {
  const payload = { ok: true, opaque: 'x'.repeat(40_000), amount: 'unchanged', revision: LARGE };
  const captured = recorder(async () => stringifyWeeklySourceC1Json(payload));
  const result = await createWeeklySourceC1Adapter(captured.rpc).executeWorkbenchAttempt({
    p_job_id: UUID.job,
    p_build_id: UUID.build,
    p_private_stage: 'RECONCILE',
    p_attempt_id: UUID.attempt,
    p_attempt_nonce: UUID.nonce,
    p_worker_id: 'worker-1',
    p_lane_identity: 'lane-1'
  });
  assert.equal(result.opaque.length, 40_000);
  assert.equal(result.amount, 'unchanged');
  assert.equal(result.revision, LARGE);
});

test('adapter source contains no browser route, fetch, direct table access or financial calculation', async () => {
  const source = await readFile(
    new URL('../broker/src/banking-pay/weekly-source-c1-adapter.mjs', import.meta.url),
    'utf8'
  );
  assert.doesNotMatch(source, /\bfetch\s*\(/);
  assert.doesNotMatch(source, /\/api\//);
  assert.doesNotMatch(source, /from\s+public\.|insert\s+into|update\s+public\.|delete\s+from/i);
  assert.doesNotMatch(source, /pay_ex_vat\s*[+*\/-]|charge_ex_vat\s*[+*\/-]/);
});

test('lossless JSON codec refuses unsafe Number input and round-trips signed bigint boundaries', () => {
  const text = stringifyWeeklySourceC1Json({ min: -(2n ** 63n), max: (2n ** 63n) - 1n });
  assert.equal(text, '{"min":-9223372036854775808,"max":9223372036854775807}');
  assert.deepEqual(parseWeeklySourceC1Json(text), {
    min: -(2n ** 63n),
    max: (2n ** 63n) - 1n
  });
  assert.throws(() => stringifyWeeklySourceC1Json({ unsafe: Number.MAX_SAFE_INTEGER + 1 }), WeeklySourceC1AdapterInputError);
  assert.throws(() => stringifyWeeklySourceC1Json({ overflow: 2n ** 63n }), WeeklySourceC1AdapterInputError);
});
