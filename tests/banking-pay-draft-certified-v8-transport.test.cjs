const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');

const MODULE_URL = new URL('../broker/src/banking-pay-draft-certified-v8.js', `file://${__filename}`);
const root = path.resolve(__dirname, '..');
const workerSource = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const certificateSchema = fs.readFileSync(path.join(root, 'supabase', 'migrations', '02092026_2300_banking_pay_workbench_settled_certificate_v8.sql'), 'utf8');
const certificateBuild = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql'), 'utf8');
const certificateReaders = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '02092026_2302_banking_pay_workbench_settled_certificate_digest_reader_v8.sql'), 'utf8');

const sha = (character) => character.repeat(64);

function validEnvelope() {
  const overall = sha('a');
  return {
    certificate_reference: {
      candidate_filter_id: null,
      certification_id: `WORKBENCH_SETTLED_CERTIFICATION_V2:${overall}`,
      client_filter_id: null,
      filter_context_digest_sha256: sha('b'),
      idempotency_key: 'draft-create:certified:test',
      manifest_digest_sha256: sha('c'),
      overall_digest_sha256: overall,
      pay_channel_scope: 'ALL',
      same_week_paye_override: {
        continue: false,
        verified: false,
        used: false,
        pay_date: '2026-09-04',
        pay_week_start: '2026-08-31',
        pay_week_end: '2026-09-06',
        reason: null,
        verified_by_user_id: null,
        verified_at_utc: null,
        reauth_purpose: null,
        guardrail_code: null
      }
    },
    pre_admission_scope_facts: {
      authority_fence_generation: 9,
      certificate_uuid: '10000000-0000-4000-8000-000000000001',
      certification_id: `WORKBENCH_SETTLED_CERTIFICATION_V2:${overall}`,
      manifest_digest_sha256: sha('c'),
      overall_digest_sha256: overall,
      pay_channel_scope: 'ALL',
      pay_date: '2026-09-04',
      pay_week_end: '2026-09-06',
      pay_week_start: '2026-08-31',
      progress_counter_version: 12,
      scope_facts_contract: 'WORKBENCH_SETTLED_CERTIFICATE_PRE_ADMISSION_SCOPE_FACTS_V1',
      selected_ready_for_request: 5000,
      selected_ready_paye: 2500,
      selected_ready_total: 5000,
      selected_ready_umbrella: 2500,
      session_version: 7,
      week_ending_cutoff: '2026-09-06',
      workbench_session_id: '20000000-0000-4000-8000-000000000002'
    }
  };
}

test('the certified Draft transport accepts the 5,000-row load-test envelope without row arrays', async () => {
  const api = await import(MODULE_URL.href);
  const validated = api.validateCurrentCertificateIssuerEnvelopeV8(validEnvelope());
  assert.equal(validated.ok, true);
  assert.equal(validated.selected_ready_for_request, 5000);
  const input = api.buildCertifiedDraftOperationInputV8(validated);
  assert.equal(api.isCertifiedDraftOperationInputV8(input), true);
  assert.equal(input.selected_preview_row_count, 5000);
  assert.deepEqual(api.findExpandedSelectionKeys(input), []);
  assert.ok(JSON.stringify(input).length < 5000);
});

test('the configured 50,000 ceiling is accepted as a scalar contract without materialising rows', async () => {
  const api = await import(MODULE_URL.href);
  const envelope = validEnvelope();
  envelope.pre_admission_scope_facts.selected_ready_for_request = 50000;
  envelope.pre_admission_scope_facts.selected_ready_total = 50000;
  envelope.pre_admission_scope_facts.selected_ready_paye = 25000;
  envelope.pre_admission_scope_facts.selected_ready_umbrella = 25000;
  const validated = api.validateCurrentCertificateIssuerEnvelopeV8(envelope);
  assert.equal(validated.ok, true);
  const input = api.buildCertifiedDraftOperationInputV8(validated);
  assert.equal(input.selected_preview_row_count, 50000);
  assert.deepEqual(api.findExpandedSelectionKeys(input), []);
  assert.ok(JSON.stringify(input).length < 5000);
});

test('50,001 selected constituents fail before operation admission', async () => {
  const api = await import(MODULE_URL.href);
  const envelope = validEnvelope();
  envelope.pre_admission_scope_facts.selected_ready_for_request = 50001;
  envelope.pre_admission_scope_facts.selected_ready_total = 50001;
  envelope.pre_admission_scope_facts.selected_ready_paye = 25001;
  const result = api.validateCurrentCertificateIssuerEnvelopeV8(envelope);
  assert.equal(result.ok, false);
  assert.equal(result.code, 'BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_INVALID');

  const issuerValidation = workerSource.indexOf('const validated = validateCurrentCertificateIssuerEnvelopeV8(raw);');
  const operationStart = workerSource.indexOf("const started = unwrapRpc(await sbRpc(env, 'banking_pay_draft_certified_operation_start_v8'");
  assert.ok(issuerValidation >= 0 && operationStart > issuerValidation,
    'the Worker must validate the scalar ceiling before creating a Draft operation');
  assert.match(certificateBuild, /LIMIT\s+50001[\s\S]*?v_selected_count\s*>\s*50000[\s\S]*?WORKBENCH_CERTIFICATE_SELECTED_LIMIT_EXCEEDED/i);
  assert.match(certificateReaders, /v_channel\.constituent_count\s+NOT\s+BETWEEN\s+1\s+AND\s+50000/i);
  for (const requiredConstraint of [
    /selected_row_count" integer NOT NULL CHECK \(selected_row_count BETWEEN 1 AND 50000\)/,
    /selected_constituent_count" integer NOT NULL CHECK \(selected_constituent_count BETWEEN 1 AND 50000\)/,
    /constituent_count" integer NOT NULL CHECK \(constituent_count BETWEEN 0 AND 50000\)/
  ]) assert.match(certificateSchema, requiredConstraint);
  assert.doesNotMatch(`${certificateSchema}\n${certificateBuild}`, /generate_series\s*\([^)]*(?:50000|50001)/i);
});

test('PAYE and Umbrella subset counts must match the exact requested scope', async () => {
  const api = await import(MODULE_URL.href);
  const envelope = validEnvelope();
  envelope.certificate_reference.pay_channel_scope = 'PAYE';
  envelope.pre_admission_scope_facts.pay_channel_scope = 'PAYE';
  envelope.pre_admission_scope_facts.selected_ready_for_request = 2500;
  assert.equal(api.validateCurrentCertificateIssuerEnvelopeV8(envelope).ok, true);
  envelope.pre_admission_scope_facts.selected_ready_for_request = 2499;
  assert.equal(api.validateCurrentCertificateIssuerEnvelopeV8(envelope).code,
    'BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_MISMATCH');
});

test('the envelope fails closed on digest, session, pay-period and shape drift', async () => {
  const api = await import(MODULE_URL.href);
  for (const mutate of [
    (value) => { value.pre_admission_scope_facts.overall_digest_sha256 = sha('d'); },
    (value) => { value.pre_admission_scope_facts.session_version = 0; },
    (value) => { value.pre_admission_scope_facts.pay_week_start = '2026-09-01'; },
    (value) => { value.pre_admission_scope_facts.pay_date = '2026-02-31'; },
    (value) => { value.certificate_reference.same_week_paye_override.pay_date = '2026-09-05'; },
    (value) => { value.certificate_reference.same_week_paye_override.continue = true; },
    (value) => { value.unexpected = true; },
    (value) => { value.certificate_reference.unexpected = true; },
    (value) => { value.pre_admission_scope_facts.unexpected = true; }
  ]) {
    const envelope = validEnvelope();
    mutate(envelope);
    assert.equal(api.validateCurrentCertificateIssuerEnvelopeV8(envelope).ok, false);
  }
});

test('a verified PAYE override retains the exact existing ceremony without adding policy', async () => {
  const api = await import(MODULE_URL.href);
  const envelope = validEnvelope();
  envelope.certificate_reference.same_week_paye_override = {
    continue: true,
    verified: true,
    used: true,
    pay_date: '2026-09-04',
    pay_week_start: '2026-08-31',
    pay_week_end: '2026-09-06',
    reason: 'Approved existing same-week exception',
    verified_by_user_id: 'f0000000-0000-8000-c000-000000000001',
    verified_at_utc: '2026-09-03T10:30:00.000Z',
    reauth_purpose: 'PAYE_SAME_WEEK_OVERRIDE',
    guardrail_code: 'PAYE_SAME_WEEK_OVERRIDE_REQUIRED'
  };
  assert.equal(api.validateCurrentCertificateIssuerEnvelopeV8(envelope).ok, true);
  envelope.certificate_reference.same_week_paye_override.guardrail_code = 'DIFFERENT_POLICY';
  assert.equal(api.validateCurrentCertificateIssuerEnvelopeV8(envelope).code,
    'BANKING_PAY_DRAFT_CERTIFIED_OVERRIDE_CONTEXT_INVALID');
});

test('no certified path can reintroduce browser or Worker selection arrays', async () => {
  const api = await import(MODULE_URL.href);
  for (const key of [
    'selected_preview_row_ids',
    'draft_selected_preview_row_contracts',
    'selected_economic_keys',
    'expected_workbench_selected_preview_row_ids'
  ]) {
    const envelope = validEnvelope();
    envelope.pre_admission_scope_facts[key] = [];
    const result = api.validateCurrentCertificateIssuerEnvelopeV8(envelope);
    assert.equal(result.ok, false);
  }
  assert.deepEqual(api.findExpandedSelectionKeys({ nested: { selected_preview_row_ids: [] } }), [
    'selected_preview_row_ids'
  ]);
});

test('legacy input is never silently treated as the certified V8 route', async () => {
  const api = await import(MODULE_URL.href);
  assert.equal(api.isCertifiedDraftOperationInputV8({ selected_preview_row_ids: ['legacy'] }), false);
  assert.equal(api.isCertifiedDraftOperationInputV8({
    certified_draft_contract: api.BANKING_PAY_DRAFT_CERTIFIED_INPUT_CONTRACT,
    workbench_settled_certificate_reference_v8: validEnvelope().certificate_reference,
    selected_preview_row_ids: []
  }), false);
});

test('the queue runner recognises only the exact database-admitted compact projection', async () => {
  const api = await import(MODULE_URL.href);
  const reference = validEnvelope().certificate_reference;
  const projection = {
    workbench_settled_certificate_reference_v8: reference,
    pay_channel_scope: 'ALL',
    draft_scope: 'ALL',
    rail_provider_snapshot: 'REVOLUT',
    rail_env_snapshot: 'PROD',
    same_week_paye_override: reference.same_week_paye_override
  };
  const validated = api.validateCertifiedDraftOperationProjectionV8(projection);
  assert.equal(validated.ok, true);
  assert.equal(validated.pay_date, '2026-09-04');
  assert.equal(api.isCertifiedDraftOperationProjectionV8(projection), true);

  assert.equal(api.isCertifiedDraftOperationProjectionV8({
    ...projection,
    selected_preview_row_ids: []
  }), false);
  assert.equal(api.isCertifiedDraftOperationProjectionV8({
    ...projection,
    draft_scope: 'PAYE'
  }), false);
  assert.equal(api.isCertifiedDraftOperationProjectionV8({
    ...projection,
    rail_env_snapshot: ''
  }), false);
  assert.equal(api.isCertifiedDraftOperationProjectionV8({
    ...projection,
    same_week_paye_override: { ...reference.same_week_paye_override }
  }), true);
  assert.equal(api.isCertifiedDraftOperationProjectionV8({
    ...projection,
    same_week_paye_override: { ...reference.same_week_paye_override, pay_date: '2026-09-05' }
  }), false);
});
