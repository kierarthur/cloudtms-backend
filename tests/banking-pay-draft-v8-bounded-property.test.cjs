const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const test = require('node:test');

const MODULE_URL = new URL('../broker/src/banking-pay-draft-certified-v8.js', `file://${__filename}`);
const RECORDED_SEEDS = Object.freeze([
  '0x43544d530001',
  '0x43544d530002',
  '0x43544d530003',
  '0x43544d530004'
]);

function digest(value) {
  return crypto.createHash('sha256').update(String(value)).digest('hex');
}

function uuid(seed, suffix) {
  const hex = digest(`${seed}:${suffix}`).slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function countsFor(scope, total, seedIndex) {
  const paye = seedIndex % 2 === 0 ? Math.ceil(total / 2) : Math.floor(total / 3);
  const umbrella = total - paye;
  return {
    selected_ready_total: total,
    selected_ready_paye: paye,
    selected_ready_umbrella: umbrella,
    selected_ready_for_request: scope === 'PAYE' ? paye : scope === 'UMBRELLA' ? umbrella : total
  };
}

function validEnvelope({ seed, seedIndex, scope, total, filtered }) {
  const overall = digest(`${seed}:${scope}:${total}:${filtered}:overall`);
  const manifest = digest(`${seed}:${scope}:${total}:${filtered}:manifest`);
  const overrideUsed = scope !== 'UMBRELLA' && seedIndex % 2 === 1;
  const countFacts = countsFor(scope, total, seedIndex);
  return {
    certificate_reference: {
      candidate_filter_id: filtered ? uuid(seed, 'candidate') : null,
      certification_id: `WORKBENCH_SETTLED_CERTIFICATION_V2:${overall}`,
      client_filter_id: filtered ? uuid(seed, 'client') : null,
      filter_context_digest_sha256: digest(`${seed}:${scope}:filter`),
      idempotency_key: `draft-create:property:${seed}:${scope}:${total}:${filtered}`,
      manifest_digest_sha256: manifest,
      overall_digest_sha256: overall,
      pay_channel_scope: scope,
      same_week_paye_override: overrideUsed ? {
        continue: true,
        guardrail_code: 'PAYE_SAME_WEEK_OVERRIDE_REQUIRED',
        pay_date: '2026-09-04',
        pay_week_end: '2026-09-06',
        pay_week_start: '2026-08-31',
        reason: 'Existing verified same-week exception',
        reauth_purpose: 'PAYE_SAME_WEEK_OVERRIDE',
        used: true,
        verified: true,
        verified_at_utc: '2026-09-03T10:30:00.000Z',
        verified_by_user_id: uuid(seed, 'verifier')
      } : {
        continue: false,
        guardrail_code: null,
        pay_date: '2026-09-04',
        pay_week_end: '2026-09-06',
        pay_week_start: '2026-08-31',
        reason: null,
        reauth_purpose: null,
        used: false,
        verified: false,
        verified_at_utc: null,
        verified_by_user_id: null
      }
    },
    pre_admission_scope_facts: {
      authority_fence_generation: 100 + seedIndex,
      certificate_uuid: uuid(seed, 'certificate'),
      certification_id: `WORKBENCH_SETTLED_CERTIFICATION_V2:${overall}`,
      manifest_digest_sha256: manifest,
      overall_digest_sha256: overall,
      pay_channel_scope: scope,
      pay_date: '2026-09-04',
      pay_week_end: '2026-09-06',
      pay_week_start: '2026-08-31',
      progress_counter_version: 200 + seedIndex,
      scope_facts_contract: 'WORKBENCH_SETTLED_CERTIFICATE_PRE_ADMISSION_SCOPE_FACTS_V1',
      ...countFacts,
      session_version: 300 + seedIndex,
      week_ending_cutoff: '2026-09-06',
      workbench_session_id: uuid(seed, 'session')
    }
  };
}

function clone(value) {
  return structuredClone(value);
}

test('recorded seeds exercise a bounded 96-case certified Draft transport matrix', async () => {
  const api = await import(MODULE_URL.href);
  const scopes = ['ALL', 'PAYE', 'UMBRELLA'];
  const totals = [101, 5000];
  let accepted = 0;
  let rejected = 0;

  for (const [seedIndex, seed] of RECORDED_SEEDS.entries()) {
    for (const scope of scopes) {
      for (const total of totals) {
        for (const filtered of [false, true]) {
          const envelope = validEnvelope({ seed, seedIndex, scope, total, filtered });
          const validated = api.validateCurrentCertificateIssuerEnvelopeV8(envelope);
          assert.equal(validated.ok, true, `${seed}:${scope}:${total}:${filtered}`);
          const input = api.buildCertifiedDraftOperationInputV8(validated);
          assert.equal(api.isCertifiedDraftOperationInputV8(input), true);
          assert.deepEqual(api.findExpandedSelectionKeys(input), []);
          assert.equal(input.selected_preview_row_count, envelope.pre_admission_scope_facts.selected_ready_for_request);
          assert.ok(Buffer.byteLength(JSON.stringify(input), 'utf8') < 5000);
          accepted += 1;

          const mutated = clone(envelope);
          const selector = accepted % 8;
          if (selector === 0) mutated.pre_admission_scope_facts.overall_digest_sha256 = digest('wrong');
          if (selector === 1) mutated.pre_admission_scope_facts.selected_ready_for_request += 1;
          if (selector === 2) mutated.pre_admission_scope_facts.session_version = 0;
          if (selector === 3) mutated.pre_admission_scope_facts.pay_week_start = '2026-09-01';
          if (selector === 4) mutated.certificate_reference.pay_channel_scope = scope === 'PAYE' ? 'UMBRELLA' : 'PAYE';
          if (selector === 5) mutated.certificate_reference.candidate_filter_id = 'not-a-uuid';
          if (selector === 6) mutated.pre_admission_scope_facts.selected_preview_row_ids = [];
          if (selector === 7) mutated.certificate_reference.manifest_digest_sha256 = digest('other-manifest');
          assert.equal(api.validateCurrentCertificateIssuerEnvelopeV8(mutated).ok, false,
            `mutation accepted for ${seed}:${scope}:${total}:${filtered}:${selector}`);
          rejected += 1;
        }
      }
    }
  }

  assert.deepEqual({ accepted, rejected, total: accepted + rejected }, {
    accepted: 48,
    rejected: 48,
    total: 96
  });
});

test('the configured ceiling remains scalar-only and 50,001 always fails before admission', async () => {
  const api = await import(MODULE_URL.href);
  for (const [seedIndex, seed] of RECORDED_SEEDS.entries()) {
    const envelope = validEnvelope({ seed, seedIndex, scope: 'ALL', total: 5000, filtered: false });
    envelope.pre_admission_scope_facts.selected_ready_total = 50001;
    envelope.pre_admission_scope_facts.selected_ready_paye = 25001;
    envelope.pre_admission_scope_facts.selected_ready_umbrella = 25000;
    envelope.pre_admission_scope_facts.selected_ready_for_request = 50001;
    const result = api.validateCurrentCertificateIssuerEnvelopeV8(envelope);
    assert.equal(result.ok, false);
    assert.equal(result.code, 'BANKING_PAY_DRAFT_CERTIFIED_SELECTED_COUNT_INVALID');
  }
});
