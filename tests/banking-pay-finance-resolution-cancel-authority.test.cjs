const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const authorityName = '17082026_2052_pay_finance_resolution_cancel_authority.sql';
const authority = fs.readFileSync(path.join(root, 'supabase', 'repeatable', authorityName), 'utf8');
const reassert = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '08082026_0902_reassert_authorities_after_legacy_monolith.sql'
), 'utf8');
const manifest = JSON.parse(fs.readFileSync(path.join(
  root,
  'supabase',
  'verification',
  'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json'
), 'utf8'));
const verifier = fs.readFileSync(path.join(
  root,
  'supabase',
  'verification',
  'verify_banking_pay_targeted_fast_route_certified_reuse_catalog.mjs'
), 'utf8');

function functionBody(name, nextName = '') {
  const marker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = authority.indexOf(marker);
  assert.ok(start >= 0, `${name} must have a complete authority definition`);
  const end = nextName
    ? authority.indexOf(`CREATE OR REPLACE FUNCTION public.${nextName}`, start + marker.length)
    : authority.length;
  assert.ok(end > start, `${name} authority boundary must be ordered`);
  return authority.slice(start, end);
}

const baseline = functionBody(
  'pay_preview_candidate_build_finance_case_baseline',
  'pay_preview_candidate_build_canonical_lines'
);
const canonical = functionBody(
  'pay_preview_candidate_build_canonical_lines',
  'pay_workbench_session_clear_case_resolution'
);
const clear = functionBody('pay_workbench_session_clear_case_resolution');

test('one durable authority owns the three finance cancellation functions and is replayed last', () => {
  assert.equal((authority.match(/^CREATE OR REPLACE FUNCTION public\./gm) || []).length, 3);
  const authorityReplayAt = reassert.indexOf(`\\ir ${authorityName}`);
  const finalReplayAt = reassert.indexOf('\\ir 19072026_1816_cancel_refresh_supersede_finance_dirty.sql');
  assert.ok(authorityReplayAt >= 0 && finalReplayAt > authorityReplayAt,
    'the later cancellation refresh authority must replay after the finance cancellation owner');
  for (const signature of [
    'pay_preview_candidate_build_finance_case_baseline(jsonb,uuid)',
    'pay_preview_candidate_build_canonical_lines(jsonb,uuid)',
    'pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb)'
  ]) {
    assert.match(authority, new RegExp(`ALTER FUNCTION public\\.${signature.replace(/[().]/g, '\\$&')}[\\s\\S]*OWNER TO postgres`, 'i'));
    assert.match(authority, new RegExp(`REVOKE ALL ON FUNCTION public\\.${signature.replace(/[().]/g, '\\$&')}[\\s\\S]*FROM PUBLIC,anon`, 'i'));
    assert.match(authority, new RegExp(`GRANT EXECUTE ON FUNCTION public\\.${signature.replace(/[().]/g, '\\$&')}[\\s\\S]*TO postgres,authenticated,service_role`, 'i'));
  }

  const names = [
    'pay_preview_candidate_build_finance_case_baseline',
    'pay_preview_candidate_build_canonical_lines',
    'pay_workbench_session_clear_case_resolution'
  ];
  for (const name of names) {
    const owner = manifest.functions.find((entry) => entry.schema === 'public' && entry.name === name);
    assert.ok(owner, `${name} must be in the installed-definition catalogue`);
    assert.deepEqual(owner.source_files, [`supabase/repeatable/${authorityName}`]);
  }
  assert.equal(manifest.function_count, manifest.functions.length);
  assert.match(verifier, /requiredFinanceCancellationOwners/);
  assert.match(verifier, /finance cancellation owner or source file is not exact/);
});

test('baseline separates required resolution from exact current saved owner', () => {
  for (const required of [
    'finance_case_resolution_owner_state',
    'required_resolution_family',
    'current_saved_resolution_family',
    'current_saved_resolution_owner_kind',
    'current_saved_resolution_owner_count',
    'finance_resolution_clearability_state',
    'RESOLVED_AND_CLEARABLE',
    'REQUIRES_RESOLUTION',
    'STALE_OR_AMBIGUOUS',
    'NOT_REQUIRED'
  ]) assert.ok(baseline.includes(required), `baseline missing ${required}`);

  assert.match(baseline, /saved_resolution_payload_json->>'resolution_family'/);
  assert.match(baseline, /saved_resolution_result_json->>'resolution_family'/);
  assert.match(baseline, /resolution_fingerprint is not distinct from component_row\.current_component_fingerprint/);
  assert.match(baseline, /current_saved_resolution_family[\s\S]*TAXABLE_CHANNEL_RESTRUCTURE[\s\S]*NON_BUCKET/);
  assert.match(baseline, /case when g\.case_needs_resolution then g\.resolution_family else null::text end as required_resolution_family/);
  assert.match(baseline, /current_saved_resolution_linked_timesheet_id/);
  assert.doesNotMatch(baseline, /min\([^\n]*::uuid\)/i);
});

test('canonical publisher emits a finance action only for exact supported current owners', () => {
  assert.match(canonical, /finance_resolution_clearability_state = 'RESOLVED_AND_CLEARABLE'/);
  assert.match(canonical, /current_saved_resolution_family in \('TAXABLE_CHANNEL_RESTRUCTURE', 'NON_BUCKET'\)/);
  assert.equal((canonical.match(/'action', 'CLEAR_CASE_RESOLUTION'/g) || []).length, 2);
  assert.equal((canonical.match(/'clear_case_resolution_action'/g) || []).length, 1);
  assert.equal((canonical.match(/'case_resolution_actions'/g) || []).length, 1);
  assert.match(canonical, /Cancel Resolved Pay Channel/);
  assert.match(canonical, /Cancel Resolved Gross Total/);
  assert.match(canonical, /when fcl\.current_saved_resolution_family = 'NON_BUCKET'[\s\S]*'linked_timesheet_id'/);
});

test('clear RPC revalidates exact owner under lock and preserves the BUCKETED path', () => {
  const lockAt = clear.indexOf('FOR UPDATE');
  const taxableValidationAt = clear.indexOf('IF v_taxable_valid_owner_component_count <> v_taxable_owner_component_count');
  const taxableUpdateAt = clear.indexOf('UPDATE public.pay_finance_case_components AS component_row', taxableValidationAt);
  assert.ok(lockAt >= 0 && taxableValidationAt > lockAt && taxableUpdateAt > taxableValidationAt);
  for (const required of [
    'WORKBENCH_FINANCE_RESOLUTION_OWNER_NOT_CURRENT',
    'WORKBENCH_FINANCE_RESOLUTION_OWNER_AMBIGUOUS',
    'WORKBENCH_FINANCE_RESOLUTION_FAMILY_MISMATCH',
    'WORKBENCH_FINANCE_RESOLUTION_IDENTITY_MISMATCH',
    'TAXABLE_CHANNEL_RESTRUCTURE_CLEARED',
    "v_resolution_family = 'NON_BUCKET'",
    "v_resolution_family = 'BUCKETED'"
  ]) assert.ok(clear.includes(required), `clear RPC missing ${required}`);

  assert.match(clear, /component_row\.resolution_fingerprint is not distinct from public\.pay_finance_component_fingerprint\(/);
  assert.match(clear, /v_request_linked_timesheet_id_text[\s\S]*v_request_timesheet_id_text[\s\S]*WORKBENCH_FINANCE_RESOLUTION_IDENTITY_MISMATCH/);
  assert.match(clear, /v_resolution_family NOT IN \('TAXABLE_CHANNEL_RESTRUCTURE', 'NON_BUCKET', 'BUCKETED'\)/);
  assert.match(clear, /pay_batch_items[\s\S]*banking_pay_batch_signal_touch\(/);
  assert.doesNotMatch(clear, /UPDATE public\.pay_batch_items[\s\S]*SET[\s\S]*(?:gross|net|amount|pay_amount)/i);
});

test('malformed numeric owner evidence is guarded before any cast', () => {
  for (const body of [baseline, clear]) {
    assert.match(body, /and case[\s\S]*~ '\^-\?\[0-9\]\+\(\\\.\[0-9\]\+\)\?\$'[\s\S]*then coalesce\([\s\S]*\)::numeric >= 0[\s\S]*else false[\s\S]*end/i);
  }
});
