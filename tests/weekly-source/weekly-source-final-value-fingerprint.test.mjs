import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import test from 'node:test';

const read = p => readFileSync(new URL(`../../${p}`, import.meta.url), 'utf8').replaceAll('\r\n', '\n');
const predecessor = read('supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql');
const successor = read('supabase/repeatable/13102026_1210_weekly_source_finalisation_unchanged_values_v1.sql');
const helpers = read('supabase/repeatable/support/13102026_1210_weekly_source_final_value_helpers_v1.sql');
const migrationPath = 'supabase/migrations/13102026_1210_weekly_source_final_value_snapshot_authority.sql';
const migration = read(migrationPath);
const functionBody = (source, name) => {
  const start = source.indexOf(`create or replace function private.${name}(`);
  assert.ok(start >= 0, name);
  return source.slice(start, source.indexOf('$function$;', start) + '$function$;'.length);
};
const before = functionBody(predecessor, 'weekly_source_finalise_engine_v1');
const after = functionBody(successor, 'weekly_source_finalise_engine_v1');

test('complete generated successor is reproducible at the reviewed predecessor', () => {
  execFileSync(process.execPath, ['scripts/build-weekly-source-final-value-successor.mjs', '--check'], { windowsHide: true });
  assert.match(successor, /\\ir support\/13102026_1210_weekly_source_final_value_helpers_v1\.sql/);
  assert.match(after, /language plpgsql volatile security definer/);
  assert.match(after, /set search_path to 'public','private','pg_catalog','pg_temp'/);
  assert.match(successor, /revoke all on function private\.weekly_source_finalise_engine_v1\(jsonb,uuid,boolean\)\s+from public,anon,authenticated,service_role/);
  assert.match(successor, /notify pgrst, 'reload schema'/);
});

test('unchanged NHSP physical report and complete native post-transition tail survive', () => {
  const nhsp = source => source.slice(source.indexOf('  else\n    v_source_total_cost:=0;'), source.indexOf('  for v_manifest in'));
  assert.equal(nhsp(after), nhsp(before));
  assert.equal(after.slice(after.indexOf('  for v_manifest in')), before.slice(before.indexOf('  for v_manifest in')));
  for (const owner of ['weekly_source_pay_query_admit_v2', 'bpay_next_source_current_lock_inventory_v1',
    'bpay_next_source_current_lock_release_v1', 'bpay_next_source_current_publish_revision_v1',
    'weekly_source_manual_reviews_finalised_v1', 'weekly_source_lock_family_rows_v1']) {
    assert.equal(after.split(owner).length, before.split(owner).length, owner);
  }
});

test('economic/provenance snapshot hash and original pay reversal/replacement owner stay exact', () => {
  const snapshotHash = source => source.slice(source.indexOf('      v_snapshot_hash:='), source.indexOf('      insert into public.weekly_source_final_snapshot_lines('));
  assert.equal(snapshotHash(after), snapshotHash(before));
  const transitionWrites = source => source.slice(source.indexOf('      v_outcome:=case'), source.indexOf("    if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'"));
  assert.equal(transitionWrites(after), transitionWrites(before));
  assert.doesNotMatch(after, /(?:invoice_correction_insert|invoice_correction_unwind|payment_execute|provider_submit)/);
});

test('value fingerprint excludes nested and top-level hashes, retains all economic dimensions and stored VAT', () => {
  const vector = functionBody(helpers, 'weekly_source_final_value_vector_v1');
  const state = functionBody(helpers, 'weekly_source_final_state_fingerprint_v1');
  const snapshot = functionBody(helpers, 'weekly_source_snapshot_value_fingerprint_v1');
  assert.doesNotMatch(vector + state, /calculation_fingerprint|policy_fingerprint|mapping_fingerprint|rate_fingerprint|updated_at|created_at|source_fingerprint/);
  for (const key of ['work_event_id', 'candidate_id', 'client_id', 'contract_id', 'work_date', 'start_at_local',
    'end_at_local', 'break_minutes', 'actual_net_minutes', 'pay_vector', 'charge_vector', 'invoice_vat_rate_pct', 'invoice_vat_chargeable', 'authority_subset']) assert.ok(state.includes(`'${key}'`), key);
  for (const key of ['row_sign', 'paid_minutes', 'break_minutes', 'total_pence', 'hours', 'rates', 'day', 'night', 'sat', 'sun', 'bh']) assert.ok(vector.includes(`'${key}'`), key);
  assert.match(snapshot, /economic\.row_resolution_id=v_snapshot\.row_resolution_id/);
  assert.match(snapshot, /v_economic\.invoice_vat_rate_pct/);
  assert.match(snapshot, /v_economic\.invoice_vat_chargeable/);
  assert.doesNotMatch(snapshot, /_weekly_source_effective_policy|from public\.(?:contracts|client_settings|contract_settings|settings_defaults)/);
});

test('new snapshot witness is nullable, append-only, closed and matches the exact locked migration', () => {
  assert.match(migration, /add column authority_subset_json jsonb/);
  assert.match(migration, /add column authority_subset_sha256 bytea/);
  assert.match(migration, /add column invoice_economics_sha256 bytea/);
  assert.doesNotMatch(migration, /\bupdate\s+public\.|\bdefault\s+|\bgrant\s+/i);
  assert.ok(helpers.includes(createHash('sha256').update(migration).digest('hex')));
  assert.match(after, /v_authority_subset,\s*private\.weekly_source_sha256_jsonb_v1\('WEEKLY_SOURCE_AUTHORITY_SUBSET_V1',v_authority_subset\)/);
  const subset = functionBody(helpers, 'weekly_source_authority_subset_v1');
  assert.deepEqual([...subset.matchAll(/p_policy->'([^']+)'/g)].map(match => match[1]),
    ['authority_mode','self_bill_enabled','source_group_id','self_bill_correction_presentation']);
});

test('old CANCEL uses exact origin proof and one legal original pay presentation, otherwise full hash fallback', () => {
  const history = functionBody(helpers, 'weekly_source_snapshot_authority_subset_v1');
  assert.match(history, /cloudtms_repeatable_ledger/);
  assert.match(history, /240e2460baf9bff3164cc4ed389b6acb5774f8f88ab0deefc70d6f085fae00f1/);
  assert.match(history, /cloudtms_migration_ledger/);
  assert.match(history, /v_snapshot\.created_at_utc>=v_cutover/);
  assert.match(history, /source_row\.work_date=v_snapshot\.work_date/);
  assert.match(history, /movement_revision\.finalised_at_utc<=v_origin\.finalised_at_utc/);
  assert.match(history, /v_movement_count<>1 or v_presentation_count<>1/);
  assert.doesNotMatch(history, /_weekly_source_effective_policy_v1|from public\.contracts|from public\.client_settings/);
  assert.match(after, /coalesce\(v_current_authority_subset,v_prior_authority_subset\) is null[\s\S]*v_current_snapshot\.policy_fingerprint,v_prior_snapshot\.policy_fingerprint/);
  assert.match(after, /v_comparison_current_authority:=null/);
});

test('invoice fingerprint is separate, exact, and refuses missing source values before F5 wiring', () => {
  const invoice = functionBody(helpers, 'weekly_source_invoice_economics_fingerprint_v1');
  assert.match(invoice, /source_shift_charge_pence','source_gross_pence','source_vat_pence/);
  assert.match(invoice, /'line_vat_method',p_line_vat_method,'vat_rate_pct',p_vat_rate/);
  assert.match(invoice, /WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY/);
  assert.doesNotMatch(invoice, /calculated_charge|total_pay|settings|updated_at|contract/);
  assert.match(after, /null -- SOURCE_AMOUNT wire waits for the exact F5 released-money producer/);
});

test('independent recorded-source census covers all attempts and all five complete engines', () => {
  const history = JSON.parse(read('docs/healthroster-build/F2_HISTORICAL_SOURCE_REVIEW.json'));
  assert.equal(history.releaseCount, 109); assert.equal(history.versions.length, 5);
  assert.ok(history.rows.some(row => row.status === 'FAILED'));
  for (const version of history.versions) assert.equal(version.everyTransitionGuardedBeforeMovement && version.prepareReturnAfterTransitionLoop && version.guardedFailureRaisesAndRollsBackFunction, true);
  for (const row of history.rows) { assert.equal(row.engineDeclarationCount, 1); assert.equal(row.snapshotWriterCount, 1); }
});
