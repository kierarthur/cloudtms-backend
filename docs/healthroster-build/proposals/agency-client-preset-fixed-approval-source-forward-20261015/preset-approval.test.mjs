import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { functionEvents, splitSqlStatements } from './reference/pinned-source-parser.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const read = name => fs.readFileSync(path.join(here, name), 'utf8');
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
const proof = JSON.parse(read('COMPLETE_REVERSES.json'));
const dated = JSON.parse(read('DATED_POLICY_COMPLETE_REVERSES.json'));
for (const owner of [...proof.owners, dated.owner]) {
  test('SOURCE_ONLY complete owner reverse ' + owner.file, () => {
    let source = read('forward/' + owner.file);
    assert.equal(sha(source), owner.after_sha256);
    for (const hunk of [...owner.hunks].reverse()) {
      assert.equal(source.split(hunk.after).length, 2, hunk.purpose);
      source = source.replace(hunk.after, () => hunk.before);
    }
    assert.equal(source, read('before/' + owner.file));
    assert.equal(sha(source), owner.before_sha256);
    const parsed = functionEvents(read('forward/' + owner.file));
    assert.equal(parsed.events.length, 1);
    assert.equal(parsed.events[0].kind, 'CREATE');
    assert.deepEqual(parsed.unresolved, []);
  });
}
for (const input of [...proof.inputs, dated.input]) {
  test('SOURCE_ONLY exact committed input ' + input.reference, () => {
    const raw = execFileSync('git', ['show', `${input.pin}:${input.path}`], { maxBuffer: 8_000_000 });
    assert.equal(raw.length, input.raw_bytes);
    assert.equal(sha(raw), input.raw_sha256);
    assert.equal(sha(read(input.reference)), input.selected_definition_sha256);
    // The final feature owner is retained as its complete .sqlinc, including
    // its actual leading source-proposal comment. The other references are
    // exact CREATE definitions selected from a multi-owner root.
    assert.ok(sha(raw) === input.selected_definition_sha256
      || functionEvents(raw.toString('utf8')).events.some(event =>
        event.kind === 'CREATE' && sha(event.definition) === input.selected_definition_sha256));
  });
}
const client = read('forward/105_complete_client_preset_fixed_approval.sqlinc');
const feature = read('forward/106_complete_feature_setup_fixed_approval_receipts.sqlinc');
const policy = read('forward/107_complete_dated_candidate_fixed_approval_policy.sqlinc');
const core = read('reference/actual_candidate_policy_resolver.sqlinc');
const configured = read('reference/actual_configured_agency_scope.sqlinc');

test('SOURCE_ONLY genuine native admin, version, membership and group locks precede Contract mutation', () => {
  const at = text => { const position = client.indexOf(text); assert.ok(position >= 0, text); return position; };
  const update = at('update public.contracts set candidate_manager_approval_policy_json=v_approval_after');
  for (const guard of [
    'perform private._weekly_source_settings_require_admin_v1(v_actor)',
    "v_current_shape->>'settings_version'<>v_expected",
    "'weekly-source-settings-client:'||v_client::text",
    'WEEKLY_SOURCE_CLIENT_GROUP_HAS_CURRENT_FINAL_REVISION',
    'WEEKLY_SOURCE_AGENCY_FIXED_APPROVAL_SCOPE_STALE',
    'order by contract.id for update'
  ]) assert.ok(at(guard) < update);
  const originalRequest = proof.owners[0].hunks[0].before;
  assert.equal(originalRequest, '  v_policy_effective_from date;');
  const whitelist = client.slice(client.indexOf('v_input,array['), client.indexOf("],'WEEKLY_SOURCE_CLIENT_SETTINGS_VALUES_INVALID'"));
  assert.ok(!whitelist.includes("'approval'") && !whitelist.includes('candidate_manager_approval_policy_json'));
});

test('SOURCE_ONLY apply/clear changes only the fixed policy key for exact Client Contracts', () => {
  assert.ok(client.includes('where contract.client_id=v_client'));
  assert.ok(client.includes("contract.weekly_timesheet_source::text='HEALTHROSTER'"));
  assert.ok(client.includes("coalesce(v_approval_before,'{}'::jsonb)||jsonb_build_object('approval','NOT_REQUIRED')"));
  assert.ok(client.includes("else v_approval_before-'approval' end"));
  assert.ok(client.includes('if v_approval_after is not distinct from v_approval_before then continue;end if;'));
  assert.equal(client.match(/update public\.contracts set candidate_manager_approval_policy_json=/g)?.length, 1);
  assert.ok(!client.includes('set_config('));
});

test('SOURCE_ONLY actual changed Contract receipts retain every before/after and actor identity', () => {
  for (const field of ['before_policy', 'after_policy', 'expected_contract_updated_at',
    'previous_source_group_id', 'effective_from', 'receipt_idempotency_key']) assert.ok(client.includes(`'${field}'`));
  assert.ok(client.includes("v_approval_key:='SOURCE_AGENCY_FIXED:'||gen_random_uuid()::text"));
  assert.ok(client.includes('v_approval_response,v_actor,transaction_timestamp()'));
  assert.ok(client.includes('public.contract_manager_authoriser_policy_get_v1(v_approval_contract.id)'));
  assert.ok(feature.includes("case when v_result ? 'contract_approval_changes'"));
  assert.ok(feature.includes("jsonb_build_object('contract_approval_changes',v_result->'contract_approval_changes')"));
});

test('SOURCE_ONLY dated effective key is reconstructed after actual core and global settings reads', () => {
  assert.ok(policy.indexOf('v_manager_policy:=') > policy.indexOf('CANDIDATE_GLOBAL_SETTINGS_MISSING'));
  assert.ok(policy.indexOf('v_manager_policy:=') > policy.indexOf('private._contract_settings_effective_core_v1('));
  assert.ok(policy.indexOf("'manager_approval_policy',v_manager_policy") < policy.indexOf("'policy_fingerprint'"));
  assert.ok(policy.includes("v_manager_policy:=coalesce(v_values->'candidate_manager_approval_policy_json','{}'::jsonb)-'approval'"));
  assert.ok(policy.includes('contract.start_date<=v_date and contract.end_date>=v_date-6'));
  assert.ok(policy.includes('greatest(contract.start_date,v_date-6),least(contract.end_date,v_date)'));
  assert.equal(policy.match(/v_fixed_from>=membership.valid_from/g)?.length, 2);
  assert.equal(policy.match(/v_fixed_to<=coalesce\(membership.valid_to,'infinity'::date\)/g)?.length, 2);
  assert.ok(policy.includes('v_agency_group.environment,v_fixed_to'));
  assert.ok(policy.includes('source_group.environment=v_global.candidate_app_environment'));
  assert.ok(policy.includes('v_agency_group_count>1'));
  assert.ok(policy.includes('into strict v_agency_group'));
});

test('SOURCE_ONLY genuine configured predicate keeps signed/NHSP/legacy-import routes outside the fixed key', () => {
  assert.ok(policy.includes("v_authority->>'configured_route'='HEALTHROSTER_WEEKLY_VALIDATION'"));
  assert.ok(policy.includes("not coalesce((v_authority#>>'{applicability,import_authoritative}')::boolean,false)"));
  assert.ok(policy.includes('private.weekly_source_configured_agency_invoice_v1('));
  assert.ok(configured.includes("a.feature_purpose='SIGNED_CHECK_ONLY'"));
  assert.ok(configured.includes("v.values_json->>'invoice_sender'='AGENCY_INVOICE'"));
  assert.ok(configured.includes("c.weekly_timesheet_source::text<>'HEALTHROSTER' or c.self_bill"));
  assert.ok(configured.includes('private._contract_settings_effective_core_v1('));
  assert.ok(!configured.includes('_candidate_policy_resolve_v1('));
});

test('SOURCE_ONLY ordinary Candidate result and fingerprint owner are preserved outside the effective key', () => {
  const resultBefore = core.slice(core.indexOf('  v_result:=jsonb_build_object('));
  const resultAfter = policy.slice(policy.indexOf('  v_result:=jsonb_build_object('));
  assert.equal(resultAfter.replace("'manager_approval_policy',v_manager_policy,",
    "'manager_approval_policy',coalesce(v_values->'candidate_manager_approval_policy_json','{}'::jsonb),"), resultBefore);
  const statements = splitSqlStatements(policy);
  assert.equal(statements.statements.length, 1);
  assert.deepEqual(statements.meta, []);
  assert.ok(!/\b(?:insert into|update public\.|delete from|set_config\()/.test(policy));
  assert.deepEqual(dated.new_signatures, []);
  assert.deepEqual(proof.new_tables, []);
  assert.equal(proof.native, false);
  assert.equal(dated.native, false);
});
