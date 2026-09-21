import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const repeatablePath = path.join(root, 'supabase', 'repeatable', '15092026_1534_weekly_source_acl_contract_v1.sql');
const verificationPath = path.join(root, 'supabase', 'verification', '15092026_1534_weekly_source_acl_contract_v1.sql');
const migrationPaths = [
  '15092026_1534_weekly_source_plan6_schema.sql',
  '15092026_2310_weekly_source_delivery_targets.sql',
  '15092026_2335_weekly_source_finalisation_pay_orchestration.sql',
].map((name) => path.join(root, 'supabase', 'migrations', name));

function contractTables(sql) {
  const start = sql.indexOf('create or replace function private._weekly_source_acl_table_contract_v1');
  const end = sql.indexOf('create or replace function private._weekly_source_acl_lifecycle_column_contract_v1', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  return [...sql.slice(start, end).matchAll(/\('([^']+)','(?:IMMUTABLE_APPEND_ONLY|IMMUTABLE_FACTS_WITH_LIFECYCLE|STATEFUL_SERVER_OWNED)'\)/g)]
    .map((match) => match[1]);
}

function contractSignatures(sql) {
  const start = sql.indexOf('create or replace function private._weekly_source_acl_service_rpc_contract_v1');
  const end = sql.indexOf('create or replace function private._weekly_source_immutable_record_guard_v1', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  return [...sql.slice(start, end).matchAll(/\('([^']+)'\)/g)].map((match) => match[1]);
}

function contractPrivateHelpers(sql) {
  const start = sql.indexOf('create or replace function private._weekly_source_acl_private_helper_contract_v1');
  const end = sql.indexOf('create or replace function private._weekly_source_acl_service_rpc_contract_v1', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  return [...sql.slice(start, end).matchAll(/\('([^']+)'\)/g)].map((match) => match[1]);
}

// Each declaration is read only as far as its own array terminator, so a
// declaration added after it can never leak into the extracted set. That
// mistake is exactly what a private-helper array declared next to the service
// array would otherwise cause.
function declaredArray(sql, declaration) {
  const start = sql.indexOf(declaration);
  assert.notEqual(start, -1, `missing declaration ${declaration}`);
  const end = sql.indexOf(']::text[];', start);
  assert.notEqual(end, -1, `unterminated declaration ${declaration}`);
  return [...sql.slice(start, end).matchAll(/'((?:public|private)\.[^']+)'/g)].map((match) => match[1]);
}

const verificationSignatures = (sql) => declaredArray(sql, 'v_expected_service_rpcs text[]:=array[');
const verificationPrivateHelpers = (sql) => declaredArray(sql, 'v_expected_private_helpers text[]:=array[');

const canonical = (value) => String(value).replaceAll(/\s+/g, '').toLowerCase();

test('central Weekly Source ACL and independent verifier seal the same exact service surface', async () => {
  const [repeatable, verification] = await Promise.all([
    readFile(repeatablePath, 'utf8'), readFile(verificationPath, 'utf8')
  ]);
  const actual = contractSignatures(repeatable);
  const expected = verificationSignatures(verification);
  // 82 + the two Plan 6.2 Gate 8 Mode A service RPCs
  // (public.weekly_source_mode_a_dispatch_atomic_v1(jsonb) and
  // public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb)) + Gate 2's
  // public.weekly_source_later_change_decide_atomic_v1(jsonb). Each is granted
  // execute to service_role by its own repeatable.
  // + the sixteen registered by the WP-15d final seals pass: one WP-03 guard
  // decision shim, two WP-06d external-publication entry points, four WP-07 /
  // WP-07c first-authorisation and withdrawal entry points, four WP-08b
  // pending-release entry points, one WP-14 guard-refusal recorder and one
  // post-rollback recorder, one WP-14 Candidate hours push, and the two WP-14
  // Timesheet audit/export readers, plus the invoice/report projection used by
  // the final self-bill reporting route. Every one was confirmed on a full NEW
  // build to hold exactly one foreign grant, service_role EXECUTE, with anon
  // and authenticated denied.
  assert.equal(actual.length, 109);
  assert.equal(new Set(actual).size, actual.length);
  assert.deepEqual([...actual].sort(), [...expected].sort());
});

test('central Weekly Source ACL registers private helpers apart from the service surface', async () => {
  const [repeatable, verification] = await Promise.all([
    readFile(repeatablePath, 'utf8'), readFile(verificationPath, 'utf8')
  ]);
  const actual = contractPrivateHelpers(repeatable);
  const expected = verificationPrivateHelpers(verification);
  // The registered private-helper inventory: 2 Gate 1 helpers, 3 Gate 1 review
  // guards and decision D8's 2 per-root authorisation guards; 2 Gate 6
  // Contract-choice facts and 6 Gate 6 rotation-authority routines; 2 Gate 8
  // Mode A facts; 13 Gate 3/5 publication routines; 6 Gate 7 invoice-issue
  // routines; and 8 Gate 2 proposal-composer routines. It is a floor, not a
  // closed set, while packages are still landing; seal it as exact once every
  // package has reported.
  //
  // 44 -> 46, WP-37: the G6-13 work-event schedule-compatibility fact
  // (`24 §9` step 2, so that durable identity is a COMPATIBLE schedule and not
  // the exact Actual start and end) and the Gate 8 superseded-head refusal
  // record (`24 §9A`, so a rotated Timesheet cannot block the family's current
  // head). Both are registered in the repeatable and in the independent
  // verifier, and the installed ACL verifier reports
  // `registered_private_helper_count: 46` on a build from empty.
  assert.equal(actual.length, 49);
  assert.equal(new Set(actual).size, actual.length);
  assert.deepEqual([...actual].sort(), [...expected].sort());
  // Every registered helper is private, and no helper may also be listed as a
  // service RPC: they are revoked from service_role, so a signature in the
  // service list that is not granted makes the ACL verifier's routine sweep
  // fail.
  for (const signature of actual) assert.match(signature, /^private\./);
  const serviceSurface = new Set(contractSignatures(repeatable).map(canonical));
  assert.deepEqual(actual.filter((signature) => serviceSurface.has(canonical(signature))), []);
  const verifierServiceSurface = new Set(verificationSignatures(verification).map(canonical));
  assert.deepEqual(actual.filter((signature) => verifierServiceSurface.has(canonical(signature))), []);
});

test('central Weekly Source ACL classifies every Plan 6 table exactly once', async () => {
  const [repeatable, ...migrations] = await Promise.all([
    readFile(repeatablePath, 'utf8'),
    ...migrationPaths.map((file) => readFile(file, 'utf8')),
  ]);
  const actual = contractTables(repeatable);
  const expected = migrations.flatMap((sql) =>
    [...sql.matchAll(/create\s+table\s+public\.([a-z0-9_]+)/gi)].map((match) => match[1])
  );
  // 93 + the four Plan 6.2 Gate 1 public relations (the decision bundle, the
  // common current-head relation, its component relation and the pending
  // publication bundle) + decision D8's per-root authorisation record. The
  // private publication receipt is not a public table and is governed by
  // proof/32 section 9 directly.
  assert.equal(actual.length, 99);
  assert.equal(new Set(actual).size, actual.length);
  assert.deepEqual([...actual].sort(), [...expected].sort());
});

test('every direct Plan 6 service grant covered by the central ACL remains covered', async () => {
  const directory = path.join(root, 'supabase', 'repeatable');
  const filenames = (await readdir(directory)).filter((name) => /^15092026_.*\.sql$/i.test(name));
  const repeatable = await readFile(repeatablePath, 'utf8');
  const allowed = new Set(contractSignatures(repeatable).map(canonical));
  const directGrants = new Set();
  for (const filename of filenames) {
    const sql = await readFile(path.join(directory, filename), 'utf8');
    for (const match of sql.matchAll(
      /grant\s+execute\s+on\s+function\s+((?:public|private)\.[a-z0-9_]+\([^;]*?\))\s+to\s+service_role/gi
    )) directGrants.add(canonical(match[1]));
  }
  const governed = [...directGrants].filter((signature) =>
    /^(?:public|private)\.(?:_?(?:ctms_)?weekly_source_|weekly_exceptional_)/.test(signature)
  );
  assert.deepEqual(governed.filter((signature) => !allowed.has(signature)), []);
  for (const required of [
    'private.weekly_source_invoice_batch_snapshot_v1()',
    'private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb)',
    'private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)',
    'public.weekly_source_correct_final_review_atomic_v1(jsonb)',
    'public.weekly_source_manager_route_prepare_atomic_v1(jsonb)',
    'public.weekly_source_message_targets_register_atomic_v1(jsonb)',
    'public.weekly_source_message_dispatch_target_claim_v1(jsonb)',
    'public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)',
    'public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)',
    'public.weekly_source_message_render_due_list_v1(jsonb)',
    'public.weekly_source_candidate_app_request_get_v1(uuid,text,uuid,timestamptz)',
    'public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz)',
    'public.weekly_source_candidate_app_draft_save_atomic_v1(uuid,text,uuid,jsonb,timestamptz)',
    'public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamptz)'
  ]) assert(allowed.has(canonical(required)), `missing ${required}`);
});

test('general service-only inventory includes the Weekly Source invoice report projection', async () => {
  const [general, auditExport, auditOrder] = await Promise.all([
    readFile(path.join(root, 'supabase', 'verification', '22082026_1302_general_browser_isolation_verification.sql'), 'utf8'),
    readFile(path.join(root, 'supabase', 'repeatable', '17092026_1200_weekly_source_audit_and_export_v1.sql'), 'utf8'),
    readFile(path.join(root, 'supabase', 'migrations', '21092026_1817_weekly_source_audit_event_order.sql'), 'utf8'),
  ]);
  assert.match(general, /v_count<>789 or v_service_missing<>75 or v_browser_executable<>0/i);
  assert.match(general, /v_hash<>'9174b0459732514d17f987720c6079f8'/i);
  assert.match(auditExport, /grant execute on function public\.weekly_source_invoice_report_rows_v1\(jsonb\) to service_role/i);
  assert.match(auditExport, /revoke all on function public\.weekly_source_invoice_report_rows_v1\(jsonb\)[\s\S]*from public,anon,authenticated/i);
  assert.match(general, /v_count<>9 or v_hash<>'7cd05e540b00e9ad067c6fc6d98e4b79'/i);
  assert.match(auditOrder, /event_sequence bigint generated always as identity/i);
});
