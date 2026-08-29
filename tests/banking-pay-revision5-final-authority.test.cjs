const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const sqlFiles = fs.readdirSync(repeatableDir)
  .filter((name) => name.endsWith('.sql'))
  .map((name) => ({ name, source: fs.readFileSync(path.join(repeatableDir, name), 'utf8') }));

const canonicalIdentities = [
  'banking_pay_operation_claim_next',
  'banking_pay_operation_release_lease',
  'banking_pay_operation_start',
  'pay_bank_event_ingest',
  'pay_bank_transfers_claim_provider_submit_chunk',
  'pay_batch_auth_apply_action',
  'pay_batch_auth_start',
  'pay_batch_cancel',
  'pay_batch_schedule',
  'pay_batches_claim_due_scheduled',
  'pay_no_money_unwind_apply_work_item',
  'pay_payment_correction_authorise',
  'pay_payment_correction_expand_work',
  'pay_payment_correction_process_chunk',
  'pay_payment_correction_request_start',
  'pay_pre_bank_cancel_apply_work_item',
  'pay_settle_rail'
];

test('every Revision 5 amended function has one repeatable source authority', () => {
  for (const identity of canonicalIdentities) {
    const re = new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+public\\.${identity}\\s*\\(`, 'gi');
    const owners = sqlFiles.flatMap(({ name, source }) => (source.match(re) || []).map(() => name));
    assert.deepEqual(owners.length, 1, `${identity} authorities: ${owners.join(', ')}`);
  }
});

test('all thirty-three manifest functions have exactly one canonical repeatable body', () => {
  const manifest = JSON.parse(fs.readFileSync(
    path.join(root, 'supabase', 'verification', 'banking_pay_revision5_catalog_manifest.json'),
    'utf8'
  ));
  assert.equal(manifest.function_count, 33);
  assert.equal(manifest.functions.length, 33);
  for (const item of manifest.functions) {
    const sourceIdentity = item.name === 'pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr'
      ? 'pay_payment_cancel_not_sent_and_recalculate_with_workbench_refresh'
      : item.name;
    const qualified = item.schema === 'private'
      ? `private\\.${sourceIdentity}`
      : `(?:public\\.)?${sourceIdentity}`;
    const re = new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+${qualified}\\s*\\(`, 'gi');
    const owners = sqlFiles.flatMap(({ name, source }) => (source.match(re) || []).map(() => `supabase/repeatable/${name}`));
    assert.deepEqual(owners, item.source_files, `${item.schema}.${item.name} source authorities`);
  }
});

test('payment-operation dependencies are hash-pinned to one saved source authority', () => {
  const manifest = JSON.parse(fs.readFileSync(
    path.join(root, 'supabase', 'verification', 'banking_pay_revision5_catalog_manifest.json'),
    'utf8'
  ));
  assert.equal(manifest.dependency_function_count, 4);
  assert.equal(manifest.dependency_functions.length, 4);
  for (const item of manifest.dependency_functions) {
    const re = new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+(?:public\\.)?${item.name}\\s*\\(`, 'gi');
    const owners = sqlFiles.flatMap(({ name, source }) => (source.match(re) || []).map(() => `supabase/repeatable/${name}`));
    assert.deepEqual(owners, item.source_files, `${item.schema}.${item.name} dependency source authorities`);
    assert.match(item.definition_sha256, /^[0-9a-f]{64}$/);
  }
});

test('legacy monolith cannot reinstall Revision 5 amended identities', () => {
  const monolith = sqlFiles.find(({ name }) => name === '26052026_2100HRS_NEW_FUNCTIONS.sql')?.source || '';
  for (const identity of canonicalIdentities) {
    assert.doesNotMatch(monolith, new RegExp(`CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+public\\.${identity}\\s*\\(`, 'i'));
  }
});

test('normal cancellation SQL and Worker paths do not call compatibility bodies', () => {
  const ownedSql = sqlFiles
    .filter(({ name }) => /^04082026_(1154|1158|1206|1207|1208|1209|1210|1211)_/.test(name))
    .map(({ source }) => source)
    .join('\n');
  const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
  const normalSources = `${ownedSql}\n${worker}`;
  for (const compatibilityName of [
    'pay_payment_cancel_not_sent_and_recalculate',
    'pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr',
    'pay_payment_cancel_not_sent_and_recalculate_complete_v1',
    'pay_payment_confirm_no_money_and_unwind'
  ]) {
    assert.doesNotMatch(normalSources, new RegExp(`\\b${compatibilityName}\\b`, 'i'));
  }
});

test('continuation consumers accept only SQL-owned typed descriptors', () => {
  const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
  assert.match(worker, /result\?\.continuation/);
  assert.match(worker, /Array\.isArray\(result\?\.continuations\)/);
  assert.match(worker, /\.slice\(0, 4\)/);
  assert.doesNotMatch(worker, /operationIds\.size\s*>=\s*10/);
  assert.match(worker, /delete saveProgressPatch\.continuation_no_progress_count/);
  assert.match(worker, /p_progress_patch_json:\s*progressPatch/);
});

test('the eleven transaction owners return typed continuation contracts', () => {
  const files = [
    '04082026_1206_pay_batch_cancel.sql',
    '04082026_1207_pay_payment_correction_request_start.sql',
    '04082026_1154_pay_payment_correction_authorise.sql',
    '04082026_1209_pay_payment_correction_process_chunk.sql',
    '04082026_1145_pay_payment_correction_status_get_v1.sql',
    '04082026_1146_pay_batch_payment_status_page_v1.sql',
    '04082026_1210_pay_bank_event_ingest.sql',
    '04082026_1154_pay_batches_claim_due_scheduled.sql',
    '04082026_1154_pay_batch_auth_apply_action.sql',
    '04082026_1158_pay_batch_schedule.sql',
    '04082026_1211_pay_settle_rail.sql'
  ];
  for (const name of files) {
    const source = fs.readFileSync(path.join(repeatableDir, name), 'utf8');
    assert.match(source, /'continuation(?:s)?'/i, `${name} continuation result missing`);
    if (name === '04082026_1206_pay_batch_cancel.sql') {
      assert.match(source, /'continuation'\s*,\s*v_result->'continuation'/i, `${name} must forward the transaction-owned descriptor`);
    } else if (['04082026_1145_pay_payment_correction_status_get_v1.sql', '04082026_1146_pay_batch_payment_status_page_v1.sql'].includes(name)) {
      assert.match(source, /'required'\s*,\s*false/i, `${name} must return an inert status descriptor`);
      assert.match(source, /'successor_relation'\s*,\s*'NONE'/i, `${name} successor relation missing`);
    } else {
      assert.match(source, /'required'/i, `${name} required flag missing`);
      assert.match(source, /'operation_id'/i, `${name} operation id missing`);
      assert.match(source, /'successor_relation'/i, `${name} successor relation missing`);
    }
  }
});

test('cancellation mutation routes fail closed and proof binding uses the locked frame', () => {
  const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
  assert.match(worker, /requireFeature:\s*true,\s*requirePaymentPermission:\s*true/);
  assert.match(worker, /banking_pay_candidate_cancellation_enabled/);
  assert.match(worker, /cloudtms-session-v1/);
  assert.match(worker, /setUint32\(0, bytes\.byteLength, false\)/);
  assert.match(worker, /session_issued_at_epoch_seconds:\s*Number\(p\.iat\)/);
  assert.doesNotMatch(worker, /p_reason:\s*String\(body\?\.reason\s*\|\|\s*'DRAFT_PAYMENT_CANCELLED_BY_USER'/);
});

test('repeatable runner applies only new or content-changed files', () => {
  const releaseEngine = fs.readFileSync(path.join(root, 'scripts', 'cloudtms-db-release.mjs'), 'utf8');
  assert.match(releaseEngine, /const installedRepeatables = new Map/);
  assert.match(releaseEngine, /current\.repeatables\.filter\(item => installedRepeatables\.get\(item\.path\) !== item\.sha256\)/);
  assert.match(releaseEngine, /for \(const item of pendingRepeatables\) \{[\s\S]+?psql\(\{ file: item\.path \}\)/);
  assert.match(releaseEngine, /on conflict\(path\) do update set closure_sha256=excluded\.closure_sha256/);
  assert.doesNotMatch(releaseEngine, /REAPPLY_AFTER_EARLIER_CHANGE/);
  assert.doesNotMatch(releaseEngine, /REAPPLY unchanged later repeatable after earlier authority change/);
});
