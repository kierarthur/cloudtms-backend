const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (kind, name) => fs.readFileSync(path.join(root, 'supabase', kind, name), 'utf8');

const migration = read('migrations', '07082026_2153_certified_source_preview_publication_guard.sql');
const publisher = read('repeatable', '07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql');
const progress = read('repeatable', '07082026_2155_pay_workbench_session_recompute_progress_counters.sql');
const completion = read('repeatable', '04082026_1219_pay_workbench_complete_job.sql');
const catalogue = JSON.parse(read(
  'verification',
  'banking_pay_workbench_certified_source_preview_catalog_manifest.json'
));
const catalogueVerifier = read(
  'verification',
  'verify_banking_pay_workbench_certified_source_preview_catalog.mjs'
);
const workflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'supabase-migrate.yml'), 'utf8');

test('schema records compact publication authority without broad legacy backfill', () => {
  assert.match(migration, /certified_preview_publication_required boolean NOT NULL DEFAULT false/);
  assert.match(migration, /certified_preview_publication_parity_ok boolean NOT NULL DEFAULT false/);
  assert.match(migration, /CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1/);
  assert.match(migration, /CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bpay_wb_scope_certified_preview_incomplete_v1/);
  assert.doesNotMatch(migration, /UPDATE\s+public\.banking_pay_workbench_session_scope/i);
});

test('publisher is one private postgres-only invoker contract', () => {
  assert.match(publisher, /CREATE OR REPLACE FUNCTION private\.pay_workbench_publish_certified_source_preview_v1\(/);
  assert.match(publisher, /SECURITY INVOKER/);
  assert.match(publisher, /SET search_path TO ''/);
  assert.match(publisher, /REVOKE ALL[\s\S]*FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(publisher, /GRANT EXECUTE[\s\S]*TO postgres/);
  assert.doesNotMatch(publisher, /TO service_role;/);
});

test('GitHub records and verifies all three installed publication authorities', () => {
  assert.equal(catalogue.function_count, 3);
  assert.deepEqual(
    catalogue.functions.map((entry) => `${entry.schema}.${entry.name}`).sort(),
    [
      'private.pay_workbench_publish_certified_source_preview_v1',
      'public.pay_workbench_complete_job',
      'public.pay_workbench_session_recompute_progress_counters'
    ]
  );
  assert.match(catalogueVerifier, /definition_sha256/);
  assert.match(catalogueVerifier, /unexpected overload/);
  assert.match(workflow, /verify_banking_pay_workbench_certified_source_preview_catalog\.mjs/);
});

test('each publication function has one authoritative SQL definition', () => {
  const sqlFiles = ['migrations', 'repeatable'].flatMap((kind) => {
    const directory = path.join(root, 'supabase', kind);
    return fs.readdirSync(directory, { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith('.sql'))
      .map((entry) => path.join(directory, entry.name));
  });
  for (const [schema, name] of [
    ['private', 'pay_workbench_publish_certified_source_preview_v1'],
    ['public', 'pay_workbench_complete_job'],
    ['public', 'pay_workbench_session_recompute_progress_counters']
  ]) {
    const pattern = new RegExp(
      `CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+${schema}\\.${name}\\s*\\(`,
      'gi'
    );
    const owners = sqlFiles.flatMap((file) => {
      const source = fs.readFileSync(file, 'utf8');
      return [...source.matchAll(pattern)].map(() => path.basename(file));
    });
    assert.equal(owners.length, 1, `${schema}.${name} owners: ${owners.join(', ')}`);
  }
});

test('publisher validates exact certified build, sequence and newer-work authority', () => {
  for (const code of [
    'CERTIFIED_SOURCE_PREVIEW_SESSION_STALE',
    'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING',
    'CERTIFIED_SOURCE_PREVIEW_BUILD_NOT_COMPLETE',
    'CERTIFIED_SOURCE_PREVIEW_BUILD_AUTHORITY_MISMATCH',
    'CERTIFIED_SOURCE_PREVIEW_SOURCE_SET_AMBIGUOUS',
    'CERTIFIED_SOURCE_PREVIEW_SOURCE_DIGEST_MISMATCH',
    'CERTIFIED_SOURCE_PREVIEW_SOURCE_SEQUENCE_STALE',
    'CERTIFIED_SOURCE_PREVIEW_NEWER_WORK_PRESENT',
    'CERTIFIED_SOURCE_PREVIEW_ROW_CONTRACT_INVALID',
    'CERTIFIED_SOURCE_PREVIEW_SELECTION_INTENT_INVALID',
    'CERTIFIED_SOURCE_PREVIEW_PARITY_FAILED'
  ]) assert.match(publisher, new RegExp(code));
  assert.match(publisher, /dependency_edge_stream_complete IS NOT TRUE/);
  assert.match(publisher, /edge_tag_stream_complete IS NOT TRUE/);
  assert.match(publisher, /effect_plan_sealed/);
  assert.match(publisher, /public\.app_change_counters/);
  assert.match(publisher, /banking_pay_workbench_candidate_delta_projection_runs/);
});

test('publisher copies certified rows and accepts the two intended public row classes', () => {
  assert.match(publisher, /ready_row\.source_row_json\s*\|\|\s*jsonb_build_object/);
  assert.match(publisher, /published_from_certified_source/);
  assert.match(publisher, /publication_authority_kind/);
  assert.match(publisher, /policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH/);
  assert.match(publisher, /pay_workbench_preview_line_contract_ok/);
  assert.match(publisher, /line_type', ''\)\)\) = 'TIMESHEET_PAYMENT'/);
  assert.match(publisher, /effective_selection_state/);
  assert.match(publisher, /'NOT_SELECTABLE'/);
  assert.doesNotMatch(publisher, /pay_workbench_candidate_line_work_process_chunk/);
});

test('stable upsert, selection identity invalidation and stale retirement are explicit', () => {
  assert.match(publisher, /ON CONFLICT \(session_id, section, candidate_id, row_key\)/);
  assert.match(publisher, /selection_identity_digest/);
  assert.match(publisher, /existing_selection_identity_digest = prepared_row\.selection_identity_digest/);
  assert.match(publisher, /AS preview_contract_json/);
  assert.match(publisher, /'preview_contract', ready_row\.preview_contract_json/);
  assert.match(publisher, /existing_preview_id IS NULL THEN true/);
  assert.match(publisher, /selection_state = 'SUPERSEDED'/);
  assert.match(publisher, /server_selected_preview_row_ids_provided IS TRUE/);
});

test('non-selectable blocked and case rows require the canonical display-row contract', () => {
  assert.match(publisher, /Retain the installed V1 allowance for non-economic timesheet context/);
  assert.match(publisher, /Other blocked\/case economic rows must also satisfy/);
  assert.match(
    publisher,
    /prepared_row\.is_selectable IS NOT TRUE[\s\S]+pay_workbench_preview_line_contract_ok\([\s\S]+->>'ok'/,
  );
  assert.match(publisher, /'READY_TO_PAY', 'CASES_RESOLUTIONS', 'BLOCKED_FOR_PAY', 'SNOOZED'/);
  assert.match(publisher, /'CASE_RESOLUTION', 'CASES_RESOLUTIONS'/);
});

test('parity proof compares both directions and attests only after equality', () => {
  assert.match(publisher, /EXCEPT ALL SELECT \* FROM preview_identities/);
  assert.match(publisher, /EXCEPT ALL SELECT \* FROM source_identities/);
  assert.match(publisher, /v_source_identity_digest IS DISTINCT FROM v_preview_identity_digest/);
  const proof = publisher.indexOf("RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PARITY_FAILED'");
  const attestation = publisher.indexOf('certified_preview_publication_parity_ok = true');
  assert.ok(proof > 0 && attestation > proof);
});

test('progress independently blocks required scopes with missing or stale parity', () => {
  assert.match(progress, /publication_current/);
  assert.match(progress, /CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE/);
  assert.match(progress, /v_certified_publication_incomplete_count/);
  assert.match(progress, /AND COALESCE\(v_certified_publication_incomplete_count, 0\) = 0/);
  assert.match(progress, /'read_only'[\s\S]*v_certified_publication_incomplete_count/);
  assert.doesNotMatch(progress, /INSERT INTO public\.banking_pay_workbench_preview_rows/);
});

test('completion publishes before success and duplicate completion self-heals', () => {
  assert.match(completion, /DUPLICATE_REPLAY_REPAIR/);
  assert.match(completion, /INITIAL_COMPLETION/);
  assert.match(completion, /private\.pay_workbench_publish_certified_source_preview_v1/);
  assert.match(completion, /CERTIFIED_SOURCE_PREVIEW_TERMINAL_COMPLETION/);
  const initialPublish = completion.lastIndexOf('v_certified_publication_json := private.pay_workbench_publish_certified_source_preview_v1');
  const successUpdate = completion.indexOf('UPDATE public.banking_pay_workbench_jobs AS update_job', initialPublish);
  assert.ok(initialPublish > 0 && successUpdate > initialPublish);
  assert.doesNotMatch(completion.slice(initialPublish, successUpdate), /pay_workbench_candidate_source_build/);
});

test('duplicate completion is an exact no-op once certified parity is current', () => {
  assert.match(publisher, /v_already_current/);
  assert.match(publisher, /current_preview\.row_json->'preview_contract' IS DISTINCT FROM exact_row\.preview_contract_json/);
  assert.match(publisher, /'already_current', true/);
  assert.match(publisher, /'upserted_row_count', 0/);
  assert.match(publisher, /'retired_row_count', 0/);
  assert.match(completion, /IF COALESCE\(\(v_certified_publication_json->>'already_current'\)::boolean, false\) IS NOT TRUE THEN/);
});

test('frozen no-change boundary remains explicit', () => {
  const combined = [migration, publisher, progress].join('\n');
  assert.doesNotMatch(combined, /pay_batch_schedule|pay_settle_rail|pay_bank_event_ingest/);
  assert.doesNotMatch(combined, /(INSERT INTO|UPDATE|DELETE FROM)\s+public\.pay_payment_correction/i);
  assert.doesNotMatch(combined, /INSERT INTO public\.pay_bank_transfers|UPDATE public\.pay_bank_transfers/);
  assert.doesNotMatch(combined, /INSERT INTO public\.pay_batches|UPDATE public\.pay_batches/);
  assert.doesNotMatch(combined, /CREATE (TABLE|TRIGGER)/i);
});
