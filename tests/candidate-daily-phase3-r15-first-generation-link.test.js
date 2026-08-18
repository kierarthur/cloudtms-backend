import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { findCandidateDailyRoute } from '../broker/src/candidate-daily-contract-v1.js';
import { candidateDailyPhase1bInternals } from '../broker/src/candidate-daily-phase1b.js';

const root = process.cwd();
const masterPath = path.join(root, 'docs', 'candidate-app', 'phase3-apps-script',
  'master-rota', 'CloudTMSCandidateBridge.gs');
const masterCodePath = path.join(root, 'docs', 'candidate-app', 'phase3-apps-script',
  'master-rota', 'Code.gs');
const sqlPath = path.join(root, 'supabase', 'repeatable',
  '18082026_0131_candidate_daily_first_generation_source_link_v1.sql');
const workflowPath = path.join(root, '.github', 'workflows', 'candidate-db-runtime.yml');
const openApiPath = path.join(root, 'docs', 'candidate-app', 'CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml');

const read = (file) => fs.readFileSync(file, 'utf8');
const correlationId = `0${'A'.repeat(25)}`;
const candidateId = '00000000-0000-4000-8000-000000000151';

function day(index) {
  const date = new Date(Date.UTC(2026, 7, 17 + index)).toISOString().slice(0, 10);
  return {
    date,
    booked: false,
    system_blocked: false,
    source_row_hash: String(index).padStart(64, '0')
  };
}

function generationItem(overrides = {}) {
  return {
    candidate_global_key: 'CID1-ABCDEFGHJKMNPQRS',
    candidate_source_hmac: 'a'.repeat(64),
    source_hmac_key_version: 1,
    source_event_id: 'master-rota.r15-first-generation',
    source_revision: `phase3.${'b'.repeat(64)}`,
    source_hash: 'c'.repeat(64),
    window_start: '2026-08-17',
    days: Array.from({ length: 14 }, (_, index) => day(index)),
    source_event_time: '2026-08-18T00:00:00.000Z',
    item_key: 'rota.r15.first-generation.fixed',
    ...overrides
  };
}

function generationBody(item = generationItem()) {
  return {
    batch_request_id: '00000000-0000-4000-8000-000000000150',
    items: [item]
  };
}

test('R15 Master publishes the exact CID1 and separate versioned source HMAC without raw Public ID', () => {
  const helper = read(masterPath);
  const master = read(masterCodePath);
  assert.match(master, /function\s+buildCandidateIdFromPublicId_\s*\(/);
  assert.match(helper, /ctmsP3_masterGlobalCandidateKey_\(publicId\)/);
  assert.match(helper, /candidate_global_key:\s*candidateGlobalKey/);
  assert.match(helper, /candidate_source_hmac:\s*sourceHmac/);
  assert.match(helper, /source_hmac_key_version:\s*CTMS_P3_MASTER_SOURCE_HMAC_KEY_VERSION_/);
  assert.match(helper, /CTMS_P3_MASTER_SOURCE_HMAC_KEY_VERSION_\s*=\s*1/);
  assert.doesNotMatch(helper,
    /(?:candidate_public_id|credentially_public_id|raw_public_id)\s*:/i);
});

test('R15 Worker accepts only the exact first-generation identity fields', () => {
  const validate = candidateDailyPhase1bInternals.validateSystemBody;
  const body = generationBody();
  assert.equal(validate('googleAvailabilityPublishRotaGenerations', body), true);

  for (const missing of ['candidate_global_key', 'candidate_source_hmac', 'source_hmac_key_version']) {
    const item = generationItem();
    delete item[missing];
    assert.equal(validate('googleAvailabilityPublishRotaGenerations', generationBody(item)), false, missing);
  }
  assert.equal(validate('googleAvailabilityPublishRotaGenerations',
    generationBody(generationItem({ candidate_global_key: 'CID1-invalid' }))), false);
  assert.equal(validate('googleAvailabilityPublishRotaGenerations',
    generationBody(generationItem({ source_hmac_key_version: 2 }))), false);
  assert.equal(validate('googleAvailabilityPublishRotaGenerations',
    generationBody(generationItem({ unexpected_identity: 'forbidden' }))), false);
});

test('R15 Worker passes the frozen identity facts unchanged to the existing five-argument RPC', async () => {
  const route = findCandidateDailyRoute('POST',
    '/candidate-system/v1/google-availability/rota-generations');
  const body = generationBody();
  const calls = [];
  const response = await candidateDailyPhase1bInternals.invokeSystemRpc({
    route,
    body,
    correlationId,
    idempotencyKey: 'r15-first-generation-fixed-key'
  }, { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {
    async rpc(name, args) {
      calls.push({ name, args });
      return {
        batch_receipt_id: '00000000-0000-4000-8000-000000000152',
        outcomes: [{
          index: 0,
          status: 'COMMITTED',
          generation_id: '00000000-0000-4000-8000-000000000153',
          generation_version: 1
        }]
      };
    }
  });
  assert.equal(response.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].name, 'candidate_daily_rota_generation_publish_atomic_v1');
  assert.deepEqual(calls[0].args.p_items, body.items);
  assert.equal(calls[0].args.p_items[0].candidate_global_key, body.items[0].candidate_global_key);
  assert.equal(calls[0].args.p_items[0].candidate_source_hmac, body.items[0].candidate_source_hmac);
});

test('R15 database authority links only one existing active CID1 Candidate and never creates a Candidate', () => {
  const sql = read(sqlPath);
  assert.match(sql, /upper\(btrim\(c\.key_norm\)\)=v_global_key/i);
  assert.match(sql, /c\.active\s+is\s+true/i);
  assert.match(sql, /v_candidate_count=0[\s\S]*IDENTITY_LINK_MISSING/i);
  assert.match(sql, /v_candidate_count<>1[\s\S]*IDENTITY_LINK_AMBIGUOUS/i);
  assert.match(sql, /insert into private\.candidate_daily_source_links/i);
  assert.match(sql, /insert into private\.candidate_daily_authority_scopes/i);
  assert.match(sql, /'GOOGLE_PRIMARY',0,false/i);
  assert.doesNotMatch(sql, /insert\s+into\s+public\.candidates|update\s+public\.candidates|delete\s+from\s+public\.candidates/i);
});

test('R15 first-generation link and generation share one item subtransaction and fail conflicts closed', () => {
  const sql = read(sqlPath);
  assert.match(sql, /for v_item in[\s\S]*begin[\s\S]*_candidate_daily_source_candidate_bind_on_generation_v1[\s\S]*insert into public\.candidate_daily_rota_generations[\s\S]*exception when others/i);
  assert.match(sql, /IDENTITY_LINK_CONFLICT/);
  assert.match(sql, /v_existing\.candidate_id<>v_candidate_id/);
  assert.match(sql, /order by q\.lock_key[\s\S]*pg_advisory_xact_lock/i);
  assert.match(sql, /v_error not in \('GENERATION_INCOMPLETE','SOURCE_EVENT_CONFLICT','IDENTITY_LINK_MISSING',[\s\S]*'IDENTITY_LINK_CONFLICT'/i);
});

test('R15 same canonical generation supports legacy HMAC tiles and new-app Candidate UUID reads', async () => {
  const legacyRoute = findCandidateDailyRoute('POST',
    '/candidate-system/v1/google-availability/legacy/tiles');
  const calls = [];
  const response = await candidateDailyPhase1bInternals.invokeSystemRpc({
    route: legacyRoute,
    body: { candidate_source_hmac: 'a'.repeat(64), from: '2026-08-17', days: 14 },
    correlationId,
    idempotencyKey: null
  }, { CANDIDATE_APP_ENVIRONMENT: 'TEST' }, {
    async rpc(name, args) {
      calls.push({ name, args });
      return {
        generation_version: 1,
        tiles: Array.from({ length: 14 }, (_, index) => {
          const date = new Date(Date.UTC(2026, 7, 17 + index));
          return {
            date: date.toISOString().slice(0, 10),
            display_day: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][date.getUTCDay()],
            display_date: `${String(date.getUTCDate()).padStart(2, '0')}/08/2026`,
            booked: false, editable: true, status: 'PENDING'
          };
        }),
        freshness: { generation_published_at: '2026-08-18T00:00:00.000Z' }
      };
    }
  });
  assert.equal(response.status, 200);
  assert.equal(calls[0].args.p_internal_context.candidate_source_hmac, 'a'.repeat(64));
  assert.equal(candidateDailyPhase1bInternals.candidateContext({
    environment: 'TEST', selected_candidate_id: candidateId
  }).candidate_id, candidateId);
  assert.match(read(sqlPath), /active_generation_id=v_generation_id/);
});

test('R15 OpenAPI and Candidate runtime workflow install and expose the exact contract', () => {
  const openApi = read(openApiPath);
  const workflow = read(workflowPath);
  assert.match(openApi, /RotaGenerationItem:[\s\S]*- candidate_global_key[\s\S]*- source_hmac_key_version/);
  assert.match(openApi, /candidate_global_key:[\s\S]*\^CID1-/);
  assert.match(openApi, /source_hmac_key_version:[\s\S]*const: 1/);
  assert.match(openApi, /const: IDENTITY_LINK_CONFLICT/);
  assert.match(workflow, /18082026_0131_candidate_daily_first_generation_source_link_v1\.sql/);
  assert.match(workflow, /18082026_0138_candidate_daily_first_generation_source_link_runtime_verification\.sql/);
});
