import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { PDFDocument } from 'pdf-lib';

import {
  runWeeklySourceCompletedPackCopies,
} from '../../broker/src/weekly-source/completed-pack-copy.mjs';
import {
  renderWeeklySourceCompletedPackArtifact,
} from '../../broker/src/candidate-app-backend.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const owner = fs.readFileSync(path.join(
  root,
  'supabase/repeatable/19092026_1645_weekly_source_completed_pack_copy_v1.sql',
), 'utf8').replaceAll('\r\n', '\n');
const UUID = '11111111-1111-4111-8111-111111111111';
const TIMESHEET = '22222222-2222-4222-8222-222222222222';
const HASH = 'a'.repeat(64);

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function inMemoryR2(seed = new Map()) {
  const objects = new Map(seed);
  return {
    objects,
    async get(key) {
      const value = objects.get(key);
      if (!value) return null;
      return {
        httpMetadata: value.httpMetadata,
        async arrayBuffer() {
          return value.bytes.buffer.slice(
            value.bytes.byteOffset, value.bytes.byteOffset + value.bytes.byteLength,
          );
        },
      };
    },
    async put(key, bytes, options) {
      const stored = new Uint8Array(bytes);
      objects.set(key, {
        bytes: stored,
        httpMetadata: options.httpMetadata,
        customMetadata: options.customMetadata,
      });
      return { etag: sha256(stored) };
    },
    async head(key) {
      const value = objects.get(key);
      return value ? { customMetadata: value.customMetadata } : null;
    },
  };
}

function job(overrides = {}) {
  return {
    workflow_id: UUID,
    workflow_generation: 2,
    timesheet_id: TIMESHEET,
    timesheet_revision: 3,
    timesheet_family: 'booking-family',
    document_mode: 'CHECK_ONLY',
    render_input_sha256: HASH,
    ...overrides,
  };
}

test('runner synchronises status, renders each due pack and commits exact artifact facts', async () => {
  const calls = [];
  const artifact = {
    workflow_id: UUID,
    render_input_sha256: HASH,
    storage_key: 'weekly-source/test/completed.pdf',
    final_document_sha256: 'b'.repeat(64),
    filename: 'Completed_Timesheet_2026-09-13.pdf',
    media_type: 'application/pdf',
    byte_size: 1200,
    page_count: 2,
    content_policy_version: 'WEEKLY_COMPLETED_PACK_COPY_CONTENT_V1',
  };
  const dependencies = {
    rpc: async (name, args) => {
      calls.push({ name, request: args.p_request });
      if (name === 'weekly_source_completed_pack_copy_status_sync_v1') {
        return { ok: true, updated_count: 1 };
      }
      if (name === 'weekly_source_completed_pack_copy_due_list_v1') {
        return { ok: true, count: 1, items: [job()] };
      }
      if (name === 'weekly_source_completed_pack_copy_commit_atomic_v1') {
        assert.deepEqual(args.p_request, artifact);
        return { ok: true, idempotent_replay: false };
      }
      throw new Error(`unexpected RPC ${name}`);
    },
    renderCompletedPack: async (_env, supplied) => {
      assert.deepEqual(supplied, job());
      return artifact;
    },
  };
  const result = await runWeeklySourceCompletedPackCopies({}, dependencies, { limit: 10 });
  assert.deepEqual(result, {
    ok: true, status_updates: 1, due: 1, committed: 1,
    replayed: 0, failed: 0, failures: [],
  });
  assert.deepEqual(calls.map(({ name }) => name), [
    'weekly_source_completed_pack_copy_status_sync_v1',
    'weekly_source_completed_pack_copy_due_list_v1',
    'weekly_source_completed_pack_copy_commit_atomic_v1',
  ]);
});

test('one render failure is bounded and does not stop another completion generation', async () => {
  const first = job();
  const second = job({ workflow_id: '33333333-3333-4333-8333-333333333333' });
  const committed = [];
  const result = await runWeeklySourceCompletedPackCopies({}, {
    rpc: async (name, args) => {
      if (name.endsWith('_status_sync_v1')) return { ok: true, updated_count: 0 };
      if (name.endsWith('_due_list_v1')) return { ok: true, items: [first, second] };
      committed.push(args.p_request.workflow_id);
      return { ok: true, idempotent_replay: false };
    },
    renderCompletedPack: async (_env, supplied) => {
      if (supplied.workflow_id === first.workflow_id) {
        throw new Error('WEEKLY_COMPLETED_PACK_RENDER_STALE');
      }
      return {
        workflow_id: supplied.workflow_id,
        render_input_sha256: supplied.render_input_sha256,
      };
    },
  });
  assert.equal(result.ok, false);
  assert.equal(result.failed, 1);
  assert.equal(result.committed, 1);
  assert.deepEqual(committed, [second.workflow_id]);
  assert.equal(result.failures[0].error_code, 'WEEKLY_COMPLETED_PACK_RENDER_STALE');
});

test('real invoice-evidence renderer combines exact final PDFs and stores one immutable pack', async () => {
  const first = await PDFDocument.create({ updateMetadata: false });
  first.addPage([300, 200]).drawText('Timesheet');
  const firstBytes = new Uint8Array(await first.save({ useObjectStreams: false }));
  const second = await PDFDocument.create({ updateMetadata: false });
  second.addPage([300, 200]).drawText('Expense evidence');
  const secondBytes = new Uint8Array(await second.save({ useObjectStreams: false }));
  const r2 = inMemoryR2(new Map([
    ['final/timesheet.pdf', {
      bytes: firstBytes, httpMetadata: { contentType: 'application/pdf' }, customMetadata: {},
    }],
    ['final/expense.pdf', {
      bytes: secondBytes, httpMetadata: { contentType: 'application/pdf' }, customMetadata: {},
    }],
  ]));
  const artifact = await renderWeeklySourceCompletedPackArtifact({
    CANDIDATE_APP_ENVIRONMENT: 'TEST', R2: r2,
  }, job({
    document_mode: 'INVOICE_EVIDENCE_REQUIRED',
    workflow_generation: 4,
    week_ending_date: '2026-09-13',
    components: [
      {
        review_ordinal: 1, storage_key: 'final/timesheet.pdf',
        content_sha256: sha256(firstBytes), media_type: 'application/pdf',
        byte_size: firstBytes.byteLength, page_count: 1,
      },
      {
        review_ordinal: 2, storage_key: 'final/expense.pdf',
        content_sha256: sha256(secondBytes), media_type: 'application/pdf',
        byte_size: secondBytes.byteLength, page_count: 1,
      },
    ],
  }));
  assert.equal(artifact.page_count, 2);
  assert.equal(artifact.media_type, 'application/pdf');
  assert.equal(artifact.filename, 'Completed_Timesheet_2026-09-13.pdf');
  assert.match(artifact.storage_key, /weekly-source\/test\/completed-pack-copy/);
  const stored = r2.objects.get(artifact.storage_key);
  assert.ok(stored);
  assert.equal(stored.httpMetadata.contentType, 'application/pdf');
  assert.equal(stored.customMetadata.purpose, 'weekly-source-completed-pack-copy');
  assert.equal(sha256(stored.bytes), artifact.final_document_sha256);
  assert.equal((await PDFDocument.load(stored.bytes)).getPageCount(), 2);
});

test('SQL owner enforces mode, signatures, final components, family idempotency and no finance writes', () => {
  assert.match(owner, /workflow_kind in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)[\s\S]*scope='WEEKLY'/i);
  assert.doesNotMatch(owner, /workflow_kind='WEEKLY'/i);
  assert.match(owner, /document_mode='CHECK_ONLY'[\s\S]*state<>'WORKER_SUBMITTED'/i);
  assert.match(owner, /candidate_signature_component_id[\s\S]*component_kind='CANDIDATE_SIGNATURE'[\s\S]*state='IMMUTABLE'/i);
  assert.match(owner, /document_mode='INVOICE_EVIDENCE_REQUIRED'[\s\S]*state<>'FINALISED'/i);
  assert.match(owner, /final_signed_render_state<>'READY'/i);
  assert.match(owner, /pg_catalog\.btrim\(event_timesheet\.booking_id\)=pg_catalog\.btrim\(v_timesheet\.booking_id\)/i);
  assert.match(owner, /completion_generation=\(v_facts->>'workflow_generation'\)::integer/i);
  assert.match(owner, /'changes_validation',false,'changes_pay',false,'changes_invoice',false/i);
  assert.match(owner, /revoke all on function public\.weekly_source_completed_pack_copy_commit_atomic_v1\(jsonb\)[\s\S]*public,anon,authenticated/i);
  assert.doesNotMatch(owner, /insert\s+into\s+public\.(timesheets_financials|invoice_lines|pay_[a-z0-9_]+)/i);
  assert.doesNotMatch(owner, /update\s+public\.(timesheets|timesheets_financials|invoice_lines|pay_[a-z0-9_]+)/i);
});

test('informational copy uses one deterministic outbox item and never contains a review link', () => {
  assert.match(owner, /'CLIENT_INFORMATIONAL_COPY'/i);
  assert.match(owner, /'WEEKLY_COMPLETED_TIMESHEET_COPY'/i);
  assert.match(owner, /No approval, signature or other action is required/i);
  assert.match(owner, /on conflict \(deterministic_outbox_key\) do update/i);
  assert.doesNotMatch(owner, /review_url|secure link|manager_route/i);
});
