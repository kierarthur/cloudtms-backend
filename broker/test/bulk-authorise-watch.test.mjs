import assert from 'node:assert/strict';
import test from 'node:test';
import { buildBulkAuthoriseWatchVector, watchVectorMatches } from '../src/bulk-authorise-watch.js';

const base = () => ({
  row_key: 'timesheet:one',
  current_timesheet_id: 'one',
  contract_week_id: 'week-one',
  backend_row_signature: 'core-one',
  data_row: {
    row_key: 'timesheet:one',
    timesheet_id: 'one',
    contract_week_id: 'week-one',
    updated_at: '2026-07-17T10:00:00.000Z',
    can_bulk_authorise: true,
    processing_status: 'PENDING_AUTH',
    client_no_timesheet_required: false,
    action_flags: { can_bulk_authorise: true }
  },
  evidence: [
    {
      id: 'evidence-one',
      kind: 'TIMESHEET',
      storage_key: 'timesheets/one.pdf',
      display_name: 'one.pdf',
      created_at: '2026-07-17T10:00:00.000Z'
    }
  ],
  compare_payload: {
    source_rows: [{ ref: 'NHSP-1', hours: 7.5 }]
  }
});

test('unchanged contexts produce the same cacheable watch token', async () => {
  const first = await buildBulkAuthoriseWatchVector(base(), { includeEvidence: true, includeImport: true });
  const second = await buildBulkAuthoriseWatchVector(base(), { includeEvidence: true, includeImport: true });
  assert.equal(first.cacheable, true);
  assert.equal(first.watch_token, second.watch_token);
  assert.equal(watchVectorMatches(first, second), true);
});

test('header, settings, evidence and imported-source changes each invalidate the token', async () => {
  const original = await buildBulkAuthoriseWatchVector(base(), { includeEvidence: true, includeImport: true });
  const variants = [];

  const header = base();
  header.data_row.processing_status = 'PROCESSED';
  variants.push(header);

  const settings = base();
  settings.data_row.client_no_timesheet_required = true;
  variants.push(settings);

  const evidence = base();
  evidence.evidence[0].storage_key = 'timesheets/replacement.pdf';
  variants.push(evidence);

  const imported = base();
  imported.compare_payload.source_rows[0].hours = 8;
  variants.push(imported);

  for (const variant of variants) {
    const next = await buildBulkAuthoriseWatchVector(variant, { includeEvidence: true, includeImport: true });
    assert.notEqual(next.watch_token, original.watch_token);
    assert.equal(watchVectorMatches(original, next), false);
  }
});

test('evidence ordering and expiring signed URLs do not cause false invalidation', async () => {
  const firstPayload = base();
  firstPayload.evidence.push({
    id: 'evidence-two',
    kind: 'ACCOMMODATION',
    storage_key: 'evidence/two.png',
    display_name: 'two.png',
    signed_url: 'https://signed.invalid/old'
  });
  const secondPayload = base();
  secondPayload.evidence.unshift({
    id: 'evidence-two',
    kind: 'ACCOMMODATION',
    storage_key: 'evidence/two.png',
    display_name: 'two.png',
    signed_url: 'https://signed.invalid/new'
  });

  const first = await buildBulkAuthoriseWatchVector(firstPayload, { includeEvidence: true, includeImport: false });
  const second = await buildBulkAuthoriseWatchVector(secondPayload, { includeEvidence: true, includeImport: false });
  assert.equal(first.watch_token, second.watch_token);
});

test('full-only editor fields do not invalidate the lightweight header profile', async () => {
  const full = base();
  full.context_profile = 'full';
  full.data_row.actual_schedule_json = [{ day: 'MON', start: '09:00', end: '17:00' }];
  full.data_row.additional_units_json = { HOME_VISIT: 1 };
  full.data_row.editor_loaded = true;

  const header = base();
  header.context_profile = 'status_header';

  const fullVector = await buildBulkAuthoriseWatchVector(full, { includeEvidence: true, includeImport: false });
  const headerVector = await buildBulkAuthoriseWatchVector(header, { includeEvidence: true, includeImport: false });
  assert.equal(fullVector.watch_token, headerVector.watch_token);
});

test('full and compare-import wrappers produce the same imported-source token', async () => {
  const sourceRow = { ref: 'NHSP-1', date: '2026-07-13', hours: 7.5 };
  const full = base();
  full.compare_payload = {
    required: true,
    imported_detail_refs: { count: 1 },
    rows: [sourceRow]
  };
  const lightweight = base();
  lightweight.compare = {
    include_import_source_rows: true,
    external_source_rows_json: [sourceRow],
    source_rows: [sourceRow]
  };
  delete lightweight.compare_payload;

  const fullVector = await buildBulkAuthoriseWatchVector(full, { includeEvidence: false, includeImport: true });
  const lightweightVector = await buildBulkAuthoriseWatchVector(lightweight, { includeEvidence: false, includeImport: true });
  assert.equal(fullVector.domain_tokens.import_source, lightweightVector.domain_tokens.import_source);
  assert.equal(fullVector.watch_token, lightweightVector.watch_token);
});

test('import row ordering is stable but imported row content changes invalidate', async () => {
  const firstPayload = base();
  firstPayload.compare_payload.source_rows.push({ ref: 'NHSP-2', hours: 4 });
  const reorderedPayload = base();
  reorderedPayload.compare_payload.source_rows.unshift({ ref: 'NHSP-2', hours: 4 });
  const changedPayload = base();
  changedPayload.compare_payload.source_rows[0].hours = 8;

  const first = await buildBulkAuthoriseWatchVector(firstPayload, { includeEvidence: false, includeImport: true });
  const reordered = await buildBulkAuthoriseWatchVector(reorderedPayload, { includeEvidence: false, includeImport: true });
  const changed = await buildBulkAuthoriseWatchVector(changedPayload, { includeEvidence: false, includeImport: true });
  assert.equal(first.watch_token, reordered.watch_token);
  assert.notEqual(first.watch_token, changed.watch_token);
});

test('authoritative core signature still invalidates schedule changes hidden from the header projection', async () => {
  const original = await buildBulkAuthoriseWatchVector(base(), { includeEvidence: true, includeImport: false });
  const changed = base();
  changed.backend_row_signature = 'core-two';
  changed.data_row.actual_schedule_json = [{ day: 'MON', start: '10:00', end: '18:00' }];
  const next = await buildBulkAuthoriseWatchVector(changed, { includeEvidence: true, includeImport: false });
  assert.notEqual(next.watch_token, original.watch_token);
});

test('a missing authoritative core signature is never cacheable', async () => {
  const payload = base();
  delete payload.backend_row_signature;
  const vector = await buildBulkAuthoriseWatchVector(payload, { includeEvidence: true, includeImport: false });
  assert.equal(vector.cacheable, false);
  assert.equal(vector.watch_token, '');
});
