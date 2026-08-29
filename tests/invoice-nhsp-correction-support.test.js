import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { buildNhspSupportHtml } from '../broker/src/invoice-document-templates.js';

const read = path => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

test('authoritative correction evidence preserves corrected-hours and reversal occurrences', () => {
  const generation = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/'
      + '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
  );

  assert.match(generation, /source_import_rows as materialized/i);
  assert.match(
    generation,
    /jsonb_build_object\(\s*'external_row_key',s\.external_row_key,\s*'evidence_role_key',s\.evidence_role_key\)/s,
  );
  assert.match(
    generation,
    /when 'REVERSAL' then case when g\.source_system='NHSP'\s*then 'NHSP Reversal'/s,
  );
  assert.match(
    generation,
    /when 'CORRECTED_HOURS' then case when g\.source_system='NHSP'\s*then 'NHSP Corrected Hours'/s,
  );
  assert.doesNotMatch(generation, /reversal_row_keys/);
});

test('NHSP support renders an explicit entry for both members of a correction pair', () => {
  const common = {
    worker: 'Test Worker',
    nhsp_shift_id: 'SHIFT-1',
    booking_reference: 'BOOK-1',
    site_ward: 'Test Site / Test Ward',
    shift_date: '2026-06-05',
    shift_times: '09:00 - 16:00',
    hours_units: '6.50',
    commission_amount: '52.50',
    total_amount: '209.85',
    source_identity: 'NHSP · import row 1',
    validation_state: 'Included',
  };
  const html = buildNhspSupportHtml({
    schema_version: 'NHSP_PRESENTATION_V1',
    rows: [
      { ...common, evidence_role: 'NHSP Corrected Hours' },
      { ...common, evidence_role: 'NHSP Reversal', reversal_state: 'REVERSED' },
    ],
  });

  assert.match(html, /<th>Entry<\/th>/);
  assert.match(html, />NHSP Corrected Hours<\/td>/);
  assert.match(html, />NHSP Reversal<\/td>/);
  assert.equal((html.match(/<tbody><tr>|<\/tr><tr>/g) || []).length, 2);
});

test('legacy NHSP reversal models remain readable and visibly labelled', () => {
  const html = buildNhspSupportHtml({
    schema_version: 'NHSP_PRESENTATION_V1',
    rows: [{
      worker: 'Test Worker', nhsp_shift_id: 'SHIFT-1', booking_reference: 'BOOK-1',
      site_ward: 'Test Site / Test Ward', shift_date: '2026-06-05',
      shift_times: '09:00 - 16:00', hours_units: '6.50', commission_amount: '52.50',
      total_amount: '209.85', source_identity: 'NHSP · import row 1',
      validation_state: 'Included', reversal_state: 'REVERSED',
    }],
  });

  assert.match(html, />NHSP Reversal<\/td>/);
});
