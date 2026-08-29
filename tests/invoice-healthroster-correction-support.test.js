import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { buildHealthRosterSupportHtml } from '../broker/src/invoice-document-templates.js';

const read = path => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

const generation = read(
  'supabase/repeatable/27072026_1042_invoice_async_v8/'
    + '27072026_1042_12_private_invoice_generation_advance_core_v8.sql',
);

test('HealthRoster authoritative corrections recover source evidence through timesheet ancestry', () => {
  assert.match(
    generation,
    /adjustment_segment_refs as materialized[\s\S]*basis::text in\('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT'\)/i,
  );
  assert.match(
    generation,
    /adjustment_segment_refs as materialized[\s\S]*healthroster_attach_allowed[\s\S]*source_timesheet_ancestry[\s\S]*upper\(coalesce\(n\.source_system::text,''\)\)='HEALTHROSTER'[\s\S]*r\.healthroster_attach_allowed/i,
  );
  assert.match(
    generation,
    /when 'REVERSAL' then case when g\.source_system='NHSP'[\s\S]*else 'HealthRoster Reversal'/i,
  );
  assert.match(
    generation,
    /when 'CORRECTED_HOURS' then case when g\.source_system='NHSP'[\s\S]*else 'HealthRoster Corrected Hours'/i,
  );
});

test('HealthRoster invoice attachment is independent of validation-only policy', () => {
  const directSourceBranch = generation.match(
    /source_segments as materialized\s*\([\s\S]*?\r?\n\s*union\r?\n/i,
  )?.[0] || '';

  assert.match(directSourceBranch, /attach_policy,hr_attach_to_invoice/i);
  assert.doesNotMatch(directSourceBranch, /attach_policy,requires_hr/i);

  const issueValidation = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_private_invoice_issue_validate_batch.sql',
  );
  const issueImportSourceBranch = issueValidation.match(
    /import_source_requirements as materialized[\s\S]*?\r?\n\),\r?\nimport_source_checks as materialized/i,
  )?.[0] || '';

  assert.match(issueImportSourceBranch, /attach_policy,hr_attach_to_invoice/i);
  assert.doesNotMatch(issueImportSourceBranch, /attach_policy,requires_hr/i);
});

test('HealthRoster presentation preserves explicit correction evidence roles', () => {
  const presentation = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql',
  );

  assert.match(
    presentation,
    /else jsonb_build_object\(\s*'evidence_role',coalesce\([\s\S]*'HealthRoster Reversal'[\s\S]*'HealthRoster Shift'/i,
  );
});

test('HealthRoster support renders corrected-hours and reversal entries', () => {
  const common = {
    worker: 'Test Worker',
    assignment: 'Test Assignment',
    shift_date: '2026-06-08',
    shift_times: '07:30 - 19:30',
    site: 'West London',
    ward: 'Test Ward',
    booking_reference: 'BOOK-1',
    units_hours: '11.00',
    validation_state: 'Included',
    source_identity: 'HEALTHROSTER · import row 1',
    reference_match_state: 'EXACT_IMPORT_EXTERNAL_ROW',
    reference_match_count: 1,
  };
  const html = buildHealthRosterSupportHtml({
    schema_version: 'HEALTHROSTER_PRESENTATION_V2',
    rows: [
      { ...common, evidence_role: 'HealthRoster Corrected Hours' },
      { ...common, evidence_role: 'HealthRoster Reversal', reversal_state: 'REVERSED' },
    ],
  });

  assert.match(html, /<th>Entry<\/th>/);
  assert.match(html, />HealthRoster Corrected Hours<\/td>/);
  assert.match(html, />HealthRoster Reversal<\/td>/);
  assert.equal((html.match(/<tbody><tr>|<\/tr><tr>/g) || []).length, 2);
});

test('legacy HealthRoster reversal models remain visibly labelled', () => {
  const html = buildHealthRosterSupportHtml({
    schema_version: 'HEALTHROSTER_PRESENTATION_V1',
    rows: [{
      worker: 'Test Worker', assignment: 'Test Assignment', shift_date: '2026-06-08',
      shift_times: '07:30 - 19:30', site: 'West London', ward: 'Test Ward',
      reference: 'BOOK-1', units_hours: '11.00', validation_state: 'Included',
      source_identity: 'HEALTHROSTER · import row 1', reversal_state: 'REVERSED',
    }],
  });

  assert.match(html, />HealthRoster Reversal<\/td>/);
});
