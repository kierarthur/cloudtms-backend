import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = relative => readFileSync(new URL(`../${relative}`, import.meta.url), 'utf8');

const detail = read(
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
    + '23072026_2207_invoice_detail_get.sql'
);
const snapshot = read(
  'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql'
);
const downstream = read(
  'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql'
);

test('invoice detail exposes semantic evidence identity to the preparation worker', () => {
  assert.match(detail, /select e\.id,e\.timesheet_id,e\.kind,e\.document_role,e\.candidate_component_id/i);
  assert.match(detail, /'document_role',e\.document_role,'candidate_component_id',e\.candidate_component_id/i);
});

test('frozen invoice evidence excludes standalone signatures and orders each summary before its expense pages', () => {
  assert.match(snapshot, /'document_role',evidence\.document_role/i);
  assert.match(snapshot, /upper\(coalesce\(e\.processing_state,''\)\)<>\s*'SUPERSEDED'/i);
  assert.match(snapshot, /not in\(\s*'CANDIDATE_SIGNATURE','MANAGER_SIGNATURE','ELECTRONIC_SIGNATURES'\)/i);
  assert.match(snapshot, /upper\(coalesce\(e\.kind,''\)\) not in\(\s*'CANDIDATE_SIGNATURE','MANAGER_SIGNATURE','ELECTRONIC_SIGNATURES'\)/i);
  assert.match(
    snapshot,
    /order by evidence\.timesheet_id,[\s\S]*EXPENSE_MILEAGE_APPROVAL_SUMMARY'[\s\S]*evidence\.created_at,evidence\.evidence_id/i
  );
  assert.match(snapshot, /line_type',''\)\) in\('MILEAGE','EXPENSES'\)/i);
});

test('live and frozen invoice assembly retain the same semantic exclusions and summary ordering', () => {
  assert.match(downstream, /upper\(coalesce\(te\.document_role,''\)\) document_role/i);
  assert.match(downstream, /upper\(coalesce\(te\.processing_state,''\)\)<>\s*'SUPERSEDED'/i);
  assert.match(downstream, /not in\(\s*'CANDIDATE_SIGNATURE','MANAGER_SIGNATURE','ELECTRONIC_SIGNATURES'\)/i);
  assert.match(downstream, /upper\(coalesce\(te\.kind,''\)\) not in\(\s*'CANDIDATE_SIGNATURE','MANAGER_SIGNATURE','ELECTRONIC_SIGNATURES'\)/i);
  assert.match(
    downstream,
    /e\.timesheet_id,[\s\S]*e\.document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY'[\s\S]*e\.created_at,e\.source_id/i
  );
});

test('legacy frozen evidence resolves its immutable semantic role by exact id and fails closed', () => {
  assert.match(
    downstream,
    /left join public\.timesheet_evidence resolved_evidence[\s\S]*resolved_evidence\.id=case[\s\S]*x\.value->>'evidence_id'/i
  );
  assert.match(
    downstream,
    /resolved_evidence\.document_role,nullif\(btrim\(x\.value->>'document_role'\),''\)/i
  );
  assert.match(
    downstream,
    /nullif\(upper\(coalesce\([\s\S]*resolved_evidence\.document_role,nullif\(btrim\(x\.value->>'document_role'\),''\),''[\s\S]*\)\),''\) is not null/i
  );
  assert.match(
    downstream,
    /upper\(coalesce\(resolved_evidence\.processing_state,''\)\)<>\s*'SUPERSEDED'/i
  );
});
