import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const read = path => fs.readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

function functionBody(source, qualifiedName) {
  const lower = source.toLowerCase();
  const start = lower.indexOf(`create or replace function ${qualifiedName.toLowerCase()}`);
  assert.notEqual(start, -1, `${qualifiedName} missing`);
  const bodyStart = lower.indexOf('as $function$', start);
  const bodyEnd = lower.indexOf('$function$;', bodyStart);
  assert.ok(bodyStart > start && bodyEnd > bodyStart, `${qualifiedName} body invalid`);
  return source.slice(start, bodyEnd + '$function$;'.length);
}

test('new invoice plans omit attachment indexes and use official timesheet V2', () => {
  const planner = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
      + '24072026_1217_private_invoice_document_advance_batch.sql'
  );
  const downstream = read(
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
      + '24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql'
  );
  assert.doesNotMatch(planner, /'ATTACHMENT_INDEX','DOCUMENT'/i);
  assert.doesNotMatch(planner, /'Attachment index'/i);
  assert.match(planner, /'TIMESHEET_RENDER_MODEL_V2'/i);
  assert.match(planner, /'timesheet-professional-v2'/i);
  assert.doesNotMatch(planner, /timesheet-professional-v1/i);
  assert.match(
    downstream,
    /select l\.id chunk_id,l\.resolved_document_version_id document_version_id/i
  );
  assert.match(downstream, /TIMESHEET_RENDER_MODEL_V2[\s\S]*QR_UNSIGNED/i);
  assert.match(downstream, /TIMESHEET_SIGNED_EVIDENCE_REQUIRED/i);
  assert.match(downstream, /FINAL_MERGE_SELECTIVE_V2/i);
  assert.match(downstream, /page_numbering_excluded_pages/i);
  assert.match(
    downstream,
    /input_type\s*=\s*'ELECTRONIC_TIMESHEET'[\s\S]*actual_page_count/i
  );
});

test('QR enqueue creates V8 document work and never creates legacy PDF-outbox work', () => {
  const source = read('supabase/repeatable/30082026_1232_candidate_qr_document_revision_order_v1.sql');
  const sql = functionBody(source, 'public.timesheet_qr_send_enqueue_v1');
  assert.match(sql, /insert into public\.invoice_operations/i);
  assert.match(sql, /insert into public\.invoice_document_versions/i);
  assert.match(sql, /insert into public\.invoice_operation_chunks/i);
  assert.match(sql, /timesheet-professional-v2/i);
  assert.match(sql, /DOCUMENT_QUEUED_MAIL_HELD/i);
  assert.match(sql, /DOCUMENT_READY_MAIL_QUEUED/i);
  assert.doesNotMatch(sql, /insert into public\.ts_pdfs_outbox/i);
  assert.doesNotMatch(sql, /update public\.ts_pdfs_outbox/i);
  assert.match(sql, /attachments\s*=\s*CASE[\s\S]*'\[\]'::jsonb/i);
});

test('timesheet invalidation includes QR printable identity', () => {
  const sql = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_trg_timesheet_document_invalidate.sql'
  );
  assert.match(sql, /n\.qr_token is distinct from o\.qr_token/i);
  assert.match(sql, /n\.qr_payload_json is distinct from o\.qr_payload_json/i);
  assert.match(sql, /n\.hospital_norm is distinct from o\.hospital_norm/i);
  assert.match(sql, /n\.ward_norm is distinct from o\.ward_norm/i);
});

test('snapshot freezes dynamic week, canonical references, complete units and HealthRoster V2', () => {
  const sql = read(
    'supabase/repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql'
  );
  assert.match(sql, /TIMESHEET_RENDER_MODEL_V2/i);
  assert.match(sql, /resolved_week_ending_date-6/i);
  assert.match(sql, /reference_source_row_keys/i);
  assert.match(sql, /TIMESHEET_ADDITIONAL_UNITS_V1/i);
  assert.match(sql, /minimum_blank_space_rows',1/i);
  assert.match(sql, /timesheet_additional_staleness/i);
  assert.match(sql, /TIMESHEET_ADDITIONAL_UNITS_SNAPSHOT_STALE/i);
  assert.match(sql, /HEALTHROSTER_PRESENTATION_V2/i);
  assert.match(sql, /public\.nhsp_shifts ns/i);
  assert.match(sql, /ns\.latest_import_id=s\.import_id/i);
  assert.match(sql, /ns\.hr_request_id=coalesce/i);
  assert.match(sql, /CONTROLLED_UNIQUE_FALLBACK/i);
  assert.match(sql, /'booking_reference',case[\s\S]*hr_match\.ref_num/i);
  assert.match(sql, /in\('BY_WEEK','ANY_WEEK'\)/i);
  assert.match(sql, /nullif\(lower\(btrim\(t\.hospital_norm\)\),'?'\)? hospital_norm/i);
  assert.match(sql, /nullif\(lower\(btrim\(t\.ward_norm\)\),'?'\)? ward_norm/i);
  assert.match(sql, /\\mSt Richards\\M','St Richard''s'/i);
  assert.match(sql, /concat_ws\(' - '/i);
  assert.match(sql, /'hospital_display',b\.hospital/i);
  assert.match(sql, /'ward_display',b\.ward_display/i);
  assert.match(sql, /'hospital_ward_display',b\.site_ward/i);
});

test('settings wording changes participate in invoice candidate revision', () => {
  const sql = read(
    'supabase/repeatable/27072026_1042_invoice_async_v8/'
      + '27072026_1250_20_private_invoice_candidate_triggers_install_v2.sql'
  );
  for (const field of [
    'timesheet_header_json',
    'timesheet_footer_json',
    'temporary_worker_declaration_json',
    'client_declaration_json'
  ]) {
    assert.match(sql, new RegExp(`"${field}"`));
  }
});

test('function-manifest enforcement is not configured for any environment', () => {
  const wrangler = read('wrangler.toml');
  const values = [...wrangler.matchAll(
    /INVOICE_ASYNC_FUNCTION_MANIFEST_ENFORCED\s*=\s*"([^"]+)"/g
  )].map(match => match[1]);
  assert.deepEqual(values, []);
});

test('planner V2 installer references both canonical planner definitions', () => {
  const installer = read(
    'supabase/repeatable/30072026_0046_invoice_document_planner_v2.sql'
  );
  assert.match(installer, /\\set ON_ERROR_STOP on/);
  assert.match(
    installer,
    /24072026_1217_private_invoice_document_advance_batch_v6_downstream\.sql/
  );
  assert.match(
    installer,
    /24072026_1217_private_invoice_document_advance_batch\.sql/
  );
});
test('material reference and location edits invalidate and queue exact replacements once', () => {
  const sql = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_invoice_apply_edits.sql'
  );
  assert.match(sql, /timesheet_location_updates/i);
  assert.match(sql, /v_source_updates_map/i);
  assert.match(sql, /expected_document_revision/i);
  assert.match(sql, /INVOICE_SOURCE_EDIT_STALE_REVISION/i);
  assert.match(sql, /select count\(\*\) from jsonb_each\(v_source_updates_map\)/i);
  assert.doesNotMatch(sql, /jsonb_object_length/i);
  assert.match(sql, /with desired as materialized/i);
  assert.match(sql, /t\.timesheet_id=any\(v_source_changed_ts_ids\)/i);
  assert.match(sql, /owned_line\.invoice_id=p_invoice_id/i);
  assert.match(sql, /v_source_edit_preexisting_preview/i);
  assert.doesNotMatch(sql, /\{meta,reference_source_change\}/i);
  assert.match(sql, /public\.invoice_operation_start_batch/i);
  assert.match(sql, /SOURCE_EDIT_REPLACEMENT_RESULT_INVALID/i);
  assert.match(sql, /SOURCE_EDIT_REPLACEMENT_REJECTED/i);
  assert.match(sql, /SOURCE_EDIT_REPLACEMENT_BLOCKED/i);
  assert.match(sql, /VIEW_TIMESHEET_DOCUMENT/i);
  assert.match(sql, /VIEW_INVOICE_DOCUMENT/i);
  assert.match(sql, /timesheet-professional-v2/i);
  assert.match(sql, /INVOICE_SOURCE_EDIT_QUEUE_V1/i);
  assert.match(sql, /v_validated_operations/i);
});

test('source-edit payloads are allowlisted and never replace SEGMENTS schedules', () => {
  const sql = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_invoice_apply_edits.sql'
  );
  assert.match(sql, /UNSUPPORTED_FIELD/i);
  assert.match(sql, /allowed_reference_fields/i);
  assert.match(sql, /allowed_location_fields/i);
  assert.match(sql, /source_mode/i);
  assert.match(sql, /SEGMENTS/i);
  assert.match(sql, /canonical_actual_schedule_json/i);
  assert.match(sql, /segment_reference_changed/i);
  assert.match(sql, /invoice_locked_invoice_id/i);
  assert.match(sql, /set_config\('cloudtms\.invoice_source_edit_marker'/i);
  assert.match(sql, /txid_current\(\)/i);
  assert.doesNotMatch(
    sql,
    /actual_schedule_json=case when \(d\.value->>'has_actual_schedule_json'\)::boolean\s+then case when jsonb_typeof\(d\.value->'actual_schedule_json'\)='null'/i
  );
});

test('source-edit invalidation uses a transaction-local exact-once marker', () => {
  const timesheetTrigger = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_trg_timesheet_document_invalidate.sql'
  );
  assert.match(timesheetTrigger, /cloudtms\.invoice_source_edit_marker/i);
  assert.match(timesheetTrigger, /current_setting/i);
  assert.match(timesheetTrigger, /txid_current\(\)/i);
  assert.match(timesheetTrigger, /expected_revision/i);
  assert.match(timesheetTrigger, /invoice_breakdown_json/i);
  assert.doesNotMatch(
    timesheetTrigger,
    /jsonb_array_elements\(ts\.actual_schedule_json\)[\s\S]*authoritative current timesheet schedule/i
  );
});

test('invoice line reference cache uses invoice-owned financial segments', () => {
  const sql = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_trg_invoice_document_invalidate.sql'
  );
  assert.match(sql, /timesheets_financials/i);
  assert.match(sql, /invoice_locked_invoice_id/i);
  assert.match(sql, /invoice_id::text/i);
});

test('invoice detail exposes direct timesheet hospital and ward source values', () => {
  const sql = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_invoice_detail_get.sql'
  );
  assert.match(sql, /'hospital_norm',t\.hospital_norm/i);
  assert.match(sql, /'ward_norm',t\.ward_norm/i);
  assert.match(sql, /'document_revision',t\.document_revision/i);
  assert.match(sql, /'can_edit_source'/i);
  assert.match(sql, /'source_edit_blocker_codes'/i);
  assert.match(sql, /'timesheet_reference_sources_by_id'/i);
  assert.match(sql, /'source_edit_queue_contract','INVOICE_SOURCE_EDIT_QUEUE_V1'/i);
  assert.match(sql, /invoice_locked_invoice_id/i);
});

test('atomic source-edit installer references exactly the four canonical definitions', () => {
  const installer = read(
    'supabase/repeatable/30072026_1603_invoice_source_edit_atomic_queue_contract.sql'
  );
  assert.match(installer, /\\set ON_ERROR_STOP on/);
  const included = [...installer.matchAll(/\\ir\s+([^\r\n]+)/g)]
    .map(match => match[1].trim());
  assert.deepEqual(included, [
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_timesheet_document_invalidate.sql',
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_invoice_document_invalidate.sql',
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql',
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_detail_get.sql'
  ]);
  assert.equal((installer.match(/create\s+or\s+replace\s+function/gi) || []).length, 0);
});

test('refs, ward and selective numbering installer references every canonical authority', () => {
  const installer = read(
    'supabase/repeatable/30072026_1216_invoice_timesheet_refs_ward_selective_numbering.sql'
  );
  assert.match(installer, /\\set ON_ERROR_STOP on/i);
  assert.match(installer, /\bbegin;/i);
  assert.match(installer, /\bcommit;/i);
  for (const authority of [
    '25072026_0002_private_invoice_presentation_snapshot_batch.sql',
    '23072026_2207_invoice_detail_get.sql',
    '23072026_2207_invoice_apply_edits.sql',
    '24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql',
    '24072026_1217_invoice_work_context_batch.sql',
    '24072026_1217_invoice_work_complete_batch.sql'
  ]) assert.match(installer, new RegExp(authority.replaceAll('.', '\\.')));
});

test('reference invalidation installer references the canonical invoice edit definition', () => {
  const installer = read(
    'supabase/repeatable/30072026_0203_invoice_apply_edits_reference_invalidation.sql'
  );
  assert.match(installer, /\\set ON_ERROR_STOP on/);
  assert.match(
    installer,
    /23072026_2207_invoice_apply_edits\.sql/
  );
});
test('invoice edits contain the complete supported add-timesheet implementation', () => {
  const sql = read(
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
      + '23072026_2207_invoice_apply_edits.sql'
  );
  assert.doesNotMatch(sql, /rest of function unchanged/i);
  assert.doesNotMatch(sql, /^\s*--\s*\.\.\.\s*$/m);
  assert.match(sql, /-- 3\) Add timesheets \(full parity: hours \+ additional \+ expenses \+ mileage\)/i);
  assert.match(sql, /'HOURS_DAILY'/i);
  assert.match(sql, /'HOURS_WEEKLY'/i);
  assert.match(sql, /'ADDITIONAL_RATE'/i);
  assert.match(sql, /'ADDITIONAL_RATE_DAILY'/i);
  assert.match(sql, /'EXPENSES_TOTAL'/i);
  assert.match(sql, /never derives economics from mutable live rows/i);
  assert.match(sql, /private\._invoice_generation_vat_policy_batch/i);
  assert.match(sql, /INVOICE_EDIT_VAT_POLICY_UNRESOLVED/i);
  assert.match(sql, /\('TRAVEL',coalesce\(snap\.travel_pay_ex_vat/i);
  assert.match(sql, /\('ACCOMMODATION',coalesce\(snap\.accommodation_pay_ex_vat/i);
  assert.match(sql, /\('OTHER',coalesce\(snap\.other_pay_ex_vat/i);
  assert.match(sql, /else 'EXPENSE_'\|\|e\.code end/i);
  assert.match(sql, /'MILEAGE'/i);
  assert.match(sql, /perform public\._inv_lock_segments_for_invoice\(p_invoice_id, seg_refs\)/i);
  assert.match(sql, /v_exact_timesheet_document_r2_key/i);
  assert.doesNotMatch(sql, /docs-pdf\/timesheets\/ts_/i);
});

test('residual source-edit installer references exactly the four canonical definitions', () => {
  const installer = read(
    'supabase/repeatable/30072026_1745_invoice_source_edit_residual_fix.sql'
  );
  assert.match(installer, /\\set ON_ERROR_STOP on/);
  assert.match(installer, /\bbegin;/i);
  assert.match(installer, /\bcommit;/i);
  const included = [...installer.matchAll(/\\ir\s+([^\r\n]+)/g)]
    .map(match => match[1].trim());
  assert.deepEqual(included, [
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_timesheet_document_invalidate.sql',
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_invoice_document_invalidate.sql',
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_apply_edits.sql',
    '23072026_2207_invoice_queue_stage1_revision8/23072026_2207_invoice_detail_get.sql'
  ]);
  assert.equal((installer.match(/create\s+or\s+replace\s+function/gi) || []).length, 0);
});
