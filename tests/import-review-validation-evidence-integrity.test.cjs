const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const read = (relative) => fs.readFileSync(path.join(__dirname, '..', relative), 'utf8');
const core = read('supabase/repeatable/21072026_1820_00_import_review_internal_core.sql');
const lifecycle = read('supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql');
const email = read('supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql');
const weekly = read('supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql');
const worker = read('broker/src/import-review.js');
const entry = read('broker/src/index.js');

test('query evidence requires a complete exact current timesheet document', () => {
  assert.match(core, /_import_review_query_evidence_core_v1/);
  assert.match(core, /current_document_version_id/);
  assert.match(core, /v_doc\.source_revision=v_ts\.document_revision::text/);
  assert.match(core, /v_doc\.status='READY'/);
  assert.match(core, /TIMESHEET_PRESENT_BUT_INVOICED/);
  assert.match(core, /TIMESHEET_EVIDENCE_INCOMPLETE/);
  assert.match(core, /TIMESHEET_EVIDENCE_PREPARING/);
  assert.match(core, /r2_nurse_key[\s\S]*r2_auth_key/);
  assert.match(core, /qr_signed_hash[\s\S]*qr_signed_at_utc/);
  assert.match(core, /v_is_signed_qr:=v_ts\.qr_status is not null/);
  assert.match(core, /jsonb_array_length[\s\S]*and not exists/);
});

test('catalog holds Daily records independently and Weekly validation atomically', () => {
  assert.match(core, /daily-validation-held-v1/);
  assert.match(core, /weekly-validation-held-v2/);
  assert.match(core, /weekly_validation_badge_code','WEEKLY_VALIDATION_HELD'/);
  assert.match(core, /hold\.action_category in \('EMAIL','PENDING','BLOCKED'\)/);
  assert.match(weekly, /tmp_mode_a_eligible_groups/);
  assert.match(weekly, /selected_row\.action_kind='NO_ACTION'/);
  assert.match(weekly, /hold\.action_category in \('EMAIL','PENDING','BLOCKED'\)/);
  assert.match(weekly, /join tmp_mode_a_eligible_groups eligible/);
});

test('query emails re-attest and attach the exact current document', () => {
  assert.match(email, /TIMESHEET_QUERY_ATTACHMENT_EVIDENCE_STALE/);
  assert.match(email, /attachment_evidence->>'evidence_fingerprint' is distinct from summary_json->>'attachment_fingerprint'/);
  assert.match(email, /Please can you kindly make amendments on HealthRoster for the below shifts/);
  assert.match(email, /The relevant timesheets have been attached to this email/);
  assert.match(email, /Many thanks/);
  assert.match(email, /v_agency_name/);
  assert.match(email, /body_text,attachments,status/);
  assert.match(email, /'r2_key'[\s\S]*'filename'/);
  assert.match(email, /when nullif\(cx\.value->>'ref_before',''\) is not null[\s\S]*cx\.value->>'ref_before'=cx\.value->>'ref_after'/);
});

test('Daily query actions freeze complete email-table evidence', () => {
  assert.match(core, /'comparison_key','hr-row:'\|\|m\.id::text/);
  assert.match(core, /'timesheet_start',to_char\(m\.worked_start_iso at time zone 'Europe\/London','HH24:MI'\)/);
  assert.match(core, /'healthroster_start',to_char\(m\.start_time_local,'HH24:MI'\)/);
  assert.match(core, /'ref_before',m\.reference_number/);
  assert.match(core, /'ref_after',m\.hr_request_id/);
  assert.match(core, /'healthroster_unit',coalesce\(nullif\(m\.payload_json->>'Unit',''\)/);
  assert.match(core, /'healthroster_request_grade',coalesce\(nullif\(m\.payload_json->>'Request Grade',''\)/);
  assert.match(core, /'comparisons',i\.email_comparisons/);
  assert.match(core, /'issue-evidence-v2',i\.issue_fingerprint,i\.email_comparisons::text/);
});

test('Weekly query actions freeze source unit and request grade into email evidence', () => {
  assert.match(core, /cx\.value\|\|jsonb_strip_nulls\(jsonb_build_object\(/);
  assert.match(core, /left join public\.hr_rows hr on hr\.id=nullif\(cx\.value->>'hr_row_id',''\)::uuid/);
  assert.match(core, /'healthroster_unit',coalesce\(nullif\(hr\.payload_json->>'Unit',''\)/);
  assert.match(core, /'healthroster_request_grade',coalesce\(nullif\(hr\.payload_json->>'Request Grade',''\)/);
});

test('query email renderer includes source unit and request grade when available', () => {
  assert.match(email, />Unit \/ ward<\/th>/);
  assert.match(email, />Request grade<\/th>/);
  assert.match(email, /public\._import_review_html_escape_v1\(l\.source_location\)/);
  assert.match(email, /public\._import_review_html_escape_v1\(l\.request_grade\)/);
  assert.match(email, /cx\.value->>'healthroster_unit'/);
  assert.match(email, /cx\.value->>'healthroster_request_grade'/);
});

test('partial-apply re-attestation excludes already completed outcomes before fingerprinting', () => {
  assert.match(lifecycle, /delete from pg_temp\.review_apply_fresh_actions n\s+using public\.import_review_action_outcomes o/);
  assert.match(lifecycle, /where o\.import_id=p_import_id and o\.action_id=n\.action_id;\s+select public\._import_review_hash_v1/);
});

test('document generation is asynchronous and idempotently nudged', () => {
  assert.match(lifecycle, /import_review_attachment_preparation_targets_v1/);
  assert.match(lifecycle, /VIEW_TIMESHEET_DOCUMENT/);
  assert.match(lifecycle, /IMPORT_REVIEW_EMAIL_EVIDENCE/);
  assert.match(worker, /scheduleAttachmentPreparation/);
  assert.match(worker, /invoice_operation_start_batch/);
  assert.match(worker, /ctx\.waitUntil\(task\)/);
  assert.match(entry, /nudgeInvoiceOperations/);
});

test('history exposes SUCCESS only from settled server evidence', () => {
  assert.match(lifecycle, /'display_status',case/);
  assert.match(lifecycle, /status='APPLIED'/);
  assert.match(lifecycle, /follow_up_status in \('COMPLETE','NOT_REQUIRED'\)/);
  assert.match(lifecycle, /applied_email_count=0/);
  assert.match(lifecycle, /validation_incomplete_count=0/);
  assert.match(lifecycle, /then 'SUCCESS'/);
});

test('scope stays outside Banking Pay and invoice/TSFIN writers', () => {
  for (const sql of [lifecycle, email, weekly]) {
    assert.doesNotMatch(sql, /create or replace function public\.pay_/i);
  }
  assert.doesNotMatch(core, /create or replace function public\.invoice_create/i);
  assert.doesNotMatch(core, /create or replace function public\.tsfin_/i);
});
