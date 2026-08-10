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

const qrSource = read('supabase/repeatable/07082026_2224_candidate_app_weekly_office_replacements_v1.sql');
const invoiceCompleteSource = read(
  'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/'
    + '24072026_1217_invoice_work_complete_batch.sql'
);
const claimSource = read(
  'supabase/repeatable/23072026_2207_email_outbox_claim_ready_batch.sql'
);
const workflowSource = read(
  'supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql'
);
const qrSettingsSource = read(
  'supabase/repeatable/07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql'
);
const routeSource = read('supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql');
const rejectSource = read('supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql');
const backendSource = read('broker/src/candidate-app-backend.js');
const compileFixture = read('tests/fixtures/07082026_2155_candidate_app_local_compile_base.sql');

test('PAPER preparation atomically composes the canonical held-mail authority', () => {
  const sql = functionBody(workflowSource, 'public.candidate_workflow_transition_atomic_v1');
  assert.match(sql, /v_action='PAPER_PREPARE'[\s\S]*timesheet_qr_send_enqueue_v1\(\$1,\$2,\$3,\$4,\$5\)/i);
  assert.match(sql, /CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE/i);
  assert.match(sql, /CANDIDATE_PAPER_PACK_QUEUE_FAILED/i);
  assert.match(sql, /CANDIDATE_PAPER_OUTBOX_NOT_READY/i);
  assert.match(sql, /candidate_paper_mail\.payment_scope_json->>'candidate_workflow_id'=v_workflow\.id::text/i);
  assert.match(sql, /last_mutation_response_json=v_response/i);
  const prepare = sql.slice(sql.indexOf("elsif v_action='PAPER_PREPARE'"), sql.indexOf("elsif v_action='PAPER_RETURN'"));
  assert.ok(prepare.indexOf("state='AWAITING_PAPER_RETURN'") < prepare.indexOf('timesheet_qr_send_enqueue_v1'));
  assert.ok(prepare.indexOf('timesheet_qr_send_enqueue_v1') < prepare.indexOf('last_mutation_response_json=v_response'));
});

test('QR enqueue atomically owns Candidate PAPER mail identity and hold state', () => {
  const sql = functionBody(qrSource, 'public.timesheet_qr_send_enqueue_v1');
  assert.match(sql, /route\s*=\s*'PAPER'[\s\S]*state\s*=\s*'AWAITING_PAPER_RETURN'/i);
  assert.match(sql, /v_candidate_paper_workflow_count\s*>\s*1[\s\S]*CANDIDATE_PAPER_WORKFLOW_CONFLICT/i);
  for (const key of [
    'candidate_workflow_id', 'candidate_workflow_generation',
    'paper_return_manifest_sha256', 'candidate_paper_pack_ready'
  ]) assert.match(sql, new RegExp(`'${key}'`, 'i'));
  assert.match(sql, /v_candidate_paper_workflow_count\s*=\s*1[\s\S]*CANDIDATE_PAPER_PACK_PENDING/i);
  assert.match(sql, /v_mail_held_until_pdf_rendered\s*:=\s*v_candidate_paper_workflow_count\s*=\s*1/i);
  assert.match(sql, /attachments\s*=\s*CASE[\s\S]*WHEN v_mail_held_until_pdf_rendered THEN '\[\]'::jsonb/i);
  assert.match(sql, /payment_scope_json\s*=\s*v_mail_scope_json/i);
  assert.match(sql, /CANDIDATE_PAPER_OUTBOX_PRESERVED/i);
  assert.match(sql, /CANDIDATE_PAPER_MAIL_FAILED/i);
});

test('ordinary QR document completion cannot release a Candidate-bound mail row', () => {
  const sql = functionBody(invoiceCompleteSource, 'public.invoice_work_complete_batch');
  const releaseStart = sql.toLowerCase().indexOf('qr_mail_release as');
  const releaseEnd = sql.toLowerCase().indexOf('issue_wake as', releaseStart);
  const release = sql.slice(releaseStart, releaseEnd);
  assert.match(release, /payment_scope_json->>'candidate_workflow_id'/i);
  assert.match(release, /is null/i);
  assert.match(release, /candidate_submission_workflows[\s\S]*AWAITING_PAPER_RETURN/i);
});

test('mail claim fails closed until the exact complete Candidate PAPER pack is ready', () => {
  const sql = functionBody(claimSource, 'public.email_outbox_claim_ready_batch');
  for (const key of [
    'candidate_workflow_id', 'candidate_workflow_generation', 'paper_return_manifest_sha256',
    'candidate_paper_pack_ready', 'candidate_complete_pack_storage_key',
    'candidate_complete_pack_sha256', 'candidate_complete_pack_size_bytes',
    'candidate_complete_pack_page_count', 'candidate_complete_pack_media_type'
  ]) assert.match(sql, new RegExp(key, 'i'));
  assert.match(sql, /jsonb_array_length\(mo\.attachments\)\s*=\s*1/i);
  assert.match(sql, /mail_held_until_pdf_rendered[\s\S]*in\('false','f','0','no'\)/i);
  assert.match(sql, /mail_hold_reason[\s\S]*is null/i);
  assert.match(sql, /sha256[\s\S]*\^\[0-9a-f\]\{64\}\$/i);
  assert.match(sql, /size_bytes[\s\S]*\^\[1-9\]\[0-9\]\{0,18\}\$/i);
  assert.match(sql, /payment_scope_json\s*\?\s*'candidate_paper_pack_ready'/i);
  assert.match(sql, /payment_scope_json\s*\?\s*'candidate_complete_pack_storage_key'/i);
  assert.match(sql, /mail_hold_reason[\s\S]*CANDIDATE_PAPER_PACK_PENDING/i);
});

test('PAPER release is a service-only atomic workflow action and the backend is only an adapter', () => {
  const sql = functionBody(workflowSource, 'public.candidate_workflow_transition_atomic_v1');
  const release = sql.slice(sql.indexOf("elsif v_action='PAPER_PACK_RELEASE'"), sql.indexOf("elsif v_action='PAPER_RETURN'"));
  assert.match(release, /state<>'AWAITING_PAPER_RETURN'[\s\S]*route<>'PAPER'/i);
  assert.match(release, /for update/i);
  assert.match(release, /update public\.mail_outbox[\s\S]*candidate_paper_pack_ready',true/i);
  assert.match(release, /insert into public\.candidate_notifications[\s\S]*on conflict\(dedupe_key\) do nothing/i);
  assert.match(sql, /service_paper_pack_release/i);

  const start = backendSource.indexOf('async function releaseCandidatePaperPack');
  const end = backendSource.indexOf('async function assembleCandidatePaperPack', start);
  const adapter = backendSource.slice(start, end);
  assert.match(adapter, /candidate_workflow_transition_atomic_v1/);
  assert.match(adapter, /PAPER_PACK_RELEASE/);
  assert.doesNotMatch(adapter, /restWrite\(env,\s*'mail_outbox'/);
  assert.doesNotMatch(adapter, /restWrite\(env,\s*'candidate_notifications'/);
});

test('one retirement helper closes mail, notification and QR authority for every retirement caller', () => {
  const helper = functionBody(workflowSource, 'private._candidate_paper_delivery_retire_v1');
  assert.match(helper, /CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS/);
  assert.match(helper, /candidate_paper_generation_retired',true/i);
  assert.match(helper, /CANDIDATE_PAPER_GENERATION_RETIRED/);
  assert.match(helper, /v_workflow\.state not in \('AWAITING_PAPER_RETURN','FINALISED'\)/i);
  assert.match(helper, /when v_workflow\.state='FINALISED' then greatest\(v_workflow\.generation-1,1\)/i);
  assert.match(helper, /candidate_workflow_generation'=v_delivery_generation::text/i);
  assert.match(helper, /v_qr_source_timesheet_id:=v_mail\.context_id/i);
  assert.match(helper, /MULTIPLE_MAIL_CONTEXTS/);
  assert.match(helper, /MULTIPLE_QR_TOKEN_HASHES/);
  assert.match(helper, /v_qr_source\.contract_id is distinct from v_workflow\.contract_id/i);
  assert.match(helper, /current_source\.booking_id=v_qr_source\.booking_id/i);
  assert.match(helper, /CURRENT_QR_TOKEN_HASH_MISMATCH/);
  assert.match(helper, /'rejected_target_timesheet_id',v_rejected_target_timesheet_id/i);
  assert.match(helper, /'qr_already_invalidated',v_qr_already_invalidated/i);
  assert.match(helper, /state='DISMISSED'/i);
  assert.match(helper, /qr_token=null/i);
  assert.match(workflowSource, /v_action='AMEND'[\s\S]*_candidate_paper_delivery_retire_v1/);
  assert.match(workflowSource, /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*_candidate_paper_delivery_retire_v1/);
  assert.match(routeSource, /_timesheet_route_supersede_candidate_v1[\s\S]*_candidate_paper_delivery_retire_v1/);
  assert.match(rejectSource, /candidate_submission_reject_atomic_v1[\s\S]*_candidate_paper_delivery_retire_v1/);
  assert.match(rejectSource, /v_workflow\.route='PAPER'[\s\S]*v_workflow\.state in \('AWAITING_PAPER_RETURN','FINALISED'\)/i);
  assert.match(rejectSource, /CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN/);
  assert.match(qrSettingsSource, /update public\.timesheet_evidence as evidence_row[\s\S]*where evidence_row\.timesheet_id=v_current\.timesheet_id/i);
});

test('Candidate PAPER hashing resolves pgcrypto exactly as the TEST schema exposes it', () => {
  const helper = functionBody(workflowSource, 'private._candidate_paper_delivery_retire_v1');
  const qr = functionBody(qrSource, 'public.timesheet_qr_send_enqueue_v1');
  assert.match(helper, /extensions\.digest\(\s*convert_to\(timesheet_row\.qr_token,'UTF8'\)/i);
  assert.doesNotMatch(helper.replace(/extensions\.digest/gi, ''), /\bdigest\s*\(/i);
  assert.match(qr, /extensions\.digest\(\s*convert_to\(/i);
  assert.doesNotMatch(qr.replace(/extensions\.digest/gi, ''), /\bdigest\s*\(/i);
  assert.doesNotMatch(compileFixture, /function\s+public\.digest\s*\(/i);
  assert.match(compileFixture, /create extension if not exists pgcrypto with schema extensions/i);
});

test('whole-record rejection transitions the exact target-or-anchor workflow set', () => {
  const reject = functionBody(rejectSource, 'public.candidate_submission_reject_atomic_v1');
  assert.match(reject, /v_rejected_workflow_ids\s+uuid\[\]/i);
  assert.match(reject, /w\.target_timesheet_id=v_timesheet\.timesheet_id[\s\S]*w\.anchor_timesheet_id=v_timesheet\.timesheet_id/i);
  assert.match(reject, /w\.state='FINALISED'\s+and w\.target_timesheet_id=v_timesheet\.timesheet_id/i);
  assert.match(reject, /v_rejected_workflow_ids:=array_append\(v_rejected_workflow_ids,v_workflow\.id\)/i);
  assert.match(reject, /candidate_approval_requests[\s\S]*workflow_id=v_workflow\.id[\s\S]*workflow_generation=v_workflow\.artifact_generation/i);
  assert.match(reject, /candidate_submission_components[\s\S]*workflow_id=v_workflow\.id[\s\S]*workflow_generation=v_workflow\.artifact_generation/i);
  assert.match(reject, /candidate_submission_workflows[\s\S]*where id=v_workflow\.id and generation=v_workflow\.generation/i);
  assert.doesNotMatch(reject, /where target_timesheet_id=v_timesheet\.timesheet_id and state not in/i);
});

test('fresh PAPER generations own fresh delivery, QR and document identities', () => {
  const sql = functionBody(qrSource, 'public.timesheet_qr_send_enqueue_v1');
  assert.match(sql, /candidate_paper_send:[\s\S]*v_candidate_paper_workflow_generation[\s\S]*v_candidate_paper_manifest_sha256/i);
  assert.match(sql, /v_candidate_paper_workflow_count\s*=\s*1\s+AND NOT v_candidate_paper_exact_mail_exists/i);
  assert.match(sql, /WHEN v_rotate_token THEN v_now/i);
  assert.match(sql, /qr_payload_hash[\s\S]*v_complete_printable_content_hash[\s\S]*v_document_idempotency/i);
});

test('Candidate mail claim revalidates current workflow generation, route, state and manifest', () => {
  const sql = functionBody(claimSource, 'public.email_outbox_claim_ready_batch');
  assert.match(sql, /exists\([\s\S]*candidate_submission_workflows workflow/i);
  assert.match(sql, /workflow\.route='PAPER'/i);
  assert.match(sql, /workflow\.state='AWAITING_PAPER_RETURN'/i);
  assert.match(sql, /encode\(workflow\.paper_return_manifest_sha256,'hex'\)/i);
  assert.match(sql, /candidate_paper_generation_retired/i);
});
