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
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
    + '23072026_2207_email_outbox_claim_ready_batch.sql'
);
const workflowSource = read(
  'supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql'
);

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
