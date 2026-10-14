import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { exactAtomicBody, scanManagedSql } from '../scripts/cloudtms-managed-release-sql.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const read=name=>fs.readFileSync(path.join(root,name==='wp_u3_native_read_vectors.sqlinc'
  ? 'supabase/verification/support' : 'supabase/repeatable/healthroster_wp_u3_source_proposals',name),'utf8');
const pages=['wp_u3_submission_context.sqlinc','wp_u3_check_only_query_rows.sqlinc','wp_u3_client_week_context.sqlinc',
  'wp_u3_signed_submission_read_context.sqlinc','wp_u3_client_calendar.sqlinc'];

test('private read proposals retain classified packaging statements with fixed owner-only boundaries',()=>{
  for(const name of pages){const source=read(name),parsed=scanManagedSql(exactAtomicBody(source));
    assert.equal(parsed.statements.length,3,name);assert.equal(parsed.meta.length,0,name);
    assert.match(source,/create or replace function private\./i);
    assert.match(source,/language plpgsql stable security definer/i);
    assert.match(source,/set search_path to 'pg_catalog','public','private','pg_temp'/i);
    assert.match(source,/owner to postgres/i);assert.match(source,/revoke all on function private\.[\s\S]*from public,anon,authenticated,service_role/i);
    assert.doesNotMatch(source,/create (?:table|index)|create or replace function public\.|\bgrant\b/i);
  }
});
test('read bodies contain no business writer, network owner or entity lock, including the only dynamic invocation',()=>{
  for(const name of pages){const source=read(name),body=source.split('$function$')[1];
    const tokens=scanManagedSql(body).statements.flatMap(s=>s.tokens);
    for(const forbidden of ['INSERT','UPDATE','DELETE','TRUNCATE','LOCK','NOTIFY','CALL']) assert.ok(!tokens.includes(forbidden),`${name}:${forbidden}`);
    assert.doesNotMatch(body,/for\s+(?:update|share)|pg_advisory|http_|dblink|tsqueryenqueue|tsfin_write|hr_issue_email.*enqueue/i);
    if(tokens.includes('EXECUTE')) assert.match(body,/execute 'select private\.weekly_source_protected_wait_witness_v1\(\$1,\$2\)'/);
  }
});
test('actual submission explicitly delegates native claim/hash and verified legacy source, with no booked-clock evidence',()=>{
  const source=read(pages[0]);
  assert.match(source,/private\._candidate_sha256_jsonb_v1\(w\.immutable_submission_json\)/);
  assert.match(source,/private\._candidate_daily_canonical_save_input_v1\(w\.id,w\.generation\)/);
  assert.match(source,/public\._import_review_query_evidence_core_v1\(t\.timesheet_id\)/);
  assert.match(source,/v_evidence->>'source_complete'/);assert.match(source,/v_evidence->>'document_ready'/);
  assert.doesNotMatch(source,/scheduled_start|scheduled_end|planned_start|rota|booking_id/i);
});
test('one native query projection reads existing D10/email delivery provenance and fences noncounted source information',()=>{
  const source=read(pages[1]);
  for(const owner of ['weekly_source_issued_signed_query_evidence_v1','import_review_decisions','hr_issue_emails',
    'hr_issue_email_deliveries','hr_issue_email_delivery_items','mail_outbox']) assert.ok(source.includes(owner),owner);
  assert.match(source,/'item_kind','SOURCE_INFORMATION'/);assert.match(source,/'counts_for_row_admission',false/);
  assert.match(source,/exists\(select 1 from actual admitted/);
  assert.match(source,/where v_admin and d\.client_id/);
  assert.match(source,/'native_issues',case when v_admin/);assert.match(source,/'query_evidence',case when v_admin/);
  assert.match(source,/WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED/);assert.doesNotMatch(source,/\boffset\b/i);
});
test('Source context exposes process history/unknown WAIT, without a parallel paid or withholding classifier',()=>{
  const source=read(pages[2]);
  for(const owner of ['weekly_source_office_authority_v1','weekly_source_effective_rule_set_v1','weekly_source_signed_submission_read_context_v1',
    'weekly_source_client_cycle_completions','weekly_source_final_revisions','weekly_source_report_scopes','weekly_source_protected_wait_witness_v1']) assert.ok(source.includes(owner),owner);
  assert.match(source,/v_wait:=null/);assert.match(source,/'earlier_open_weeks',v_earlier_open/);
  assert.match(source,/jsonb_agg\(jsonb_build_object\('code',reason,'unproved_timesheet_count'/);
  assert.doesNotMatch(source,/'paid'\s*,|'holds_pay'\s*,|'withheld_amount_pence'\s*,|current_setting|set_config/i);
});
test('pending Daily read arm leaves the legal F1 predicate unchanged and offers no query/action admission',()=>{
  const source=read(pages[3]);assert.match(source,/v_native:=private\.weekly_source_signed_check_only_v1\(p_timesheet_id\)/);
  assert.match(source,/public\._import_review_effective_authority_core_v1\('HR_DAILY',t\.contract_id,v_client,v_work_date\)/);
  assert.match(source,/'SUBMITTED_PENDING_NATIVE_MATERIALISATION','query_route_available',false/);
  assert.match(source,/'pending_materialisation',true,'actions_held',true/);
  assert.doesNotMatch(source,/scheduled_start|scheduled_end|planned_start|booking_id/i);
  assert.match(read(pages[1]),/when \(a\.route_read->>'query_route_available'\)::boolean is not true then null/);
});
test('native verifier refuses missing or empty fixtures rather than passing absent behavior',()=>{
  const source=read('wp_u3_native_read_vectors.sqlinc');
  assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,1);
  assert.match(source,/WP_U3_NATIVE_FIXTURES_REQUIRED/);assert.match(source,/WP_U3_NATIVE_FIXTURE_SET_EMPTY/);
  for(const table of ['wp_u3_submission_fixtures','wp_u3_read_admission_fixtures','wp_u3_context_fixtures','wp_u3_query_fixtures']) assert.ok(source.includes(`pg_temp.${table}`));
  assert.match(source,/aclexplode/);assert.match(source,/expected_actual_submitted/);assert.match(source,/expected_invoice_hidden/);
});
