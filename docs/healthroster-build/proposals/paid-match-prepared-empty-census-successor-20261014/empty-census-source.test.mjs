import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { scanManagedSql } from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
const dir = path.dirname(fileURLToPath(import.meta.url));
const read = file => fs.readFileSync(path.join(dir, file), 'utf8');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

test('three complete owners reverse five unique hunks to exact frozen Prepared17', () => {
  const proof = JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));
  assert.equal(proof.records.length, 3);
  assert.equal(proof.records.reduce((n, r) => n + r.hunks.length, 0), 5);
  for (const r of proof.records) {
    let after = read('forward/' + r.file);
    assert.equal(sha(after), r.after_sha256);
    for (const h of [...r.hunks].reverse()) { assert.equal(after.split(h.after).length, 2); after = after.replace(h.after, h.before); }
    assert.equal(after, read('before/' + r.file));
    assert.equal(sha(after), r.before_sha256);
  }
});
test('actual helper no longer returns on stored empty before real discovery', () => {
  const before = read('before/01_prepared_paid_choice_census.sqlinc');
  const after = read('forward/01_prepared_paid_choice_census.sqlinc');
  assert.ok(before.indexOf("then return '{}'::jsonb;end if;") < before.indexOf('for v_row in'));
  assert.doesNotMatch(after.slice(0, after.indexOf('for v_row in')), /\breturn\b/);
  assert.ok(after.indexOf('for v_row in') < after.indexOf('v_census is distinct from revision.paid_overlap_choice_census'));
  assert.match(after, /where raw\.upload_id=revision\.upload_id order by raw\.source_row_ordinal,raw\.id/);
  assert.match(after, /resolution\.generation=coalesce\(publication\.projection_generation,publication\.authority_scope_version\)/);
});
test('source-selected whole-census predicate catches empty additions, removal and mutation', () => {
  const source = read('forward/01_prepared_paid_choice_census.sqlinc');
  assert.match(source, /if v_census is distinct from revision\.paid_overlap_choice_census then\s*raise exception 'WEEKLY_SOURCE_PAID_MATCH_PREPARED_CENSUS_CHANGED' using errcode='40001'/);
  const compare = (stored, current) => JSON.stringify(stored) === JSON.stringify(current) ? 'MATCH' : '40001';
  assert.equal(compare({}, {}), 'MATCH');
  assert.equal(compare({}, { row1: { choice_id: 1, pay_effect: 'NONE' } }), '40001');
  assert.equal(compare({ row1: { choice_id: 1 } }, {}), '40001');
  assert.equal(compare({ row1: { choice_id: 1 } }, { row1: { choice_id: 2 } }), '40001');
  assert.equal(compare({ row1: { choice_id: 1 } }, { row1: { choice_id: 1 } }), 'MATCH');
});
test('prepared CYCLE calls actual recheck even for empty while native NHSP empty remains unchanged', () => {
  const s = read('forward/02_complete_prepared_context.sqlinc');
  const gate = "if v_revision.authority_scope_kind='CYCLE' or v_revision.paid_overlap_choice_census<>'{}'::jsonb then";
  assert.match(s, /v_paid_choice_census:=private\.weekly_source_paid_match_prepared_census_recheck_v1\(v_revision.id,v_session.actor_user_id\)/);
  assert.ok(s.indexOf(gate) < s.indexOf('for v_root in'));
  const invoke = (scope, hasChoice) => scope === 'CYCLE' || hasChoice;
  assert.equal(invoke('CYCLE', false), true);
  assert.equal(invoke('CYCLE', true), true);
  assert.equal(invoke('NHSP_REPORT_SCOPE', false), false);
  assert.equal(invoke('NHSP_REPORT_SCOPE', true), true);
  assert.match(read('forward/01_prepared_paid_choice_census.sqlinc'), /revision.authority_scope_kind is distinct from 'CYCLE'/);
});
test('apply empty proof remains after genuine root refusals before first COMMITTING and after replay', () => {
  const s = read('forward/04_complete_apply_census_recheck.sqlinc');
  const proof = s.indexOf('perform private.weekly_source_paid_match_prepared_census_recheck_v1');
  assert.match(s, /if v_prepared_revision.authority_scope_kind='CYCLE' or v_prepared_revision.paid_overlap_choice_census<>'\{\}'::jsonb then/);
  assert.ok(s.indexOf("if v_session.state='APPLIED'") < proof);
  for (const refusal of ['WEEKLY_SOURCE_CORRECTION_ALREADY_PAID', 'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS', 'WEEKLY_SOURCE_CORRECTION_ROOT_NOT_MUTABLE']) assert.ok(s.indexOf(refusal) < proof);
  assert.ok(proof < s.indexOf("set state='COMMITTING',apply_idempotency_key"));
});
test('optional empty DTO shape and exact actor/session/publication/CURRENT choice checks stay intact', () => {
  const s = read('forward/02_complete_prepared_context.sqlinc');
  assert.match(s, /case when v_revision.paid_overlap_choice_census='\{\}'::jsonb then '\{\}'::jsonb else jsonb_build_object\(\s*'paid_overlap_choice_census',v_paid_choice_census\) end/);
  const helper = read('forward/01_prepared_paid_choice_census.sqlinc');
  for (const marker of ['revision.finalised_by_user_id is distinct from p_actor_user_id', 'replacement_correction_upload_id=revision.upload_id', 'correction_session_id=session.id', "state='CORRECTION_READY'", 'weekly_source_paid_overlap_choice_current_v1(v_row.id,p_actor_user_id)', 'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_CHOICE_V1', 'choice_proof_sha256']) assert.ok(helper.includes(marker), marker);
});
test('current legal-path source pins prove ordinary/correction boundary without manufacturing a late choice', () => {
  const disposition = JSON.parse(read('CURRENT_NATIVE_PATH_DISPOSITION.json'));
  assert.equal(disposition.observed_legal_interleaving, false);
  assert.equal(disposition.literal_choice_writer_occurrences.length, 2);
  const writer = read('input/08102026_1635_weekly_source_released_client_eligibility_v1.sql');
  for (const marker of ["v_upload.purpose<>'ORDINARY'", "v_upload.state<>'CURRENT'", "v_cycle.state in ('FINALISING','FINALISED')", "v_publication.state<>'CURRENT'"]) assert.ok(writer.includes(marker), marker);
  const schema = read('input/30092026_2129_weekly_source_office_resolution.sql');
  assert.match(schema, /revoke all on private.weekly_source_office_row_choices,private.weekly_source_office_rechecks from public,anon,authenticated,service_role/);
  const admit = (purpose, uploadState, cycleState) => purpose === 'ORDINARY' && uploadState === 'CURRENT' && !['FINALISING', 'FINALISED'].includes(cycleState);
  assert.equal(admit('ORDINARY', 'CURRENT', 'OPEN'), true);
  assert.equal(admit('FINAL_SOURCE_CORRECTION', 'CORRECTION_READY', 'FINALISED'), false);
  assert.equal(admit('ORDINARY', 'CURRENT', 'FINALISED'), false);
});
test('finite existing signatures and owner ACL contain no new writer or financial authority', () => {
  for (const file of fs.readdirSync(path.join(dir, 'forward'))) {
    const s = read('forward/' + file), scan = scanManagedSql(s);
    assert.ok(scan.statements.length); assert.equal(scan.meta.length, 0);
  }
  const s = read('forward/01_prepared_paid_choice_census.sqlinc');
  assert.match(s, /language plpgsql stable security definer/);
  assert.match(s, /from public,anon,authenticated,service_role/);
  assert.doesNotMatch(s, /\b(?:insert into|update public\.|delete from|create table|paid_at|paid_evidence_state)\b/i);
});
