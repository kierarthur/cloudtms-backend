import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { functionEvents } from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';

const dir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(dir, '../../../..');
const commit = '76a5dbd0721ae2c41edb049312aa81d5df5a06be';
const canonical = 'fa66183f01913e86651e82816f5e88f97e07123c';
const base = 'docs/healthroster-build/proposals/paid-match-prepared-pay-consumer-construction-20261014';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
assert.ok(!fs.existsSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json')));
fs.mkdirSync(path.join(dir, 'before'), { recursive: true });
fs.mkdirSync(path.join(dir, 'forward'), { recursive: true });
fs.mkdirSync(path.join(dir, 'input'), { recursive: true });
const records = [];
function compose(file, hunks) {
  const source = base + '/forward/' + file;
  const raw = fs.readFileSync(path.join(root, source));
  assert.deepEqual(raw, execFileSync('git', ['show', commit + ':' + source], { cwd: root, maxBuffer: 2e6 }));
  const before = raw.toString(), eol = before.includes('\r\n') ? '\r\n' : '\n';
  hunks = hunks.map(h => ({ ...h, before: h.before.replaceAll('\n', eol), after: h.after.replaceAll('\n', eol) }));
  let after = before;
  for (const h of hunks) { assert.equal(after.split(h.before).length, 2, h.name); after = after.replace(h.before, h.after); }
  let reverse = after;
  for (const h of [...hunks].reverse()) { assert.equal(reverse.split(h.after).length, 2, h.name); reverse = reverse.replace(h.after, h.before); }
  assert.equal(reverse, before);
  fs.writeFileSync(path.join(dir, 'before', file), raw);
  fs.writeFileSync(path.join(dir, 'forward', file), after);
  records.push({ source, containing_commit: commit, file, before_bytes: raw.length, before_sha256: sha(raw), after_bytes: Buffer.byteLength(after), after_sha256: sha(after), hunks, whole_reverse_verified: true });
}
compose('01_prepared_paid_choice_census.sqlinc', [{
  name: 'stored empty census must not short-circuit actual CYCLE choice discovery',
  before: "  select * into strict revision from public.weekly_source_final_revisions where id=p_final_revision_id;\n  if revision.paid_overlap_choice_census='{}'::jsonb then return '{}'::jsonb;end if;",
  after: '  select * into strict revision from public.weekly_source_final_revisions where id=p_final_revision_id;'
}]);
compose('02_complete_prepared_context.sqlinc', [
  { name: 'one actual CYCLE census read is reused by optional result construction', before: '  v_context jsonb;', after: "  v_context jsonb;\n  v_paid_choice_census jsonb;" },
  { name: 'derive actual CYCLE choices even when the immutable prepared census is empty', before: "  v_source_mode:=case when v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'", after: "  if v_revision.authority_scope_kind='CYCLE' or v_revision.paid_overlap_choice_census<>'{}'::jsonb then\n    v_paid_choice_census:=private.weekly_source_paid_match_prepared_census_recheck_v1(v_revision.id,v_session.actor_user_id);\n  end if;\n  v_source_mode:=case when v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'" },
  { name: 'reuse exact rederived census while preserving existing optional empty DTO shape', before: "'paid_overlap_choice_census',private.weekly_source_paid_match_prepared_census_recheck_v1(v_revision.id,v_session.actor_user_id)) end;", after: "'paid_overlap_choice_census',v_paid_choice_census) end;" }
]);
compose('04_complete_apply_census_recheck.sqlinc', [{
  name: 'repeat full CYCLE empty or nonempty choice reproof after native root locks and before COMMITTING',
  before: "  if v_prepared_revision.paid_overlap_choice_census<>'{}'::jsonb then",
  after: "  if v_prepared_revision.authority_scope_kind='CYCLE' or v_prepared_revision.paid_overlap_choice_census<>'{}'::jsonb then"
}]);
const pins = [];
for (const file of [
  'supabase/repeatable/08102026_1635_weekly_source_released_client_eligibility_v1.sql',
  'supabase/migrations/30092026_2129_weekly_source_office_resolution.sql'
]) {
  const raw = execFileSync('git', ['show', canonical + ':' + file], { cwd: root, maxBuffer: 3e6 });
  const saved = 'input/' + path.basename(file);
  fs.writeFileSync(path.join(dir, saved), raw);
  pins.push({ commit: canonical, file, saved, bytes: raw.length, sha256: sha(raw) });
}
const writerRaw = fs.readFileSync(path.join(dir, pins[0].saved)).toString();
const writer = functionEvents(writerRaw).events.find(e => e.kind === 'CREATE' && e.id === 'public.weekly_source_office_recheck_begin_v1(jsonb)');
assert.ok(writer);
for (const marker of ["v_upload.purpose<>'ORDINARY'", "v_upload.state<>'CURRENT'", "v_cycle.state in ('FINALISING','FINALISED')", "v_publication.state<>'CURRENT'", "for update"]) assert.ok(writer.definition.includes(marker), marker);
const literalWriters = execFileSync('git', ['grep', '-n', '-i', '-E', 'insert into private.weekly_source_office_row_choices|update private.weekly_source_office_row_choices|delete from private.weekly_source_office_row_choices', canonical, '--', 'supabase'], { cwd: root, maxBuffer: 3e6 }).toString().trim().split(/\r?\n/);
assert.equal(literalWriters.length, 2);
assert.ok(literalWriters.every(line => line.includes('insert into private.weekly_source_office_row_choices')));
fs.writeFileSync(path.join(dir, 'COMPLETE_CURRENT_REVERSES.json'), JSON.stringify({ source_only: true, records, native_input_pins: pins, original_replay_NHSP_and_mutability_guards_retained: true, legal_late_choice_interleaving_proved: false, SQL_compiled: false, native_proof: false }, null, 2) + '\n');
fs.writeFileSync(path.join(dir, 'CURRENT_NATIVE_PATH_DISPOSITION.json'), JSON.stringify({ source_only: true, canonical_commit: canonical, literal_choice_writer_occurrences: literalWriters, current_native_choice_writer: { signature: writer.id, definition_sha256: sha(writer.definition), path: pins[0].file }, current_public_paid_choice_path: 'ORDINARY CURRENT upload; not FINALISING/FINALISED; actual current upload/publication under retained scope/cycle/publication locks', prepared_replacement_path: 'FINAL_SOURCE_CORRECTION CORRECTION_READY upload/publication and genuine session; original PREPARE/apply/replay/mutability owners retained', direct_service_DML_authority: false, observed_legal_interleaving: false, disposition: 'PREVENTIVE CYCLE COUPLING SUCCESSOR; not a demonstrated current release defect; no new CorrectFinal paid-choice admission' }, null, 2) + '\n');
console.log(JSON.stringify({ whole_owners: records.length, unique_hunks: records.reduce((n, r) => n + r.hunks.length, 0), pins: pins.length, records: records.map(r => ({ file: r.file, sha256: r.after_sha256 })) }));
