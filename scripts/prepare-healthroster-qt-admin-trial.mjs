import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mapExecutableSqlSource } from './cloudtms-db-release-lib.mjs';
import { exactAtomicBody, scanManagedSql } from './cloudtms-managed-release-sql.mjs';

// This prepares source only. The account-appropriate native MCP owns execution
// and rollback; the separate existing shared-turn helper owns the lock session.
const root = process.cwd();
const contact = process.argv.includes('--contact');
const calendar = contact || process.argv.includes('--calendar');
assert(process.argv.slice(2).every(value => ['--calendar','--contact'].includes(value)), 'unknown trial option');
const output = path.resolve(root, contact ? '.codex-tmp/contact-pause-native-20261016'
  : calendar ? '.codex-tmp/qt-calendar-native-20261016' : '.codex-tmp/qt-admin-native-20261016');
const sourceCommit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
const sources = [
  'supabase/migrations/15102026_0800_candidate_push_timing_authority.sql',
  'supabase/repeatable/15102026_0800_candidate_push_timing_authority_v1.sql',
  'tests/weekly-source/push-timing-admin-native.sql'
];
if (calendar) sources.push('supabase/repeatable/15102026_0801_candidate_push_calendar_v1.sql',
  'tests/weekly-source/push-timing-calendar-native.sql');
if (contact) sources.push('supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql',
  'supabase/repeatable/15102026_0802_weekly_contact_pause_v1.sql','tests/weekly-source/contact-pause-native.sql');
const sha = value => createHash('sha256').update(value).digest('hex');
const functions = [
  'private.weekly_contact_assert_closed_v1(jsonb,text[],text[],text)',
  'private.weekly_contact_revision_v1(jsonb,boolean)',
  'private.candidate_push_timing_policy_validate_v1(jsonb)',
  'private.weekly_contact_deployment_lock_v1(uuid,text)',
  'private.candidate_push_timing_immutable_guard_v1()',
  'public.candidate_push_timing_get_v1(jsonb)',
  'public.candidate_push_timing_save_v1(jsonb)'
];
if (calendar) functions.push(
  'private.weekly_contact_london_boundary_v1(date,time without time zone,boolean)',
  'private.weekly_contact_push_allowed_windows_v1(jsonb,date,boolean)',
  'private.weekly_contact_push_timing_decision_v1(jsonb,timestamptz,timestamptz,jsonb)',
  'public.weekly_source_push_calendar_decision_v1(jsonb)');
if (contact) functions.push('private.weekly_contact_uuid_v1(jsonb)',
  'private.weekly_contact_context_v1(uuid,text)','private.weekly_contact_next_monday_v1(timestamptz)',
  'private.weekly_contact_scope_lock_v1(uuid,text,uuid,uuid)','private.weekly_contact_scope_guard_v1(uuid,uuid,text,timestamptz)',
  'private.weekly_contact_pause_immutable_guard_v1()','private.weekly_contact_pause_inflight_v1(uuid,text,uuid,uuid)',
  'private.weekly_contact_pause_shape_v1(uuid,text,uuid,uuid,timestamptz)','private.weekly_contact_pause_command_v1(jsonb,text)',
  'public.weekly_source_contact_pause_get_v1(jsonb)','public.weekly_source_contact_pause_atomic_v1(jsonb)',
  'public.weekly_source_contact_resume_atomic_v1(jsonb)');
const contactTables = contact ? ['weekly_contact_pause_scopes','weekly_contact_pause_commands','weekly_contact_pause_events',
  'weekly_contact_submission_permits','weekly_contact_delivery_memberships','weekly_contact_recheck_tasks'] : [];
const contactAbsence = contactTables.map(name => `to_regclass('public.${name}') is null`).join(' and ');
const names = functions.map(value => `'${value}'`).join(',');
const frame = `select jsonb_build_object(
 'database',current_database(),
 'tables_absent',to_regclass('public.candidate_push_timing_policy') is null and to_regclass('public.candidate_push_timing_commands') is null${contact ? ' and '+contactAbsence : ''},
 'functions_absent',not exists(select 1 from unnest(array[${names}]) n where to_regprocedure(n) is not null),
 'fixture_absent',not exists(select 1 from public.tms_users where id='f7160000-0000-4000-8000-000000000001' or email='healthroster-qt-rollback-only@example.invalid')${calendar ? " and not exists(select 1 from public.candidates where id='f7160000-0000-4000-8000-000000000010' or email='healthroster-qt-calendar@example.invalid' or key_norm='HR_QT_CALENDAR_ROLLBACK') and not exists(select 1 from public.clients where id='f7160000-0000-4000-8000-000000000011') and not exists(select 1 from public.client_settings where client_id='f7160000-0000-4000-8000-000000000011')" : ''},
 'legacy_one',exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1),
 'migration_ledger',(select md5(coalesce(string_agg(to_jsonb(t)::text,'|' order by to_jsonb(t)::text),'')) from private.cloudtms_migration_ledger t),
 'repeatable_ledger',(select md5(coalesce(string_agg(to_jsonb(t)::text,'|' order by to_jsonb(t)::text),'')) from private.cloudtms_repeatable_ledger t));\n`;
const absenceGuard = `do $guard$ begin
 if current_database()<>'cloudtms_test_clone'
   or to_regclass('public.candidate_push_timing_policy') is not null
   or to_regclass('public.candidate_push_timing_commands') is not null
   ${contact ? 'or not('+contactAbsence+')' : ''}
   or exists(select 1 from unnest(array[${names}]) n where to_regprocedure(n) is not null)
 then raise exception 'QT_TRIAL_BASELINE_NOT_ABSENT'; end if;
end; $guard$;\n`;
const previous = process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
let fragments;
const pins = [];
try {
  process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = 'CURRENT_USER';
  fragments = sources.map(file => {
    const saved = fs.readFileSync(file);
    const published = execFileSync('git', ['cat-file', 'blob', `${sourceCommit}:${file}`]);
    assert.deepEqual(saved, published, `${file}: publish exact source before trial`);
    pins.push({ path: file, sha256: sha(saved), bytes: saved.length });
    return mapExecutableSqlSource(exactAtomicBody(saved.toString('utf8')), file);
  });
} finally {
  if (previous === undefined) delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = previous;
}
const sql = 'set local jit=off;\nset local pg_show_plans.is_enabled=off;\n' + absenceGuard + fragments.join('\n');
const manifest = {
  source_commit: sourceCommit, sources: pins, sql_bytes: Buffer.byteLength(sql), sql_sha256: sha(sql),
  statements: scanManagedSql(sql).statements.length, fingerprint_sha256: sha(frame),
  account: 'kier@arthur-rai.co.uk', native_server: 'cloudtms-miget-operations-kier-codex', database: 'agency_test',
  scope: contact
    ? 'QT/calendar plus actual pause GET/PAUSE/RESUME control and empty permit stores only; no dispatch/START/consume/send, race, configured bootstrap or full-release acceptance'
    : calendar
    ? 'QT administration, pure London timing/DST and existing-calendar adapter only; no configured bootstrap, dispatch/provider, START/race or full-release acceptance'
    : 'QT administration native constructor/fixture only; no configured bootstrap, dispatch, provider, Worker, UI, race or full-release acceptance'
};
assert(manifest.sql_bytes <= 8_000_000 && manifest.statements <= 1_000);
fs.mkdirSync(output, { recursive: true });
for (const [name, value] of Object.entries({ 'TRIAL.sql': sql, 'FINGERPRINT.sql': frame, 'MANIFEST.json': JSON.stringify(manifest, null, 2) + '\n' })) {
  const target = path.join(output, name);
  if (fs.existsSync(target)) assert.equal(fs.readFileSync(target, 'utf8'), value, 'retain previous prepared trial');
  else fs.writeFileSync(target, value);
}
console.log(JSON.stringify(manifest));
