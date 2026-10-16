import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mapExecutableSqlSource } from './cloudtms-db-release-lib.mjs';
import { exactAtomicBody, scanManagedSql, readManagedClosure } from './cloudtms-managed-release-sql.mjs';

// This prepares source only. The account-appropriate native MCP owns execution
// and rollback; the separate existing shared-turn helper owns the lock session.
const root = process.cwd();
const roundArgs = process.argv.slice(2).filter(value => value.startsWith('--round='));
assert(roundArgs.length <= 1 && roundArgs.every(value => /^--round=[1-3]$/.test(value)), 'invalid trial round');
const round = Number(roundArgs[0]?.slice('--round='.length) || '1');
const queryCompatibility = process.argv.includes('--query-compatibility');
const dispatch = process.argv.includes('--dispatch');
const globalTiming = process.argv.includes('--global-timing');
const contact = dispatch || globalTiming || queryCompatibility || process.argv.includes('--contact');
const calendar = contact || process.argv.includes('--calendar');
assert([globalTiming,dispatch,queryCompatibility].filter(Boolean).length<=1, 'distinct bounded trial scopes required');
assert(process.argv.slice(2).every(value => ['--calendar','--contact','--dispatch','--global-timing','--query-compatibility'].includes(value) || /^--round=[1-3]$/.test(value)), 'unknown trial option');
const output = path.resolve(root, queryCompatibility ? '.codex-tmp/query-contact-schema-native-20261016'
  : globalTiming ? '.codex-tmp/global-contact-timing-native-20261016'
  : dispatch ? '.codex-tmp/push-dispatch-consume-native-20261016'
  : contact ? '.codex-tmp/contact-pause-native-20261016'
  : calendar ? '.codex-tmp/qt-calendar-native-20261016' : '.codex-tmp/qt-admin-native-20261016') + (round > 1 ? `-r${round}` : '');
const sourceCommit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
const sources = [
  'supabase/migrations/15102026_0800_candidate_push_timing_authority.sql',
  'supabase/repeatable/15102026_0800_candidate_push_timing_authority_v1.sql',
  'tests/weekly-source/push-timing-admin-native.sql'
];
if (globalTiming) sources.unshift('supabase/migrations/13102026_1456_weekly_source_protection_query_support_v1.sql');
if (calendar) sources.push('supabase/repeatable/15102026_0801_candidate_push_calendar_v1.sql',
  'tests/weekly-source/push-timing-calendar-native.sql');
if (contact) sources.push('supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql',
  'supabase/repeatable/15102026_0802_weekly_contact_pause_v1.sql','tests/weekly-source/contact-pause-native.sql');
if (dispatch) sources.push('supabase/migrations/16102026_0135_weekly_contact_native_attempt_permit_unique.sql',
  'supabase/migrations/16102026_0142_weekly_contact_reuse_native_attempt_uniqueness.sql',
  'supabase/repeatable/15102026_0803_candidate_push_dispatch_read_v1.sql',
  'supabase/repeatable/16102026_0130_weekly_contact_submission_consume_v1.sql',
  'tests/weekly-source/push-dispatch-consume-native.sql');
if (globalTiming) sources.push('supabase/migrations/16102026_0244_weekly_contact_global_timing_schema.sql',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/06_automatic_actor_settings.sqlinc',
  'tests/weekly-source/global-contact-timing-native.sql');
if (queryCompatibility) sources.push(
  'supabase/migrations/16102026_0339_weekly_contact_query_lineage_and_claim_fence.sql',
  'supabase/repeatable/16102026_0339_weekly_contact_query_native_lifecycle_v1.sql',
  'supabase/repeatable/16102026_0339_weekly_contact_query_native_lifecycle_v1.sql',
  'tests/weekly-source/query-contact-schema-native.sql');
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
if (dispatch) functions.push('private.weekly_contact_push_dispatch_context_v1(uuid,uuid,text,bytea,uuid,text)',
  'private.weekly_contact_push_calendar_witness_v1(jsonb,jsonb,timestamp with time zone)',
  'public.candidate_push_timing_for_dispatch_v1(jsonb)', 'public.weekly_contact_submission_consume_v1(jsonb)',
  'private.weekly_contact_source_result_v1(uuid,text,text,jsonb,jsonb)');
if (globalTiming) functions.push('private.weekly_source_manager_interval_valid_v1(integer,text,boolean)',
  'private.weekly_source_manager_followups_valid_v1(jsonb)','private.weekly_source_global_settings_receipt_guard_v1()');
const contactTables = contact ? ['weekly_contact_pause_scopes','weekly_contact_pause_commands','weekly_contact_pause_events',
  'weekly_contact_submission_permits','weekly_contact_delivery_memberships','weekly_contact_recheck_tasks'] : [];
if (globalTiming) contactTables.push('weekly_source_global_settings_receipts');
const contactAbsence = contactTables.map(name => `to_regclass('public.${name}') is null`).join(' and ');
const dispatchAbsence = dispatch ? " and not exists(select 1 from public.weekly_source_groups where id='f7160000-0000-4000-8000-000000000300' or code='HR_QT_DISPATCH_ROLLBACK') and not exists(select 1 from public.weekly_source_cycles where id='f7160000-0000-4000-8000-000000000301') and not exists(select 1 from public.candidate_app_accounts where id='f7160000-0000-4000-8000-000000000305' or email_normalized='healthroster-qt-dispatch@example.invalid') and not exists(select 1 from public.candidate_app_global_membership_links where membership_id='f7160000-0000-4000-8000-000000000306') and not exists(select 1 from public.candidate_notifications where candidate_id='f7160000-0000-4000-8000-000000000010')" : '';
if (queryCompatibility) functions.push(...["private.weekly_contact_query_member_v1(uuid,uuid)","private.weekly_contact_query_saved_payload_v1(uuid,uuid)","private.weekly_contact_query_enqueue_core_v1(uuid,uuid,jsonb,uuid,integer,integer,uuid,bigint)","private.weekly_contact_query_recompose_v1(uuid,uuid,text,uuid,text)","public.timesheet_query_email_recompose_v1(jsonb)","public.timesheet_query_email_recompose_ready_v1(jsonb)","public.timesheet_query_email_contact_result_v1(jsonb)","private.weekly_contact_query_complete_v1(uuid,uuid,uuid)","private.weekly_contact_query_integrity_v1()","public._email_outbox_contact_claim_stamp_v1(uuid[],text,timestamptz,timestamptz,uuid,text)","public._email_outbox_contact_claim_cleanup_v1(text)","private.weekly_contact_query_claim_guard_v1()","public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text)","public._email_outbox_contact_claim_deployment_v1(uuid,text)","public.email_outbox_claim_ready_batch_contact_v1(jsonb)","private.weekly_contact_query_legacy_claims_v1(uuid,uuid,uuid,text)"]);
const names = functions.map(value => `'${value}'`).join(',');
const frame = `select jsonb_build_object(
 'database',current_database(),
 'tables_absent',to_regclass('public.candidate_push_timing_policy') is null and to_regclass('public.candidate_push_timing_commands') is null${contact ? ' and '+contactAbsence : ''},
 'functions_absent',not exists(select 1 from unnest(array[${names}]) n where to_regprocedure(n) is not null),
 'fixture_absent',not exists(select 1 from public.tms_users where id='f7160000-0000-4000-8000-000000000001' or email='healthroster-qt-rollback-only@example.invalid')${calendar ? " and not exists(select 1 from public.candidates where id='f7160000-0000-4000-8000-000000000010' or email='healthroster-qt-calendar@example.invalid' or key_norm='HR_QT_CALENDAR_ROLLBACK') and not exists(select 1 from public.clients where id='f7160000-0000-4000-8000-000000000011') and not exists(select 1 from public.client_settings where client_id='f7160000-0000-4000-8000-000000000011')" : ''},
 'dispatch_fixture_absent',true${dispatchAbsence},
 ${globalTiming ? `'global_settings_row',(select md5(to_jsonb(s)::text) from public.weekly_source_global_settings s where singleton),
 'global_relations',(select md5(string_agg(x,'|' order by x)) from (
  select c.relname||':'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull::text x
   from pg_class c join pg_namespace n on n.oid=c.relnamespace join pg_attribute a on a.attrelid=c.oid
   where n.nspname='public' and c.relname in('weekly_source_global_settings','weekly_manager_cohort_due_events') and a.attnum>0 and not a.attisdropped
  union all select c.relname||':'||k.conname||':'||pg_get_constraintdef(k.oid,true) from pg_class c join pg_namespace n on n.oid=c.relnamespace join pg_constraint k on k.conrelid=c.oid
   where n.nspname='public' and c.relname in('weekly_source_global_settings','weekly_manager_cohort_due_events'))f),
 'global_owners',(select md5(string_agg(pg_get_functiondef(p.oid),'|' order by p.proname)) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public' and p.proname in('weekly_source_global_settings_get_v1','weekly_source_global_settings_save_atomic_v1')),` : ''}
 ${queryCompatibility ? `'query_marker_absent',to_regclass('public.healthroster_query_schema_trial_16102026') is null,
 'query_relations',(select md5(string_agg(x,'|' order by x)) from(
  select 'column:'||c.relname||':'||a.attname||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull::text||':'||coalesce(pg_get_expr(d.adbin,d.adrelid),'') x
   from pg_class c join pg_namespace n on n.oid=c.relnamespace join pg_attribute a on a.attrelid=c.oid
   left join pg_attrdef d on d.adrelid=c.oid and d.adnum=a.attnum
   where n.nspname='public' and c.relname in('mail_outbox','hr_issue_email_deliveries','hr_issue_email_delivery_items','hr_issue_emails') and a.attnum>0 and not a.attisdropped
  union all select 'constraint:'||c.relname||':'||k.conname||':'||pg_get_constraintdef(k.oid,true) from pg_class c join pg_namespace n on n.oid=c.relnamespace join pg_constraint k on k.conrelid=c.oid
   where n.nspname='public' and c.relname in('mail_outbox','hr_issue_email_deliveries','hr_issue_email_delivery_items','hr_issue_emails')
  union all select 'index:'||tablename||':'||indexname||':'||indexdef from pg_indexes where schemaname='public' and tablename in('mail_outbox','hr_issue_email_deliveries','hr_issue_email_delivery_items','hr_issue_emails')
  union all select 'trigger:'||c.relname||':'||t.tgname||':'||pg_get_triggerdef(t.oid,true) from pg_trigger t join pg_class c on c.oid=t.tgrelid join pg_namespace n on n.oid=c.relnamespace
   where n.nspname='public' and c.relname in('mail_outbox','hr_issue_email_deliveries','hr_issue_email_delivery_items','hr_issue_emails') and not t.tgisinternal
  union all select 'acl:'||c.relname||':'||c.relrowsecurity::text||':'||c.relforcerowsecurity::text||':'||c.relowner::regrole::text||':'||coalesce(c.relacl::text,'') from pg_class c join pg_namespace n on n.oid=c.relnamespace
   where n.nspname='public' and c.relname in('mail_outbox','hr_issue_email_deliveries','hr_issue_email_delivery_items','hr_issue_emails')
  union all select 'policy:'||tablename||':'||policyname||':'||coalesce(qual,'')||':'||coalesce(with_check,'')||':'||roles::text from pg_policies where schemaname='public' and tablename in('mail_outbox','hr_issue_email_deliveries','hr_issue_email_delivery_items','hr_issue_emails'))q),
 'query_owners',(select md5(string_agg(pg_get_functiondef(p.oid)||':'||p.proowner::regrole::text||':'||coalesce(p.proacl::text,''),'|' order by p.proname,p.oid)) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public' and p.proname in('timesheet_query_email_enqueue_v1','_timesheet_query_email_delivery_mark_core_v1','email_outbox_claim_ready_batch')),
` : ''}
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
    if (/^\s*\\ir\s/m.test(saved.toString('utf8'))) {
      const closure=readManagedClosure(file,root,[],(text,included)=>{
        const bytes=fs.readFileSync(included),publishedInclude=execFileSync('git',['cat-file','blob',sourceCommit+':'+included]);
        assert.deepEqual(bytes,publishedInclude,included+': publish exact include before trial');
        if(included!==file)pins.push({path:included,sha256:sha(bytes),bytes:bytes.length,included_by:file});
        return mapExecutableSqlSource(text,included);
      });
      return exactAtomicBody(closure.expanded);
    }
    return mapExecutableSqlSource(exactAtomicBody(saved.toString('utf8')), file);
  });
} finally {
  if (previous === undefined) delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = previous;
}
const sql = 'set local jit=off;\nset local pg_show_plans.is_enabled=off;\n' + absenceGuard + fragments.join('\n');
const manifest = {
  source_commit: sourceCommit, round, sources: pins, sql_bytes: Buffer.byteLength(sql), sql_sha256: sha(sql),
  statements: scanManagedSql(sql).statements.length, fingerprint_sha256: sha(frame),
  account: 'kier@arthur-rai.co.uk', native_server: 'cloudtms-miget-operations-kier-codex', database: 'agency_test',
  scope: queryCompatibility
    ? 'Atomic existing query schema/claim-fence constructor, repeatable applied twice and actual pause GET through factual legacy reader only. No enqueue, claim, provider, result, recomposition, full query or concurrency acceptance.'
    : globalTiming
    ? 'Actual global singleton GET/save CAS/immutable receipt/137-11 preservation/large string revisions/admin replay loss/RLS and ACL, after complete QT/calendar/pause control prerequisites. Native manager generation/scheduler/follow-up/common START/Office API/UI/races/full release NOT_RUN.'
    : dispatch
    ? 'QT/pause prerequisites plus saved-target advisory and already-admitted synthetic native-attempt consume/permit-result constructor only; original START/claim/native result/membership-budget/race/producer/API/full-release acceptance NOT_RUN'
    : contact
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
