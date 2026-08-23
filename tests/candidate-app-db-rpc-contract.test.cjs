const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const files = {
  foundation: 'supabase/migrations/07082026_2055_candidate_app_foundation_schema.sql',
  evidence: 'supabase/migrations/07082026_2055_candidate_app_timesheet_evidence_integrity.sql',
  managerReviewSchema: 'supabase/migrations/07082026_2306_candidate_app_manager_review_documents.sql',
  replacementLineage: 'supabase/migrations/11082026_1708_candidate_workflow_replacement_lineage.sql',
  creationIdentity: 'supabase/migrations/11082026_2112_candidate_workflow_creation_identity.sql',
  helpers: 'supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql',
  managerReviewHelpers: 'supabase/repeatable/07082026_2310_candidate_manager_review_helpers_v1.sql',
  auth: 'supabase/repeatable/07082026_2103_candidate_auth_rpcs_v1.sql',
  reads: 'supabase/repeatable/07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql',
  expenses: 'supabase/repeatable/07082026_2113_candidate_expense_placement_rpcs_v1.sql',
  workflow: 'supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql',
  final: 'supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql',
  routeAuthority: 'supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql'
};

const sql = Object.fromEntries(Object.entries(files).map(([key, file]) => [key, read(file)]));
const all = Object.values(sql).join('\n');
const codeOnly = all.replace(/^\s*--.*$/gm, '');

const generatedOffice = read('supabase/repeatable/07082026_2224_candidate_app_weekly_office_replacements_v1.sql');
const generatedOther = read('supabase/repeatable/07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql');
const replacements = {
  draft: generatedOffice,
  process: generatedOffice,
  contexts: generatedOffice,
  lifecycle: generatedOffice,
  qr: generatedOther,
  clientCreate: generatedOther,
  clientUpdate: generatedOther,
  invoiceGroups: generatedOther,
  invoiceDelivery: generatedOther,
  invoiceIssue: generatedOther,
  invoiceAdvance: generatedOther,
  generatedOffice,
  generatedOther,
  routeAuthority: sql.routeAuthority
};

const publicRpcNames = [...all.matchAll(
  /^create or replace function public\.(candidate_[a-z0-9_]+_v1|expense_[a-z0-9_]+_v1|timesheet_expense_apply_atomic_v1)\s*\(/gmi
)].map((match) => match[1]);

const expectedRpcs = [
  'candidate_auth_account_transition_v1',
  'candidate_auth_challenge_transition_v1',
  'candidate_app_bootstrap_v1',
  'candidate_app_timesheet_page_v1',
  'candidate_app_timesheet_detail_v1',
  'candidate_missing_week_options_v1',
  'candidate_contract_week_add_missing_atomic_v1',
  'expense_placement_resolve_v1',
  'expense_carrier_resolve_or_create_atomic_v1',
  'timesheet_expense_apply_atomic_v1',
  'candidate_workflow_transition_atomic_v1',
  'candidate_submission_finalize_atomic_v1',
  'candidate_submission_reject_atomic_v1',
  'candidate_no_work_atomic_v1'
];

function definition(source, name) {
  const marker = `create or replace function public.${name}`;
  const start = source.toLowerCase().indexOf(marker);
  assert.notEqual(start, -1, `${name} definition must exist`);
  const end = source.toLowerCase().indexOf('\ncreate or replace function ', start + marker.length);
  return source.slice(start, end === -1 ? source.length : end);
}

function privateDefinition(source, name) {
  const marker = `create or replace function private.${name}`;
  const start = source.toLowerCase().indexOf(marker);
  assert.notEqual(start, -1, `${name} definition must exist`);
  const end = source.toLowerCase().indexOf('\ncreate or replace function ', start + marker.length);
  return source.slice(start, end === -1 ? source.length : end);
}

test('the local DB milestone contains exactly the fourteen approved public RPCs', () => {
  assert.deepEqual([...new Set(publicRpcNames)].sort(), [...expectedRpcs].sort());
  assert.equal(publicRpcNames.length, expectedRpcs.length, 'no public Candidate App RPC may be defined twice');
});

test('the installable Candidate App SQL contains only one latest definition of every function', () => {
  const installableSql = `${all}\n${replacements.generatedOffice}\n${replacements.generatedOther}`;
  const names = [...installableSql.matchAll(/^create(?: or replace)? function\s+((?:public|private)\.[a-z0-9_]+)\s*\(/gmi)]
    .map((match) => match[1].toLowerCase());
  assert.equal(names.length, 106, 'the latest-only payload must contain the closed 106-definition function inventory');
  const duplicates = [...new Set(names.filter((name, index) => names.indexOf(name) !== index))].sort();
  assert.deepEqual(duplicates, []);
});

test('foundation creates the seven private Candidate App tables with environment isolation', () => {
  const expectedTables = [
    'candidate_app_accounts',
    'candidate_app_sessions',
    'candidate_auth_challenges',
    'candidate_submission_workflows',
    'candidate_submission_components',
    'candidate_approval_requests',
    'candidate_notifications'
  ];
  for (const table of expectedTables) {
    assert.match(sql.foundation, new RegExp(`create table(?: if not exists)? public\\.${table}\\b`, 'i'), `${table} must be created`);
    assert.match(sql.foundation, new RegExp(`alter table public\\.${table} enable row level security`, 'i'));
    assert.match(sql.foundation, new RegExp(`alter table public\\.${table} force row level security`, 'i'));
  }
  assert.match(sql.foundation, /candidate_app_environment text not null default 'TEST'/i);
  assert.match(sql.foundation, /check \(candidate_app_environment in \('TEST','LIVE'\)\)/i);
  assert.match(sql.foundation, /unique \(environment,email_normalized\)/i);
  assert.match(sql.foundation, /candidate_app_system_actor_user_id uuid/i);
  assert.match(sql.foundation, /foreign key \(candidate_app_system_actor_user_id\)[\s\S]*references public\.tms_users\(id\)/i);
});

test('all cutover defaults are dormant and candidate auto-authorisation defaults false', () => {
  assert.match(sql.foundation, /candidate_electronic_auto_authorise_default boolean not null default false/i);
  const flags = [
    'candidate_account_registration', 'candidate_app_reads', 'candidate_app_writes',
    'candidate_record_role_capabilities', 'candidate_expense_atomic_placement',
    'candidate_expense_invoice_routing_v1', 'candidate_manager_approval',
    'candidate_paper_qr', 'candidate_notifications', 'candidate_daily_finalisation'
  ];
  for (const flag of flags) assert.match(sql.foundation, new RegExp(`"${flag}":false`, 'i'));
  assert.match(sql.helpers, /candidate_electronic_auto_authorise_override[\s\S]*candidate_electronic_auto_authorise[\s\S]*candidate_electronic_auto_authorise_default/i);
});

test('candidate tables and RPCs are unavailable to browser roles and executable only by service_role', () => {
  for (const table of [
    'candidate_app_accounts', 'candidate_app_sessions', 'candidate_auth_challenges',
    'candidate_submission_workflows', 'candidate_submission_components',
    'candidate_approval_requests', 'candidate_notifications'
  ]) {
    assert.match(sql.foundation, new RegExp(`revoke all on table public\\.${table} from public,anon,authenticated`, 'i'));
    assert.match(sql.foundation, new RegExp(`grant select,insert,update,delete on table public\\.${table} to service_role`, 'i'));
  }
  for (const name of expectedRpcs) {
    const grants = new RegExp(`grant execute on function public\\.${name}\\([\\s\\S]*?\\) to service_role`, 'i');
    const revokes = new RegExp(`revoke all on function public\\.${name}\\([\\s\\S]*?\\) from public,anon,authenticated`, 'i');
    assert.match(all, revokes, `${name} must revoke public/anon/authenticated`);
    assert.match(all, grants, `${name} must grant service_role`);
  }
});

test('new authentication is clean registration only and stores versioned verifiers plus token hashes', () => {
  assert.match(sql.foundation, /password_scheme text/);
  assert.match(sql.foundation, /password_scheme_version smallint/);
  assert.match(sql.foundation, /password_digest bytea/);
  assert.match(sql.foundation, /password_salt bytea/);
  assert.match(sql.foundation, /octet_length\(refresh_token_hash\)\s*=\s*32/i);
  assert.match(sql.foundation, /octet_length\(token_hash\)\s*=\s*32/i);
  assert.match(sql.auth, /CANDIDATE_AUTH_PLAINTEXT_SECRET_FORBIDDEN/);
  assert.match(sql.auth, /length\(v_payload->>'password_digest_hex'\) not between 64 and 256/i);
  assert.doesNotMatch(sql.auth, /\{64,256\}/);
  assert.match(sql.auth, /ACTIVATE_PASSWORD/);
  assert.match(sql.auth, /v_challenge\.purpose not in \('ACTIVATE','RESET','RECOVERY'\)/i);
  assert.match(sql.auth, /CANDIDATE_PASSWORD_RESET/);
  assert.match(sql.auth, /CANDIDATE_IDEMPOTENCY_KEY_REQUIRED/);
  assert.match(sql.auth, /deterministic_outbox_key=btrim\(p_idempotency_key\)/i);
  assert.match(sql.auth, /attempt_count=least\(attempt_count\+1,5\)[\s\S]*v_response:=jsonb_build_object\([\s\S]*CANDIDATE_CHALLENGE_INVALID[\s\S]*_candidate_auth_mutation_receipt_v1\([\s\S]*return v_response/i);
  assert.doesNotMatch(sql.auth, /attempt_count=least\(attempt_count\+1,5\)[\s\S]{0,500}raise exception 'CANDIDATE_CHALLENGE_INVALID'/i);
  assert.doesNotMatch(codeOnly, /GOOGLE_LEGACY|legacy_password|_deriveKeyHmacSha256|passwordhash|passwordsalt/i);
});

test('refresh-token-reuse revocation and its negative response share one durable receipt', () => {
  const auth = definition(sql.auth, 'candidate_auth_account_transition_v1');
  assert.match(
    auth,
    /v_session\.status='ROTATED'[\s\S]*v_response:=jsonb_build_object\([\s\S]*CANDIDATE_REFRESH_TOKEN_REUSE[\s\S]*revoke_reason='REFRESH_TOKEN_REUSE'[\s\S]*_candidate_auth_mutation_receipt_v1\([\s\S]*return v_response/i
  );
});

test('account session creation rotation and invalidation share one transaction lock authority', () => {
  const auth = definition(sql.auth, 'candidate_auth_account_transition_v1');
  const lock = privateDefinition(sql.auth, '_candidate_auth_account_session_lock_v1');
  assert.match(lock, /pg_catalog\.pg_advisory_xact_lock\(pg_catalog\.hashtextextended\(/i);
  assert.match(lock, /CANDIDATE_AUTH_ACCOUNT_SESSION_V1/i);
  assert.match(sql.auth, /revoke all on function private\._candidate_auth_account_session_lock_v1\(text,uuid\) from public,anon,authenticated,service_role/i);
  assert.match(auth, /REFRESH_SESSION[\s\S]*_candidate_auth_account_session_lock_v1\([\s\S]*candidate_app_accounts[\s\S]*for update[\s\S]*candidate_app_sessions[\s\S]*for update/i);
  assert.match(auth, /REFRESH_TOKEN_REUSE[\s\S]*status in \('ACTIVE','ROTATED'\)[\s\S]*CANDIDATE_SESSION_INVALIDATION_INCOMPLETE/i);
  assert.match(auth, /CHANGE_PASSWORD[\s\S]*_candidate_auth_account_session_lock_v1\([\s\S]*PASSWORD_CHANGED[\s\S]*CANDIDATE_SESSION_INVALIDATION_INCOMPLETE/i);
  assert.match(auth, /REVOKE_SESSIONS[\s\S]*_candidate_auth_account_session_lock_v1\([\s\S]*LOCK','DISABLE[\s\S]*CANDIDATE_SESSION_INVALIDATION_INCOMPLETE/i);
});

test('login and password change revalidate the exact non-plaintext password authority under the account lock', () => {
  const auth = definition(sql.auth, 'candidate_auth_account_transition_v1');
  const fingerprint = privateDefinition(sql.auth, '_candidate_password_authority_sha256_v1');
  assert.match(fingerprint, /CANDIDATE_PASSWORD_AUTHORITY_V1/);
  assert.match(fingerprint, /extensions\.digest\(/i);
  assert.doesNotMatch(fingerprint, /plaintext_password/i);
  assert.match(sql.auth, /revoke all on function private\._candidate_password_authority_sha256_v1\([\s\S]*service_role/i);
  assert.match(auth, /_candidate_auth_account_session_lock_v1\([\s\S]*for update[\s\S]*expected_password_authority_sha256[\s\S]*presented_password_digest_hex/i);
  assert.match(auth, /v_password_authority_current[\s\S]*v_password_matches_current[\s\S]*failed_login_recorded/i);
  assert.match(auth, /CHANGE_PASSWORD[\s\S]*_candidate_password_authority_sha256_v1\([\s\S]*not v_password_matches_current[\s\S]*password_changed_at_utc/i);
  assert.doesNotMatch(auth, /if v_action='LOGIN_FAILURE'/i);
  assert.doesNotMatch(auth, /v_payload\s*\?\s*'password'/i);
});

test('exact-byte evidence has one digest authority and one active TIMESHEET constraint', () => {
  assert.match(sql.foundation, /source_content_sha256 bytea/);
  assert.match(sql.foundation, /octet_length\(source_content_sha256\)\s*=\s*32/i);
  assert.match(sql.foundation, /create unique index(?: if not exists)? candidate_submission_components_source_sha256_uq[\s\S]*source_content_sha256/i);
  assert.doesNotMatch(sql.evidence, /source_content_sha256/i, 'timesheet_evidence must not duplicate the component digest');
  assert.match(sql.evidence, /candidate_component_id uuid/i);
  assert.match(sql.evidence, /timesheet_evidence_one_active_timesheet_uq/i);
  assert.match(sql.evidence, /where upper\(btrim\(kind\)\)\s*=\s*'TIMESHEET'[\s\S]*processing_state\s*<>\s*'SUPERSEDED'/i);
  assert.match(sql.managerReviewHelpers, /CANDIDATE_COMPONENT_IMMUTABLE/);
  assert.match(sql.workflow, /get stacked diagnostics v_constraint_name=constraint_name/i);
});

test('candidate reads do not multiply economics by workflow count and hide expense carriers', () => {
  const bootstrap = definition(sql.reads, 'candidate_app_bootstrap_v1');
  const page = definition(sql.reads, 'candidate_app_timesheet_page_v1');
  assert.match(bootstrap, /nullif\(btrim\(coalesce\(v_candidate\.key_norm,''\)\),''\) is not null/i);
  assert.doesNotMatch(bootstrap, /key_norm\)\)\s*=\s*'GCK'|key_norm\s*=\s*'GCK'/i);
  assert.match(bootstrap, /week_ending_weekday_snapshot[\s\S]*current_week_ending_date[\s\S]*interval '6 months'/i);
  assert.match(page, /expense_carriers as materialized/i);
  assert.match(page, /expense_carrier_resolution as materialized/i);
  assert.match(page, /current_version_resolution as materialized/i);
  assert.match(page, /current_row\.booking_id[\s\S]*history\.booking_id/i);
  assert.match(page, /expense_anchor_totals as materialized/i);
  assert.match(page, /workflow_overlay as materialized/i);
  assert.match(page, /AMBIGUOUS_WORKFLOW_ANCHOR|INVALID_PARENT_ANCHOR/i);
  assert.match(page, /EXPENSE_DISPLAY_ANCHOR_NOT_FOUND/i);
  assert.match(page, /EXPENSE_DISPLAY_ANCHOR_AMBIGUOUS/i);
  assert.match(page, /expense_overlay_conflict_code/i);
  assert.doesNotMatch(page, /from candidate_weeks base\s+left join public\.candidate_submission_workflows[\s\S]*sum\(/i);
  assert.match(page, /where not exists\(select 1 from expense_carriers carrier where carrier\.id=base\.id\)/i);
  assert.match(page, /v2\|[\s\S]*v_view[\s\S]*v_snapshot_utc[\s\S]*week_ending_date[\s\S]*contract_id[\s\S]*additional_seq[\s\S]*id/i);
  assert.doesNotMatch(page, /candidate_name|candidate_email|storage_key|pay_rate|charge_rate|margin_ex_vat/i);
});

test('authority correction binds workflow identity, manager signatures, cursor rank and authorised locks', () => {
  const page = definition(sql.reads, 'candidate_app_timesheet_page_v1');
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  const finalise = definition(sql.final, 'candidate_submission_finalize_atomic_v1');
  assert.match(sql.foundation, /candidate_submission_workflows_kind_scope_route_ck/i);
  assert.match(sql.foundation, /workflow_kind='DAILY'[\s\S]*scope='DAILY'[\s\S]*route in \('PHONE','EMAIL'\)/i);
  assert.match(workflow, /v_scope<>'WEEKLY' or v_route not in \('ELECTRONIC','PAPER'\)/i);
  assert.match(workflow, /CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED/);
  assert.match(workflow, /CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH/);
  assert.match(workflow, /for update[\s\S]*CANDIDATE_WORKFLOW_WEEK_MISMATCH/i);
  assert.match(finalise, /CANDIDATE_CONTRACT_WEEK_IDENTITY_MISMATCH/);
  assert.match(finalise, /CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH/);
  assert.match(sql.managerReviewSchema, /approval_request_id uuid/i);
  assert.match(sql.managerReviewSchema, /candidate_submission_components_approval_request_fk/i);
  assert.match(workflow, /approval_request_id=v_approval\.id/i);
  assert.match(finalise, /approval_request_id=v_approved_request\.id/i);
  assert.match(sql.helpers, /v_candidate_mutation_locked:=v_fin\.authorised_at_utc is not null/i);
  assert.match(sql.helpers, /'candidate_mutation_locked',v_candidate_mutation_locked/i);
  assert.match(workflow, /CANDIDATE_RECORD_MUTATION_LOCKED/i);
  assert.match(finalise, /CANDIDATE_RECORD_MUTATION_LOCKED/i);
  assert.match(page, /cursor_version','v2'/i);
  assert.match(page, /week_ending_date desc[\s\S]*contract_id desc[\s\S]*additional_seq desc[\s\S]*id desc/i);
  assert.match(workflow, /_candidate_submission_issue_codes_v1/i);
  assert.doesNotMatch(workflow, /coalesce\(v_payload->'issue_codes'/i);
  assert.match(sql.helpers, /BARRED_DOMAIN_POLICY_NOT_CONFIGURED/);
  assert.match(generatedOther, /CANDIDATE_BARRED_MANAGER_DOMAIN_POLICY_REQUIRED/i);
});

test('missing weeks preserve override-aware submission mode and build dated schedules', () => {
  assert.match(sql.helpers, /case when coalesce\(v_contract\.overrideclientsettings,false\)/i);
  assert.match(sql.helpers, /_candidate_week_schedule_from_template_v1/i);
  assert.match(sql.reads, /_candidate_submission_mode_v1\(v_contract\.client_id,v_contract\.id,p_week_ending_date\)/i);
  assert.match(sql.reads, /_candidate_week_schedule_from_template_v1\([\s\S]*v_contract\.std_schedule_json/i);
  assert.doesNotMatch(sql.reads, /coalesce\(v_contract\.std_schedule_json,'\[\]'::jsonb\),p_now_utc/i);
});

test('expense application is one canonical transaction with locked carrier placement', () => {
  const carrier = definition(sql.expenses, 'expense_carrier_resolve_or_create_atomic_v1');
  const apply = definition(sql.expenses, 'timesheet_expense_apply_atomic_v1');
  assert.match(carrier, /pg_advisory_xact_lock/);
  assert.match(carrier, /order by cw\.additional_seq,cw\.id for update/i);
  assert.match(sql.expenses, /EXPENSE_CARRIER_AMBIGUOUS/);
  assert.match(apply, /contract_week_manual_upsert_atomic/);
  assert.equal((apply.match(/contract_week_manual_upsert_atomic/g) || []).length, 1);
  assert.match(apply, /p_actor_user_id=>v_system_actor/i);
  assert.match(apply, /EXPENSE_EVIDENCE_REQUIRED/);
  assert.match(apply, /HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS/);
  assert.doesNotMatch(apply, /insert into public\.timesheets_financials/i);
  assert.match(carrier, /v_anchor_week\.enforce_day_partition,v_anchor_week\.allowed_days_mask/i);
  assert.match(carrier, /v_anchor_week\.split_boundary_date,v_anchor_week\.split_group_key/i);
  assert.match(carrier, /v_next_seq,'OPEN','MANUAL'/i);
});

test('workflow enforces one-signature approval, manager identity, reminder limits and DAILY paper exclusion', () => {
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  assert.match(workflow, /RECORD_REVIEW_PROGRESS/);
  assert.match(workflow, /MANAGER_REVIEW_COMPONENT_NOT_REVIEWED/);
  assert.match(workflow, /manager_name/);
  assert.match(workflow, /manager_position/);
  assert.match(workflow, /MANAGER_SIGNATURE_REQUIRED/);
  assert.match(workflow, /resend_count>=5/);
  assert.match(workflow, /interval '24 hours'/);
  assert.match(workflow, /interval '7 days'/);
  assert.match(workflow, /v_workflow\.scope='DAILY'[\s\S]*CANDIDATE_PAPER_ROUTE_NOT_ALLOWED/i);
  assert.match(workflow, /CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED/);
  assert.match(workflow, /CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT/);
  assert.match(workflow, /source_component_id is null[\s\S]*created_at_utc>=coalesce\(v_workflow\.worker_submitted_at_utc/i);
});

test('manager approval and evidence completion are session-independent, token-bound and byte-exact', () => {
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  const finalise = definition(sql.final, 'candidate_submission_finalize_atomic_v1');
  assert.match(workflow, /v_is_public_manager_action:=not v_is_service_action[\s\S]*PHONE_APPROVE[\s\S]*COMPONENT_COMPLETE/i);
  assert.match(workflow, /method in \('EMAIL','PHONE'\)[\s\S]*expires_at_utc>p_now_utc/i);
  assert.match(workflow, /'PHONE','PENDING',v_token_hash[\s\S]*expires_at_utc/i);
  assert.match(workflow, /v_action='SELECT_PHONE_APPROVAL'[\s\S]*'approval_token_hash_hex',encode\(v_approval\.token_hash,'hex'\)[\s\S]*'handoff_token_key_version'/i);
  assert.match(workflow, /CANCEL_MANAGER_HANDOFF[\s\S]*handoff_cancelled/i);
  assert.match(workflow, /EXPENSE_EVIDENCE'\)[\s\S]*application\/pdf/i);
  assert.match(workflow, /v_component\.source_content_sha256=v_digest[\s\S]*CANDIDATE_COMPONENT_IMMUTABLE_CONFLICT/i);
  assert.match(finalise, /CANDIDATE_MANAGER_FINALISATION_V1/i);
  assert.match(finalise, /p_session_id is null[\s\S]*approval_request_id[\s\S]*state='APPROVED'/i);
  assert.match(finalise, /v_candidate_id:=v_workflow\.candidate_id/i);
});

test('manager reminder renewal cancellation and detail actions use one provider-owned contract', () => {
  const workflow = sql.workflow;
  const reads = sql.reads;
  assert.match(workflow, /v_action='REMIND'[\s\S]*approval_token_hash_hex[\s\S]*manager_mail\.status='SENT'[\s\S]*provider_status[\s\S]*interval '24 hours'[\s\S]*token_hash=v_token_hash[\s\S]*candidate_manager_mail_kind','REMINDER'/i);
  assert.match(workflow, /v_action='RENEW'[\s\S]*v_approval\.state='PENDING'[\s\S]*expires_at_utc<=p_now_utc[\s\S]*state='EXPIRED'[\s\S]*v_approval\.state<>'EXPIRED'[\s\S]*candidate_manager_mail_kind','RENEWAL'/i);
  assert.match(workflow, /v_action='MANAGER_REQUEST_CANCEL'[\s\S]*CANDIDATE_CANCELLATION_REASON_REQUIRED[\s\S]*_candidate_manager_mail_retire_v1[\s\S]*candidate_manager_mail_kind','WITHDRAWAL'[\s\S]*cancellation_reason[\s\S]*manager_withdrawal_count/i);
  assert.match(workflow, /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*CANDIDATE_CANCELLATION_REASON_REQUIRED[\s\S]*_candidate_manager_mail_retire_v1[\s\S]*candidate_manager_mail_kind','CANCELLATION'[\s\S]*cancellation_reason[\s\S]*manager_withdrawal_count/i);
  assert.match(reads, /_candidate_timesheet_action_contract_v1[\s\S]*provider_accepted_at_utc[\s\S]*SEND_MANAGER_REMINDER[\s\S]*REQUEST_APPROVAL_AGAIN[\s\S]*DOWNLOAD_PAPER_DOCUMENTS[\s\S]*RETRY_FINALISATION/i);
  assert.doesNotMatch(reads, /'code','CONTINUE_WORKFLOW'/i);
});

test('manager-review addendum uses the existing three workflow tables and adds no eighth table', () => {
  assert.doesNotMatch(sql.managerReviewSchema, /create table/i);
  for (const table of [
    'candidate_submission_workflows','candidate_submission_components','candidate_approval_requests'
  ]) assert.match(sql.managerReviewSchema, new RegExp(`alter table public\\.${table}`, 'i'));
  assert.match(sql.managerReviewSchema, /alter table public\.timesheets/i);
  assert.match(sql.managerReviewSchema, /immutable_submission_json jsonb/i);
  assert.match(sql.managerReviewSchema, /review_manifest_sha256 bytea/i);
  assert.match(sql.managerReviewSchema, /review_storage_key text/i);
  assert.match(sql.managerReviewSchema, /final_signed_storage_key text/i);
  assert.match(sql.managerReviewSchema, /manager_review_timesheet_component_id uuid/i);
  assert.match(sql.managerReviewSchema, /document_role='ELECTRONIC_TIMESHEET_MANAGER_REVIEW'/i);
  assert.match(sql.managerReviewSchema, /review_page_count=1/i);
  assert.match(sql.managerReviewSchema, /final_signed_page_count=1/i);
});

test('official review and final render contracts bind the same immutable submission and candidate signature', () => {
  const helpers = sql.managerReviewHelpers;
  assert.match(helpers, /_candidate_render_input_v1/i);
  assert.match(helpers, /immutable_submission_json/i);
  assert.match(helpers, /candidate_signature_sha256/i);
  assert.match(helpers, /render_input_sha256/i);
  assert.match(helpers, /ELECTRONIC_MANAGER_REVIEW/);
  assert.match(helpers, /ELECTRONIC_SIGNED/);
  assert.match(helpers, /manager_signature_embedded',v_phase='FINAL'/i);
  assert.match(helpers, /manager_approval_date_embedded',v_phase='FINAL'/i);
  assert.match(helpers, /review_content_sha256 identifies the exact page shown to the manager/i);
  assert.match(helpers, /final_signed_content_sha256 identifies the signed derivative/i);
});

test('workflow gates manager review and finalisation on registered official document bytes', () => {
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  for (const action of [
    'WORKER_SUBMIT','REGISTER_MANAGER_REVIEW_DOCUMENT','SELECT_PHONE_APPROVAL',
    'CREATE_EMAIL_APPROVAL_REQUEST','BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS',
    'PHONE_APPROVE','EMAIL_APPROVE','MANAGER_REFUSE','REGISTER_FINAL_SIGNED_DOCUMENT',
    'CANCEL','SUPERSEDE'
  ]) assert.match(workflow, new RegExp(`'${action}'`));
  assert.match(workflow, /WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT/);
  assert.match(workflow, /READY_FOR_MANAGER_APPROVAL/);
  assert.match(workflow, /MANAGER_APPROVED_PENDING_FINAL_DOCUMENT/);
  assert.match(workflow, /READY_TO_FINALISE/);
  assert.match(workflow, /MANAGER_REVIEW_DOCUMENT_NOT_READY/);
  assert.match(workflow, /MANAGER_REVIEW_MANIFEST_MISMATCH/);
  assert.match(workflow, /FINAL_RENDER_INPUT_MISMATCH/);
  assert.match(workflow, /candidate_signature_embedded/);
  assert.match(workflow, /manager_signature_embedded/);
  assert.match(workflow, /manager_approval_date_embedded/);
  assert.match(workflow, /review_manifest_sha256/);
  assert.match(workflow, /required_component_ids/);
  assert.match(workflow, /final_signed_render_state='READY'/i);
  assert.match(workflow, /v_component_kind='MANAGER_SIGNATURE' and v_approval\.method='PHONE'[\s\S]*v_manager_capture_method:='DRAW'/i);
  assert.match(workflow, /v_approval\.method='EMAIL' or nullif\(v_payload->>'progress_version',''\) is not null/i);
  assert.match(workflow, /v_approval\.method='EMAIL'[\s\S]*MANAGER_APPROVAL_ATTESTATION_V1/i);
});

test('rejected resubmission is source-bound, transaction-owned and request-aware', () => {
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  assert.match(sql.replacementLineage, /replacement_of_workflow_id uuid/i);
  assert.match(sql.replacementLineage, /candidate_submission_workflows_replacement_of_fk/i);
  assert.match(sql.replacementLineage, /candidate_submission_workflows_replacement_source_uq/i);
  assert.match(sql.replacementLineage, /where replacement_of_workflow_id is not null/i);
  assert.match(sql.creationIdentity, /creation_request_sha256 bytea/i);
  assert.match(sql.creationIdentity, /creation_identity_json jsonb/i);
  assert.match(sql.creationIdentity, /candidate_submission_workflows_creation_identity_group_ck/i);
  assert.match(workflow, /v_action='RESUBMIT_REJECTED'/i);
  assert.match(workflow, /candidate-workflow-idempotency\|/i);
  assert.match(workflow, /candidate-rejected-source\|/i);
  assert.match(workflow, /replacement_of_workflow_id=v_source_workflow\.id/i);
  assert.match(workflow, /private\._candidate_rejection_replaced_v1\(v_source_workflow\.id\)/i);
  assert.match(workflow, /CANDIDATE_REJECTED_WORKFLOW_ALREADY_REPLACED/i);
  assert.match(workflow, /CANDIDATE_IDEMPOTENCY_CONFLICT/i);
  assert.match(workflow, /_candidate_workflow_creation_request_sha256_v1/i);
  assert.match(workflow, /creation_request_sha256 is distinct from v_creation_request_sha256/i);
  assert.match(workflow, /when v_source_workflow\.rejection_scope='COMPLETE_EXPENSE_CLAIM'[\s\S]*then 'CONTRACT_EXPENSE'/i);
  assert.match(workflow, /v_source_workflow\.route='PAPER' then 'PAPER' else 'ELECTRONIC'/i);
  assert.match(workflow, /'rejected_workflow_id',v_replacement_of_workflow_id/i);
  assert.match(workflow, /'replacement_workflow_id',v_workflow\.id/i);
  assert.match(workflow, /idempotency_key=btrim\(p_idempotency_key\)[\s\S]*for update/i);
});

test('NO_WORK_THIS_WEEK asks only for genuine user input', () => {
  const invocation = privateDefinition(sql.reads, '_candidate_action_invocation_v1');
  assert.match(invocation, /when 'NO_WORK_THIS_WEEK' then '\[\{"name":"idempotency_key","type":"uuid","required":true\}\]'/i);
  assert.doesNotMatch(invocation, /when 'NO_WORK_THIS_WEEK' then '[^']*expected_row_signature/i);
});

test('candidate read contracts expose readiness metadata but never review or final storage keys', () => {
  const detail = definition(sql.reads, 'candidate_app_timesheet_detail_v1');
  assert.match(detail, /review_document_ready/);
  assert.match(detail, /review_document_component_id/);
  assert.match(detail, /review_document_generation/);
  assert.match(detail, /review_page_count/);
  assert.match(detail, /manager_approval_state/);
  assert.match(detail, /final_signed_document_ready/);
  assert.doesNotMatch(detail, /review_storage_key|final_signed_storage_key/);
});

test('finalisation rechecks policy and composes existing WEEKLY, DAILY and authorise authorities', () => {
  const finalise = definition(sql.final, 'candidate_submission_finalize_atomic_v1');
  assert.match(finalise, /policy_fingerprint/);
  assert.match(finalise, /CANDIDATE_POLICY_CHANGED/);
  assert.match(finalise, /candidate_electronic_auto_authorise/);
  assert.match(finalise, /contract_week_manual_upsert_atomic/);
  assert.match(finalise, /timesheet_daily_manual_process_atomic/);
  assert.match(finalise, /timesheet_authorise_generic_atomic/);
  assert.match(finalise, /v_workflow\.route='PAPER'[\s\S]*v_auto_blocked:=true/i);
  assert.match(finalise, /candidate_app_system_actor_user_id/);
  assert.match(finalise, /v_workflow\.state<>'READY_TO_FINALISE'/i);
  assert.match(finalise, /review_render_input_sha256[\s\S]*final_signed_render_input_sha256/i);
  assert.match(finalise, /r2_nurse_key/);
  assert.match(finalise, /r2_auth_key/);
  assert.match(finalise, /ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE/);
  assert.match(finalise, /Official electronically signed timesheet/);
  assert.match(finalise, /'SIGNED_TIMESHEET'/);
  assert.doesNotMatch(finalise, /review_storage_key[\s\S]*insert into public\.timesheet_evidence/i);
});

test('whole-record rejection clears every active economic/document field and is durably idempotent', () => {
  const reject = definition(sql.final, 'candidate_submission_reject_atomic_v1');
  for (const required of [
    'additional_units_week', 'additional_units_per_day', 'manual_pdf_r2_key',
    'expenses_pay_ex_vat', 'expenses_charge_ex_vat', 'expenses_evidence_r2_key',
    'expenses_evidence_manifest', 'mileage_units', 'mileage_pay_ex_vat',
    'mileage_evidence_r2_key', 'mileage_evidence_manifest', 'travel_pay_ex_vat',
    'accommodation_pay_ex_vat', 'other_pay_ex_vat', 'actual_schedule_json',
    'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat'
  ]) assert.ok(reject.includes(required), `reject must clear ${required}`);
  assert.match(reject, /CANDIDATE_REJECT_REQUIRES_UNAUTHORISE/);
  assert.match(reject, /CANDIDATE_REJECT_PROTECTED_HISTORY/);
  assert.match(reject, /timesheet_qr_refuse_and_reset/);
  assert.match(reject, /processing_status='UNPROCESSED'/);
  assert.match(reject, /status='OPEN'/);
  assert.match(reject, /audit_events[\s\S]*correlation_id=p_idempotency_key[\s\S]*idempotent_replay/i);
  assert.match(reject, /w\.state='FINALISED'\s+and w\.target_timesheet_id=v_timesheet\.timesheet_id/i);
  assert.match(reject, /w\.state not in \('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'\)[\s\S]*w\.anchor_timesheet_id=v_timesheet\.timesheet_id[\s\S]*w\.state='FINALISED'[\s\S]*w\.target_timesheet_id=v_timesheet\.timesheet_id/i);
  assert.match(reject, /greatest\(w\.generation-1,1\) else w\.generation end as artifact_generation/i);
  assert.match(reject, /workflow_generation=v_workflow\.artifact_generation/i);
  assert.match(reject, /state=v_workflow\.captured_state/i);
});

test('candidate no-work composes existing delete/archive authority and replays safely', () => {
  const noWork = definition(sql.final, 'candidate_no_work_atomic_v1');
  assert.match(noWork, /contract_week_delete_planned/);
  assert.match(noWork, /timesheet_standard_delete_preview_v1/);
  assert.match(noWork, /timesheet_standard_delete_apply_v1/);
  assert.match(noWork, /timesheet_archive_transition_v1/);
  assert.match(noWork, /audit_events[\s\S]*correlation_id=p_idempotency_key[\s\S]*idempotent_replay/i);
});

test('existing weekly draft and canonical upsert authorities enforce candidate final-state guards', () => {
  assert.match(replacements.draft, /_candidate_draft_totals_guard_v1\(v_week\.id,p_totals_json\)/i);
  assert.match(replacements.draft, /'candidate_record_role',\s*v_candidate_guard->>'record_role'/i);
  assert.match(replacements.process, /_candidate_weekly_final_state_guard_v1\([\s\S]*p_tsfin_snapshot_json/i);
  assert.match(replacements.process, /'candidate_record_role',\s*v_candidate_final_state_guard->>'record_role'/i);
  assert.match(replacements.process, /'candidate_expected_line_type',\s*v_candidate_final_state_guard->>'expected_line_type'/i);
});

test('existing WEEKLY and DAILY materialisation authorities accept the complete electronic signature pair only in Candidate finalise context', () => {
  const process = replacements.process;
  assert.match(process, /current_setting\('cloudtms\.candidate_electronic_finalise',\s*true\)/i);
  for (const field of [
    'submission_mode',
    'auth_name',
    'auth_job_title',
    'r2_nurse_key',
    'r2_auth_key',
    'img_sha256_nurse',
    'img_sha256_auth',
    'candidate_workflow_id',
    'candidate_workflow_generation',
    'candidate_manager_approved_at_utc'
  ]) assert.match(process, new RegExp(field, 'i'), `${field} must be handled by the existing materialisation authorities`);
  assert.match(sql.final, /set_config\('cloudtms\.candidate_electronic_finalise',\s*'on',\s*true\)/i);
  assert.doesNotMatch(sql.managerReviewSchema, /drop\s+constraint\s+chk_ts_signatures_for_electronic/i);
  assert.doesNotMatch(sql.managerReviewSchema, /alter\s+table\s+public\.timesheets[\s\S]*disable\s+trigger/i);
});

test('existing office datasets, contexts, row patch and lifecycle signatures use one server capability authority', () => {
  assert.equal((replacements.process.match(/_candidate_dataset_overlay_v1/g) || []).length, 2);
  assert.match(replacements.process, /_candidate_office_context_overlay_v1/i);
  assert.ok((replacements.contexts.match(/_candidate_office_context_overlay_v1/g) || []).length >= 8);
  assert.match(replacements.lifecycle, /_candidate_signature_component_v1/i);
  for (const key of ['record_role', 'can_attach_timesheet', 'candidate_preview_border', 'can_reject_candidate_submission']) {
    assert.match(sql.helpers, new RegExp(`'${key}'`, 'i'));
  }
  assert.match(sql.helpers, /'candidate_preview_border',jsonb_build_object\([\s\S]*'width_px',4/i);
});

test('QR refusal is a whole-record reset and replacement email uses the approved sentence', () => {
  const reset = replacements.qr.slice(
    replacements.qr.toLowerCase().indexOf('create or replace function public.timesheet_qr_refuse_and_reset'),
    replacements.qr.toLowerCase().indexOf('create or replace function public.client_update_with_settings_v1')
  );
  assert.match(reset, /v_candidate_rejection_enabled boolean\s*:=\s*private\._candidate_feature_enabled_current_v1\('candidate_paper_qr'\)/i);
  assert.match(reset, /CASE WHEN v_candidate_rejection_enabled THEN '\{\}'::jsonb ELSE COALESCE\(v_current\.additional_units_week/i);
  for (const field of [
    'additional_units_json', 'expenses_pay_ex_vat', 'expenses_charge_ex_vat',
    'mileage_units', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat',
    'travel_pay_ex_vat', 'accommodation_pay_ex_vat', 'other_pay_ex_vat'
  ]) assert.match(reset, new RegExp(`${field}\\s*=`, 'i'));
  assert.match(reset, /WHEN v_candidate_rejection_enabled THEN 'UNPROCESSED'/i);
  assert.match(reset, /status\s*=\s*CASE WHEN v_candidate_rejection_enabled THEN 'OPEN'/i);
  assert.match(reset, /update public\.timesheet_evidence[\s\S]*processing_state='SUPERSEDED'/i);
  assert.match(replacements.contexts, /Please remember to sign the replacement timesheet before returning it\./);
});

test('existing client create/update RPCs accept and validate the complete candidate policy surface', () => {
  const fields = [
    'candidate_electronic_auto_authorise',
    'candidate_expenses_require_separate_timesheet',
    'candidate_paper_submission_enabled',
    'candidate_expense_invoice_email',
    'candidate_manager_approval_policy_json',
    'allow_daily_manager_authorise_on_phone',
    'allow_daily_manager_authorise_by_email'
  ];
  for (const field of fields) {
    assert.match(replacements.clientCreate, new RegExp(field, 'i'));
    assert.match(replacements.clientUpdate, new RegExp(field, 'i'));
  }
  assert.match(replacements.clientCreate, /CLIENT_SETTINGS_DAILY_MANAGER_METHOD_REQUIRED/);
  assert.match(replacements.clientUpdate, /CLIENT_DAILY_MANAGER_METHOD_REQUIRED/);
  assert.match(replacements.clientCreate, /CANDIDATE_IMPORT_EXPENSE_SEPARATION_REQUIRED/);
  assert.match(replacements.clientCreate, /CANDIDATE_IMPORT_EXPENSE_EMAIL_REQUIRED/);
  assert.match(replacements.clientUpdate, /CANDIDATE_IMPORT_EXPENSE_SEPARATION_REQUIRED/);
  assert.match(replacements.clientUpdate, /CANDIDATE_IMPORT_EXPENSE_EMAIL_REQUIRED/);
});

test('the targeted correction pass enforces exact categories, complete economics and complete paper returns', () => {
  const apply = definition(sql.expenses, 'timesheet_expense_apply_atomic_v1');
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  const finalise = definition(sql.final, 'candidate_submission_finalize_atomic_v1');
  assert.match(sql.foundation, /component_kind='EXPENSE_EVIDENCE' and expense_category is not null[\s\S]*expense_category in \('TRAVEL','ACCOMMODATION','OTHER','MILEAGE'\)/i);
  assert.match(sql.foundation, /component_kind='MILEAGE_FORM' and expense_category is not null and expense_category='MILEAGE'/i);
  assert.match(workflow, /v_component_kind='EXPENSE_EVIDENCE'[\s\S]*v_expense_category in \('TRAVEL','ACCOMMODATION','OTHER','MILEAGE'\)/i);
  assert.doesNotMatch(apply, /component\.expense_category=v_category\s+or\s+component\.expense_category is null/i);
  for (const field of [
    'expenses_pay_ex_vat','expenses_charge_ex_vat','mileage_units','mileage_pay_ex_vat',
    'mileage_charge_ex_vat','travel_pay_ex_vat','travel_charge_ex_vat',
    'accommodation_pay_ex_vat','accommodation_charge_ex_vat','other_pay_ex_vat','other_charge_ex_vat'
  ]) {
    assert.match(apply, new RegExp(field, 'i'), `expense apply must validate ${field}`);
    assert.match(workflow, new RegExp(field, 'i'), `worker submission must freeze ${field}`);
  }
  assert.match(workflow, /CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE/);
  assert.match(workflow, /pg_advisory_xact_lock[\s\S]*CANDIDATE_EXPENSE_CLAIM/i);
  assert.match(workflow, /prior_fin\.authorised_at_utc is not null/i);
  assert.match(sql.helpers, /IMPORT_MANDATORY/);
  assert.match(sql.helpers, /v_role='IMPORT_HOURS' or \(v_separate and v_role='HOURS_ONLY'\)/i);
  assert.match(sql.managerReviewSchema, /paper_return_manifest_json jsonb/i);
  assert.match(sql.managerReviewSchema, /paper_return_manifest_sha256 bytea/i);
  assert.match(sql.managerReviewSchema, /paper_return_page_key text/i);
  assert.match(workflow, /CANDIDATE_PAPER_RETURN_INCOMPLETE/);
  assert.match(workflow, /candidate_submission_components_paper_return_page_uq/);
  assert.match(finalise, /paper_return_manifest_sha256[\s\S]*CANDIDATE_PAPER_RETURN_INCOMPLETE/i);
  assert.match(sql.managerReviewHelpers, /component_kind='SIGNED_RETURN'[\s\S]*paper_return_page_key/i);
});

test('read and placement policy uses all authoritative worked and dated boundaries', () => {
  const page = definition(sql.reads, 'candidate_app_timesheet_page_v1');
  const missing = definition(sql.reads, 'candidate_missing_week_options_v1');
  const placement = definition(sql.expenses, 'expense_placement_resolve_v1');
  assert.match(sql.helpers, /v_additional=0[\s\S]*additional_units_week[\s\S]*additional_units_per_day/i);
  assert.match(placement, /tf\.additional_units_json/i);
  assert.match(placement, /t\.additional_units_week[\s\S]*t\.additional_units_per_day/i);
  assert.match(page, /additional_units_json[\s\S]*additional_units_week[\s\S]*additional_units_per_day/i);
  assert.match(page, /current_week_ending_date-105/i);
  assert.doesNotMatch(page, /current_date\s*-\s*55/i);
  assert.match(missing, /generate_series[\s\S]*cross join lateral[\s\S]*_candidate_policy_resolve_v1\(v_contract\.client_id,v_contract\.id,g::date\)/i);
  assert.match(missing, /_candidate_submission_mode_v1\(v_contract\.client_id,v_contract\.id,g::date\)/i);
});

test('rejected workflows project through the replacement current version with server-owned recovery scope', () => {
  const page = definition(sql.reads, 'candidate_app_timesheet_page_v1');
  const detail = definition(sql.reads, 'candidate_app_timesheet_detail_v1');
  const replaced = privateDefinition(sql.reads, '_candidate_rejection_replaced_v1');
  assert.match(page, /classified\.state='REJECTED'[\s\S]*resolution\.carrier_contract_week_id=classified\.contract_week_id/i);
  assert.match(page, /classified\.state='REJECTED'[\s\S]*current_week\.id=classified\.contract_week_id/i);
  assert.match(page, /RESUBMIT_EXPENSE_CLAIM/);
  assert.match(page, /RESUBMIT_TIMESHEET_AND_EXPENSES/);
  assert.match(page, /RESUBMIT_TIMESHEET/);
  assert.match(replaced, /later\.state not in \('CANCELLED','EXPIRED','SUPERSEDED'\)/i);
  assert.match(replaced, /direct_replacement\.replacement_of_workflow_id=v_rejected\.id[\s\S]*return true/i);
  assert.match(replaced, /later\.created_at_utc>=v_rejected\.updated_at_utc/i);
  assert.match(replaced, /v_rejected\.workflow_kind='CONTRACT_COMBINED'[\s\S]*later\.workflow_kind='CONTRACT_COMBINED'[\s\S]*later\.contract_week_id is not distinct from v_rejected\.contract_week_id/i);
  assert.match(replaced, /v_rejected\.workflow_kind='CONTRACT_HOURS'[\s\S]*later\.workflow_kind in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)[\s\S]*later\.contract_week_id is not distinct from v_rejected\.contract_week_id/i);
  assert.match(replaced, /v_rejected\.workflow_kind='CONTRACT_EXPENSE'[\s\S]*later\.week_ending_date is not distinct from v_rejected\.week_ending_date[\s\S]*later\.workflow_kind in \('CONTRACT_EXPENSE','CONTRACT_COMBINED'\)/i);
  assert.match(replaced, /v_rejected\.workflow_kind='DAILY'[\s\S]*later\.workflow_kind='DAILY'[\s\S]*later\.work_date is not distinct from v_rejected\.work_date[\s\S]*later_timesheet\.booking_id[\s\S]*rejected_timesheet\.booking_id/i);
  assert.match(page, /_candidate_rejection_replaced_v1\(w\.id\)/i);
  assert.match(page, /'claim_family',resolved\.claim_family/i);
  assert.match(page, /'rejection_actionable',resolved\.rejection_actionable/i);
  assert.match(page, /'rejections',d\.actionable_rejections/i);
  assert.match(page, /'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'/i);
  assert.match(page, /private\._candidate_status_code_v1\([\s\S]*d\.rejected_workflow is not null/i);
  assert.match(page, /'rejection',case[\s\S]*'required_action'/i);
  assert.doesNotMatch(page, /or d\.active_workflow_state is not null then null/i);
  assert.match(detail, /v_detail_source_timesheet_id:=case[\s\S]*CONTRACT_EXPENSE[\s\S]*anchor_timesheet_id[\s\S]*source_version[\s\S]*current_version\.booking_id=source_version\.booking_id/i);
  assert.match(detail, /_candidate_workflow_maps_to_card_v1\(w\.id,p_timesheet_id,v_week\.id\)/i);
  assert.match(detail, /'required_resubmission_action'[\s\S]*RESUBMIT_EXPENSE_CLAIM/i);
  assert.match(detail, /'rejections',[\s\S]*rejection_actionable/i);
  assert.match(detail, /_candidate_rejection_replaced_v1\(w\.id\)/i);
  assert.match(sql.reads, /_candidate_paper_pack_readiness_v1[\s\S]*candidate_paper_pack_ready[\s\S]*candidate_complete_pack_storage_key/i);
  assert.match(sql.reads, /\/resubmit[\s\S]*_candidate_action_invocation_v1/i);
});

test('PAPER rejection retires a shared QR source as one locked set before record rotation', () => {
  const retireSet = privateDefinition(sql.workflow, '_candidate_paper_delivery_retire_set_v1');
  const sourceContext = privateDefinition(
    sql.workflow,
    '_candidate_paper_source_workflow_context_v1'
  );
  const transition = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  const reject = definition(sql.final, 'candidate_submission_reject_atomic_v1');
  assert.match(retireSet, /cardinality\(p_workflow_ids\) is distinct from cardinality\(p_expected_generations\)/i);
  assert.match(retireSet, /MULTIPLE_QR_SOURCE_FAMILIES|QR_SOURCE_SCOPE_MISMATCH/i);
  assert.match(retireSet, /CANDIDATE_PAPER_FAMILY:[\s\S]*pg_advisory_xact_lock\(hashtextextended\(v_family_key,0\)\)/i);
  assert.match(retireSet, /CANDIDATE_PAPER_SOURCE:'\|\|v_source_key/i);
  assert.match(retireSet, /state in \('AWAITING_PAPER_RETURN','RECEIVED','FINALISED'\)/i);
  assert.match(retireSet, /attempt_lease_expires_at_utc>p_now_utc[\s\S]*CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS/i);
  assert.match(retireSet, /qr_token_hash[\s\S]*v_current_token_hash/i);
  assert.match(retireSet, /_candidate_paper_delivery_retire_v1\([\s\S]*v_current_token_owner_workflow_id[\s\S]*v_current_token_owner_generation/i);
  assert.match(retireSet, /'qr_invalidation_proven',true/i);
  assert.match(retireSet, /'preserved_workflows'/i);
  assert.match(sourceContext, /current_token_owner_workflow_id/i);
  assert.match(sourceContext, /selected_workflow_id/i);
  assert.match(sourceContext, /MULTIPLE_NONTERMINAL_PAPER_WORKFLOWS/i);
  assert.match(sourceContext, /CURRENT_QR_TOKEN_OWNER_TERMINAL_WITH_LIVE_WORKFLOW/i);
  assert.match(sourceContext, /affected_nonterminal_workflows/i);
  assert.match(sourceContext, /WORKER_DRAFT[\s\S]*mail receipt/i);
  assert.match(sourceContext, /CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT/i);
  assert.match(retireSet, /CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT/i);
  assert.match(retireSet, /unselected_nonterminal_workflows/i);
  assert.match(reject, /v_paper_workflow_ids:=array_append\(v_paper_workflow_ids,v_workflow\.id\)/i);
  assert.match(reject, /CANDIDATE_PAPER_FAMILY:[\s\S]*pg_advisory_xact_lock\(hashtextextended\(v_rejection_family_key,0\)\)/i);
  assert.match(reject, /v_workflow\.state in \('AWAITING_PAPER_RETURN','RECEIVED','FINALISED'\)/i);
  assert.match(reject, /_candidate_paper_delivery_retire_set_v1\([\s\S]{0,80}v_paper_workflow_ids,v_paper_workflow_generations/i);
  assert.match(reject, /v_paper_retirement_result->>'qr_invalidation_proven'[\s\S]{0,40}::boolean,false/i);
  assert.doesNotMatch(reject, /_candidate_paper_delivery_retire_v1\(v_captured_workflow\.id/i);

  assert.match(transition, /v_action in \('CANCEL','SUPERSEDE'\)[\s\S]*v_workflow\.state in \('AWAITING_PAPER_RETURN','RECEIVED'\)[\s\S]*_candidate_paper_delivery_retire_set_v1/i);
  assert.match(transition, /v_action='PAPER_PROVIDER_SUBMIT_PERMIT'[\s\S]*from public\.mail_outbox[\s\S]*for update[\s\S]*attempt_lease_expires_at_utc=v_provider_permit_expires_at/i);
  assert.match(transition, /v_action='PAPER_RETURN'[\s\S]*from public\.mail_outbox[\s\S]*for update[\s\S]*CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS/i);
});

test('invoice grouping derives expense-only economics and isolates the effective expense recipient', () => {
  assert.match(replacements.invoiceGroups, /tf\.hours_day,tf\.hours_night,tf\.hours_sat,tf\.hours_sun,tf\.hours_bh/i);
  assert.match(replacements.invoiceGroups, /tf\.expenses_pay_ex_vat,tf\.expenses_charge_ex_vat/i);
  assert.match(replacements.invoiceGroups, /tf\.mileage_pay_ex_vat,tf\.mileage_charge_ex_vat/i);
  assert.match(replacements.invoiceGroups, /then 'EXPENSE'/i);
  assert.match(replacements.invoiceGroups, /expense_delivery_identity/i);
  assert.match(replacements.invoiceGroups, /EXPENSE_INVOICE_EMAIL_REQUIRED/);
});

test('invoice delivery sends expense stream to the expense email without self-bill suppression', () => {
  assert.match(replacements.invoiceDelivery, /when private\._candidate_feature_enabled_current_v1\('candidate_expense_invoice_routing_v1'\)[\s\S]{0,120}and f\.invoice_stream='EXPENSE' then 'EXPENSE_INVOICE_EMAIL'/i);
  assert.match(replacements.invoiceDelivery, /f\.invoice_stream<>'EXPENSE' and f\.invoice_self_bill/i);
  assert.match(replacements.invoiceDelivery, /EXPENSE_INVOICE_EMAIL_REQUIRED/);
  assert.match(replacements.invoiceDelivery, /INVOICE_DELIVERY_ROUTE_V6/);
});

test('route, DAILY and auto-authorisation authority is enforced at every mutation boundary', () => {
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  const finalise = definition(sql.final, 'candidate_submission_finalize_atomic_v1');
  const noWork = definition(sql.final, 'candidate_no_work_atomic_v1');
  const page = definition(sql.reads, 'candidate_app_timesheet_page_v1');

  assert.match(sql.helpers, /_candidate_route_family_v1/i);
  assert.match(sql.helpers, /IMPORT_AUTHORITATIVE|MANUAL_NON_QR|ELECTRONIC|QR/i);
  assert.match(workflow, /_candidate_route_family_v1[\s\S]*CANDIDATE_ROUTE_FAMILY_MISMATCH/i);
  assert.match(finalise, /_candidate_route_family_v1[\s\S]*CANDIDATE_ROUTE_FAMILY_MISMATCH/i);
  assert.match(noWork, /candidate_no_work_allowed[\s\S]*CANDIDATE_NO_WORK_NOT_ALLOWED/i);

  assert.match(workflow, /_candidate_daily_entitled_v1[\s\S]*CANDIDATE_DAILY_ENTITLEMENT_REQUIRED/i);
  assert.match(finalise, /_candidate_daily_entitled_v1[\s\S]*CANDIDATE_DAILY_ENTITLEMENT_REQUIRED/i);
  assert.match(sql.helpers, /_candidate_daily_work_date_v1[\s\S]*Europe\/London/i);
  assert.match(workflow, /_candidate_daily_work_date_v1/i);
  assert.match(finalise, /_candidate_daily_work_date_v1/i);

  assert.match(sql.helpers, /_candidate_submission_issue_codes_v1\([\s\S]*p_policy_snapshot jsonb/i);
  assert.match(sql.helpers, /actual_days[\s\S]*sum\(net_minutes\)[\s\S]*planned_days[\s\S]*sum\(gross_minutes\)/i);
  assert.match(sql.helpers, /count\(explicit_break_minutes\)>0[\s\S]*when v_workflow\.workflow_kind='DAILY' then 60/i);
  assert.match(sql.helpers, /actual\.net_minutes,0\)>planned\.net_minutes\*\(1\+v_threshold\/100\.0\)/i);
  assert.match(sql.helpers, /HEALTHROSTER_VALIDATION_REQUIRED/i);
  assert.match(finalise, /ADDITIONAL_UNITS_NEEDS_CHECKING/i);

  assert.match(sql.final, /_candidate_daily_save_recalculate_atomic_v1/i);
  assert.match(sql.helpers, /_candidate_daily_canonical_save_input_v1/i);
  assert.match(workflow, /BEGIN_CANONICAL_DAILY_SAVE/i);
  assert.doesNotMatch(workflow, /REGISTER_CANONICAL_DAILY_SAVE/i);
  assert.match(sql.final, /expected_pre_save_row_signature[\s\S]*CANDIDATE_DAILY_CANONICAL_SAVE_STALE[\s\S]*update public\.timesheets/i);
  assert.match(sql.managerReviewSchema, /daily_context_sha256 bytea/i);
  assert.match(sql.managerReviewSchema, /canonical_financial_sha256 bytea/i);
  assert.match(sql.final, /_candidate_daily_context_contract_v1/i);
  assert.match(sql.final, /CANDIDATE_DAILY_LOCKED_CONTEXT_STALE/i);
  assert.match(sql.final, /canonical_context_sha256_hex/i);
  assert.match(sql.final, /financial_policy_sha256_hex/i);
  assert.match(sql.final, /_candidate_financial_content_sha256_v1/i);
  assert.match(finalise, /_candidate_daily_save_recalculate_atomic_v1[\s\S]*timesheet_daily_manual_process_atomic/i);
  assert.match(sql.final, /tsfin_resolve_rates_batch[\s\S]*tsfin_write_current_snapshot_single_bounded/i);
  assert.doesNotMatch(finalise, /classifyMinutes|apply_erni_to|erni_pct|invoiceSegments\.push/i);

  assert.match(page, /client_name/i);
  assert.match(page, /job_title/i);
  assert.match(page, /band/i);
  assert.match(page, /current_week_ending_date/i);
  assert.match(page, /workflow_overlay as materialized[\s\S]*display_timesheet_id/i);
  assert.match(sql.helpers, /_candidate_normalize_domain_v1[\s\S]*regexp_replace\([\s\S]*'\^@'/i);
  assert.match(sql.routeAuthority, /v_current_row\.authorised_at_server is not null[\s\S]*v_tsfin_row\.authorised_at_utc is not null[\s\S]*Unauthorise it before changing its submission route/i);
  assert.match(sql.routeAuthority, /day_references_json[\s\S]*TIMESHEET_REVERT_DOCUMENT_LINEAGE_INCOMPLETE/i);
  assert.match(sql.routeAuthority, /candidate_workflow_id[\s\S]*final_signed_content_sha256[\s\S]*candidate_component_id/i);
  assert.match(sql.routeAuthority, /_candidate_financial_content_sha256_v1[\s\S]*TIMESHEET_REVERT_FINANCIAL_CONTENT_MISMATCH/i);
  assert.match(sql.managerReviewSchema, /timesheets_manual_adjustment_idempotency_uq/i);
});

test('invoice generation and issue validation preserve the expense stream end to end', () => {
  assert.match(replacements.invoiceAdvance, /header_snapshot_json#>>'\{meta,invoice_stream\}'/i);
  assert.match(replacements.invoiceAdvance, /upper\(coalesce\(c\.payload_json->>'invoice_stream','NORMAL'\)\)/i);
  assert.match(replacements.invoiceAdvance, /target_header_streams as materialized/i);
  assert.match(replacements.invoiceAdvance, /'invoice_stream',s\.invoice_stream/i);
  assert.match(replacements.invoiceIssue, /header_snapshot_json#>>'\{meta,invoice_stream\}'/i);
  assert.match(replacements.invoiceIssue, /header_snapshot_json->>'invoice_stream'/i);
});

test('standalone handover replacements contain all nineteen amended existing functions and eight route support authorities in full', () => {
  const generated = `${replacements.generatedOffice}\n${replacements.generatedOther}\n${replacements.routeAuthority}`;
  const expected = [
    'public.contract_week_manual_draft_upsert_atomic_v1',
    'public.contract_week_manual_upsert_atomic',
    'public.timesheet_daily_manual_process_atomic',
    'public.bulk_process_dataset_v1',
    'public.bulk_authorise_dataset_v1',
    'public.bulk_timesheet_row_patch_v1',
    'public.bulk_process_row_context_v1',
    'public.bulk_authorise_row_context_v1',
    'public.timesheet_qr_send_enqueue_v1',
    'public.timesheet_lifecycle_guard_signature_v1',
    'public.timesheet_qr_refuse_and_reset',
    'public.client_update_with_settings_v1',
    'public.client_create_with_settings_v1',
    'private._invoice_generation_resolve_command_groups',
    'private._invoice_delivery_routes_batch',
    'private._invoice_issue_validate_batch',
    'private._invoice_generation_advance_core_v8',
    'public.timesheet_route_version_rotate',
    'public.timesheet_qr_restore_version',
    'private._timesheet_route_version_core_v1',
    'private._timesheet_exact_electronic_restore_proof_v1',
    'private._timesheet_route_change_context_v1',
    'private._timesheet_route_supersede_candidate_v1',
    'private._timesheet_route_resubmission_notifications_v1',
    'private._candidate_qr_pack_ready_notification_v1',
    'public.timesheet_route_version_preview_v1',
    'public.timesheet_route_version_confirmed_v1'
  ];
  for (const name of expected) {
    assert.match(generated, new RegExp(`create(?: or replace)? function ${name.replace('.', '\\.') }\\s*\\(`, 'i'), `${name} must be packaged in full`);
  }
  const definitions = [...generated.matchAll(/^create(?: or replace)? function (public|private)\.[a-z0-9_]+\s*\(/gmi)];
  assert.equal(definitions.length, expected.length);
  assert.ok(
    replacements.generatedOther.toLowerCase().indexOf('drop function if exists private._invoice_issue_validate_batch')
      < replacements.generatedOther.toLowerCase().indexOf('drop function if exists private._invoice_delivery_routes_batch'),
    'repeatable replacement must drop the dependent issue validator before invoice delivery'
  );
});

test('final route conversion is signed-state aware, stale-safe, reasoned, retention-safe and notification-complete', () => {
  const route = replacements.routeAuthority;
  assert.match(route, /candidate_route_confirmation/i);
  assert.match(route, /ROUTE_CHANGE_CONTEXT_CHANGED/i);
  assert.match(route, /CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL/i);
  assert.match(route, /MANAGER_APPROVED_TO_MANUAL/i);
  assert.match(route, /QR_ISSUED_TO_MANUAL/i);
  assert.match(route, /QR_SIGNED_TO_MANUAL/i);
  assert.match(route, /CANDIDATE_REPORTED_HOURS_INCORRECT/i);
  assert.match(route, /OTHER_EXCEPTIONAL_OFFICE_INTERVENTION/i);
  assert.match(route, /The approval request for this timesheet has been withdrawn by CloudTMS\. No further action is required\./i);
  assert.match(route, /resubmission_required/i);
  assert.match(route, /candidate-resubmission-required:/i);
  assert.match(route, /retain_historical_evidence/i);
  assert.match(route, /QR_RESTORE_RETIRED_USE_FRESH_GENERATION/i);
  assert.match(route, /_candidate_submission_mode_v1/i);
  assert.match(route, /_timesheet_route_supersede_candidate_v1[\s\S]*state in \('AWAITING_PAPER_RETURN','RECEIVED','FINALISED'\)[\s\S]*_candidate_paper_delivery_retire_set_v1/i);
  assert.match(route, /CANDIDATE_PAPER_FAMILY:[\s\S]*pg_advisory_xact_lock\(hashtextextended\(v_route_family_key,0\)\)[\s\S]*hashtext\(btrim\(v_requested\.booking_id\)\)/i);
  assert.match(route, /'paper_workflow_id',v_paper_workflow\.id/i);
  assert.match(route, /_candidate_paper_source_workflow_context_v1\(\s*v_current\.timesheet_id/i);
  assert.match(route, /paper_source_current_token_owner_workflow_id/i);
  assert.match(route, /paper_source_affected_nonterminal_workflow_count/i);
  assert.match(route, /CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT/i);
  assert.match(route, /no live[\s\S]*PAPER workflow[\s\S]*historical/i);
  assert.match(route, /_timesheet_route_supersede_candidate_v1\([\s\S]*v_context->>'paper_workflow_id'/i);
  assert.match(route, /CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM/i);
  assert.match(route, /The candidate has started an expense claim but has not completed it\. Do you want to remove the incomplete claim and continue\?/i);
  assert.match(route, /incomplete_expense_claim_removal_required[\s\S]*incomplete_expense_claim_removed/i);
  assert.match(route, /v_incomplete_expense_workflow_id[\s\S]*_timesheet_route_supersede_candidate_v1/i);
  assert.match(route, /REFUSED remains recoverable[\s\S]*workflow\.state not in \('FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'\)/i);
  assert.match(route, /Equivalent mail-independent invariant for ELECTRONIC source rotation[\s\S]*CANDIDATE_ROUTE_ACTIVE_WORKFLOW_CONFLICT/i);
  assert.match(route, /ALLOW_QR_AGAIN_REQUIRES_PRIOR_LINEAGE_OR_PAPER_PERMISSION/i);
  assert.match(route, /_timesheet_route_version_legacy_v1/i);
  assert.match(route, /legacy_route_version_legacy_v1|_timesheet_route_version_legacy_v1/i);
  assert.match(route, /ROUTE_CHANGE_WORKFLOW_CONFLICT/i);
  assert.match(route, /QR_REPLACED_BY_OFFICE/i);
  assert.match(route, /notification_deferred_until_pack_ready/i);
  assert.match(route, /candidate_submission_route_intent/i);
  assert.match(route, /DAILY_PAPER_ROUTE_NOT_ALLOWED/i);
  assert.match(route, /v_current\.r2_nurse_key[\s\S]*v_current\.img_sha256_nurse/i);
  assert.match(route, /v_current\.r2_auth_key[\s\S]*v_current\.img_sha256_auth[\s\S]*candidate_manager_approved_at_utc/i);
  const signedClassifier = route.match(/v_qr_signed:=v_qr_route_active[\s\S]*?\n  v_scope:=/i)?.[0] || '';
  assert.match(signedClassifier, /qr_scanned_at/i);
  assert.match(signedClassifier, /document_role='SIGNED_TIMESHEET'/i);
  assert.doesNotMatch(signedClassifier, /manual_pdf_r2_key|manual_document_asset_id/i);
  assert.match(route, /v_qr_code_generated:=[\s\S]*qr_token[\s\S]*qr_generated_at/i);
  assert.match(route, /v_qr_pack_ready:=[\s\S]*manual_pdf_r2_key[\s\S]*document_state::text,''\)\)='READY'/i);
  assert.match(route, /v_qr_pack_issued_or_sent:=[\s\S]*qr_last_sent_at_utc[\s\S]*qr_last_sent_hash/i);
  assert.match(route, /QR_PACK_READY_NOT_ISSUED/i);
  assert.match(route, /required_component_ids[\s\S]*required_component_manifest_json/i);
  assert.match(route, /unnest\(v_revert_required_component_ids\)[\s\S]*component\.state<>'IMMUTABLE'/i);
  assert.match(route, /APPROVED_MANIFEST_PROOF_INCOMPLETE|TIMESHEET_REVERT_APPROVED_MANIFEST_INCOMPLETE/i);
  assert.match(route, /REQUIRED_FINAL_PAGE_PROOF_INCOMPLETE/i);
  assert.match(route, /contract_row\.candidate_id/i);
  assert.match(route, /expires_at_utc<=p_now_utc/i);
  assert.match(route, /revoke all on function public\.timesheet_qr_restore_version\(uuid,uuid,text,uuid\)[\s\S]*from public,anon/i);
  assert.match(route, /grant execute on function public\.timesheet_qr_restore_version\(uuid,uuid,text,uuid\)[\s\S]*to authenticated,service_role/i);
  assert.match(sql.foundation, /"resubmission_required":true/i);
  assert.match(sql.foundation, /candidate_submission_route_intent[\s\S]*'PAPER'[\s\S]*sheet_scope\s*=\s*'WEEKLY'/i);
  assert.doesNotMatch(sql.foundation, /candidate_submission_route_intent\s*=\s*'PAPER'[\s\S]{0,300}sheet_scope\s+in\s*\([\s\S]{0,150}'DAILY'/i);
  assert.match(sql.helpers, /candidate_paper_submission_allowed'[\s\S]*not v_is_daily/i);
});

test('Candidate App SQL does not implement payment, settlement, invoice issue or provider delivery', () => {
  assert.doesNotMatch(all, /pay_batch|banking_pay|settlement|remittance|provider_submission|webhook_replay/i);
  assert.doesNotMatch(all, /invoice_issue_and_queue|invoice_generation|invoice_delivery_routes/i);
  assert.doesNotMatch(all, /http_post|net\.http|pg_net|comms_outbox/i);
});

test('component preparation replay returns one immutable database-owned upload contract', () => {
  const workflow = definition(sql.workflow, 'candidate_workflow_transition_atomic_v1');
  assert.match(workflow, /CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT/i);
  assert.match(workflow, /v_component\.component_kind is distinct from v_component_kind/i);
  assert.match(workflow, /v_component\.document_role is distinct from v_document_role/i);
  assert.match(workflow, /v_component\.expense_category is distinct from v_expense_category/i);
  assert.match(workflow, /lower\(v_component\.media_type\) is distinct from v_requested_media_type/i);
  assert.match(workflow, /v_component\.byte_size is distinct from v_requested_byte_size/i);
  assert.match(workflow, /v_component\.workflow_generation is distinct from v_workflow\.generation[\s\S]*CANDIDATE_COMPONENT_PREPARE_GENERATION_CONFLICT/i);
  assert.match(workflow, /v_component\.state not in \('PENDING','IMMUTABLE'\)[\s\S]*CANDIDATE_COMPONENT_PREPARE_STATE_CONFLICT/i);
  const replay = workflow.match(/if found then[\s\S]*?end if;[\s\S]*?select coalesce\(max\(component_no\)/i)?.[0] || '';
  for (const field of [
    'storage_key', 'media_type', 'byte_size', 'component_kind',
    'document_role', 'expense_category', 'paper_return_page_key', 'workflow_generation', 'state'
  ]) assert.match(replay, new RegExp(`'${field}'`, 'i'), `replay must return ${field}`);
  const first = [...workflow.matchAll(/(?:return|v_response\s*:=)\s*jsonb_build_object\('ok',true,'idempotent_replay',false[\s\S]*?\);/gi)]
    .map(match => match[0]).find(value => /'component_id'/i.test(value)) || '';
  for (const field of [
    'storage_key', 'media_type', 'byte_size', 'component_kind',
    'document_role', 'expense_category', 'paper_return_page_key', 'workflow_generation', 'state'
  ]) assert.match(first, new RegExp(`'${field}'`, 'i'), `first prepare must return ${field}`);
  assert.match(workflow, /if v_component\.state<>'PENDING'[\s\S]*CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT/i);
  assert.match(workflow, /where id=v_component\.id and state='PENDING' returning \* into v_component/i);
});

test('every SQL file has complete function/migration dollar-quote pairs', () => {
  for (const [key, source] of Object.entries(sql)) {
    for (const delimiter of ['$function$', '$migration$']) {
      const count = source.split(delimiter).length - 1;
      assert.equal(count % 2, 0, `${key} has an unbalanced ${delimiter}`);
    }
  }
  assert.doesNotMatch(all, /^end\s*\r?\n\$(?:function|migration)\$/gmi);
  assert.doesNotMatch(all, /return case when tg_op=/i);
});
