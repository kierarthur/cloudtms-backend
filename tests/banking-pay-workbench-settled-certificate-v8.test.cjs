const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const migration = read('supabase/migrations/02092026_2300_banking_pay_workbench_settled_certificate_v8.sql');
const financeKeyMigration = read('supabase/migrations/02092026_2305_banking_pay_workbench_settled_certificate_finance_keys_v8.sql');
const build = read('supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql');
const readers = read('supabase/repeatable/02092026_2302_banking_pay_workbench_settled_certificate_digest_reader_v8.sql');
const certifiedStart = read('supabase/repeatable/02092026_2303_banking_pay_draft_certified_operation_start_v8.sql');
const fence = read('supabase/repeatable/02092026_2304_banking_pay_workbench_session_authority_fence_v1.sql');
const operationStart = read('supabase/repeatable/04082026_1154_banking_pay_operation_start.sql');
const operationStartClosure = read('supabase/repeatable/02092026_2305_banking_pay_operation_start_lock_budget_v1.sql');
const publisher = read('supabase/repeatable/07082026_2154_pay_workbench_publish_certified_source_preview_v1.sql');
const attemptExecute = read('supabase/repeatable/07082026_1014_pay_workbench_source_build_attempt_execute_v1.sql');
const failJob = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');
const orphanRepair = read('supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql');
const catalogManifest = JSON.parse(read('supabase/verification/banking_pay_workbench_settled_certificate_v8_catalog_manifest.json'));
const catalogVerifier = read('supabase/verification/verify_banking_pay_workbench_settled_certificate_v8_catalog.mjs');
const preapplyGenerator = read('supabase/verification/generate_banking_pay_catalog_preapply_check.mjs');
const release = JSON.parse(read('supabase/release/current-release.json'));
const sourceWorkflow = read('.github/workflows/supabase-migrate.yml');
const broker = read('broker/src/index.js');
const runtimeVerification = read('tests/02092026_2392_banking_pay_workbench_settled_certificate_v8_runtime_verification.sql');

test('targeted publication certifies completed rotation-family scope and permits a correct zero-row result', () => {
  const start = publisher.indexOf("IF v_refresh_scope_kind = 'TARGETED_TIMESHEETS' THEN");
  const end = publisher.indexOf('IF v_publication_identity_write_enabled', start);
  assert.ok(start >= 0 && end > start);
  const targeted = publisher.slice(start, end);
  assert.match(targeted, /public\._pay_timesheet_rotation_scope\s*\(/i);
  assert.match(targeted, /private\.banking_pay_workbench_economic_build_scope/i);
  assert.match(targeted, /build_scope\.build_id\s*=\s*p_economic_build_id/i);
  assert.match(targeted, /build_scope\.candidate_id\s*=\s*p_candidate_id/i);
  assert.match(targeted, /build_scope\.closure_status\s*=\s*'SEALED'/i);
  assert.match(targeted, /TARGETED_SCOPE_NOT_IN_COMPLETED_BUILD/i);
  assert.doesNotMatch(targeted, /banking_pay_workbench_candidate_source_lines/i);
});

test('a genuine targeted-scope mismatch preserves its detail and stops deterministic retry loops', () => {
  assert.match(attemptExecute, /v_error_detail=PG_EXCEPTION_DETAIL/i);
  assert.match(attemptExecute, /'detail',NULLIF\(v_error_detail,''\)/i);
  assert.match(failJob, /v_is_deterministic_stage_error := v_error_code IN \([\s\S]*?'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'/i);
  assert.match(failJob, /'detail',p_error_json->>'detail'/i);
  assert.match(orphanRepair, /error_class = 'DETERMINISTIC_STAGE_ERROR'[\s\S]*?'CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'/i);
});

test('certificate facts come from exact sealed Workbench owners without zero defaults or case-only joins', () => {
  assert.match(build, /private\.pay_current_timesheet_entitlement_components_from_build_v1\(\s*v_economic_build_id,NULL\s*\)/i);
  assert.match(build, /certified_preview_publication_attestation_json->>'economic_build_id'/i);
  assert.match(build, /build_row\.status='COMPLETE'/i);
  assert.match(build, /ACTIVE_SETTLED_COMPONENT_BASELINE_APPLIED/i);
  assert.doesNotMatch(build, /v_json->>'prior_paid_amount_ex_vat'/i);
  assert.match(build, /fact\.fact_family='RESERVATION_COMPONENT'/i);
  assert.match(build, /fact\.build_id=v_economic_build_id/i);
  assert.match(build, /fact\.candidate_id=v_preview\.candidate_id/i);
  assert.match(build, /fact\.timesheet_id IS NOT DISTINCT FROM v_timesheet_id/i);
  assert.match(build, /fact\.economic_key_type\)=v_key_type/i);
  assert.match(build, /fact\.economic_key_value=v_key_value/i);
  assert.match(build, /fact\.finance_component_id=\(v_json->>'finance_component_id'\)::uuid/i);
  assert.doesNotMatch(build, /FROM public\.pay_advance_reservations reservation[\s\S]{0,300}reservation\.finance_case_id/i);
  assert.match(build, /banking_pay_workbench_settled_certificate_superseded_sources_v8/i);
  assert.match(build, /old_source\.status\)\)='SUPERSEDED'/i);
});

test('certificate eligibility admits only producer-certified visible finance constituents', () => {
  const start = build.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_preview_contract_v8');
  const end = build.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_constituent_seed_v8', start);
  assert.ok(start >= 0 && end > start);
  const previewContract = build.slice(start, end);
  const financeStart = previewContract.indexOf('v_certified_finance :=');
  const eligibilityStart = previewContract.indexOf('v_eligible :=', financeStart);
  assert.ok(financeStart >= 0 && eligibilityStart > financeStart);
  const finance = previewContract.slice(financeStart, eligibilityStart);
  for (const visibleAlias of [
    'OVERPAYMENT_RECOVERY',
    'MANUAL_DEBT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
  ]) assert.match(finance, new RegExp(`'${visibleAlias}'`, 'i'));
  assert.doesNotMatch(finance, /'LOAN_REPAYMENT'|'MANUAL_CREDIT_PAYOUT'/i);
  assert.match(finance, /v_timesheet_id IS NULL/i);
  assert.match(finance, /v_finance_case_id IS NOT NULL/i);
  assert.match(finance, /v_finance_component_id IS NOT NULL/i);
  assert.match(finance, /v_key_type IN \('CASE_TOTAL','FINANCE_COMPONENT'\)/i);
  assert.match(finance, /source_function[^\n]*pay_workbench_candidate_source_build_chunk/i);
  assert.match(finance, /dependency_family_key[^\n]*'finance:' \|\| v_finance_case_id::text/i);
  assert.match(finance, /source_ref[^\n]*'advance:' \|\| v_finance_case_id::text/i);
  assert.match(finance, /policy_x_authority_scope[^\n]*PRE_DRAFT_LIVE_TRUTH/i);
  assert.match(finance, /jsonb_array_elements\(v_json->'case_components'\)/i);
  assert.match(finance, /component\.value->>'finance_component_id' = v_finance_component_id::text/i);
  assert.match(previewContract, /OR v_certified_finance/i);
  assert.match(previewContract, /'certified_finance', v_certified_finance/i);
  const componentStart = build.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_component_seed_v8');
  const componentEnd = build.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_build_start_v8', componentStart);
  const componentSeed = build.slice(componentStart, componentEnd);
  assert.match(componentSeed, /'CASE_TOTAL','FINANCE_COMPONENT'/i);
  assert.match(componentSeed, /v_component_fallback = 'WORKED_TIME_AMOUNT'/i);
});

test('Ready Workbench sessions automatically advance the bounded certificate producer', () => {
  assert.match(broker, /async function advanceBankingPayWorkbenchSettledCertificateV8\(/i);
  assert.match(broker, /p_requested_limit:\s*256/i);
  assert.match(broker, /maximum_requested_page_size:\s*256/i);
  assert.match(broker, /maximum_build_emission_rows:\s*64/i);
  assert.match(broker, /maxAppendPages:\s*8/i);
  assert.match(broker, /maxRuntimeMs:\s*20000/i);
  assert.match(broker, /ctx\.waitUntil\(certificatePromise\)/i);
  assert.match(broker, /async function advanceReadyBankingPayWorkbenchCertificatesV8\(/i);
  assert.match(broker, /pay_workbench_settled_certificate_due_claim_v8/i);
  assert.match(broker, /leaseOwner:\s*row\?\.lease_owner/i);
  assert.doesNotMatch(broker.slice(
    broker.indexOf('async function advanceReadyBankingPayWorkbenchCertificatesV8'),
    broker.indexOf('async function bankingPayWorkbenchCronTick')
  ), /banking_pay_workbench_sessions\?/i);
  assert.match(broker, /settled_certificate_producer_result:\s*certificateProducerResult/i);
  assert.match(broker, /settled_certificate_producer:\s*certificateProducer/i);
  assert.doesNotMatch(broker, /WORKBENCH_SETTLED_CERTIFICATE[\s\S]{0,200}(?:payment_provider|settlement|remittance)/i);
});

test('Workbench display readiness stays visible while exact certificate readiness gates Create Draft', () => {
  assert.match(build, /CREATE OR REPLACE FUNCTION public\.pay_workbench_settled_certificate_status_v8\(\s*p_workbench_session_id uuid,\s*p_actor_user_id uuid/i);
  assert.match(build, /\bSTABLE\b[\s\S]*?SECURITY DEFINER[\s\S]*?SET search_path = ''/i);
  assert.match(build, /statement_timeout', '6000'/i);
  assert.match(build, /lock_timeout', '1000'/i);
  assert.match(build, /candidate\.session_version = session\.version/i);
  assert.match(build, /candidate\.progress_counter_version = session\.progress_counter_version/i);
  assert.match(build, /candidate\.authority_fence_generation = session\.authority_fence_generation/i);
  assert.match(build, /'certificate_ready_for_draft', v_certificate_ready/i);
  assert.match(build, /v_recovery_required := v_session_current[\s\S]*OR v_lifecycle = 'SESSION_NOT_CURRENT_READY'/i);
  assert.match(build, /REVOKE ALL ON FUNCTION public\.pay_workbench_settled_certificate_status_v8\(uuid,uuid\) FROM PUBLIC, anon, authenticated/i);
  assert.match(build, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_settled_certificate_status_v8\(uuid,uuid\) TO postgres, service_role/i);
  const statusFunction = build.slice(
    build.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_status_v8'),
    build.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_due_claim_v8')
  );
  assert.doesNotMatch(statusFunction, /FOR (?:UPDATE|SHARE)|jsonb_agg|array_agg|canonical_preview_lines|selected_preview_row_ids/i);

  assert.match(broker, /async function readBankingPayWorkbenchSettledCertificateStatusV8\(/i);
  assert.match(broker, /pay_workbench_settled_certificate_status_v8/i);
  assert.match(broker, /timeoutMs:\s*6000/i);
  assert.match(broker, /const certificateReadyForDraft = workbenchDisplayReady[\s\S]*certificateStatus\.certificate_lifecycle === 'SEALED_CURRENT'/i);
  assert.match(broker, /progressPayload\.display_ready = workbenchDisplayReady/i);
  assert.match(broker, /progressPayload\.ready_for_draft = certificateReadyForDraft/i);
  assert.match(broker, /const certificateDraftBlocker = !workbenchDisplayReady/i);
  assert.match(broker, /if \(workbenchDisplayReady && certificateBuildPending\)/i);
  assert.match(broker, /CREATE_DRAFT_CERTIFICATE_STATUS_V8/i);
  assert.match(broker, /WORKBENCH_SETTLED_CERTIFICATE_NOT_READY/i);
});

test('server sweep can finish a claimed certificate after the browser has stopped polling', async () => {
  const start = broker.indexOf('async function advanceBankingPayWorkbenchSettledCertificateV8');
  const end = broker.indexOf('async function bankingPayWorkbenchCronTick', start);
  assert.ok(start >= 0 && end > start);
  const calls = [];
  const sessionId = '10000000-0000-4000-8000-000000000001';
  const actorUserId = '10000000-0000-4000-8000-000000000002';
  const certificateUuid = '10000000-0000-4000-8000-000000000003';
  const leaseOwner = 'WORKBENCH_CERTIFICATE_SWEEP_V8:10000000-0000-4000-8000-000000000004';
  const digest = 'a'.repeat(64);
  const sbRpc = async (_env, name, args) => {
    calls.push({ name, args });
    if (name === 'pay_workbench_settled_certificate_due_claim_v8') {
      return { ok: true, claims: [{
        session_id: sessionId,
        actor_user_id: actorUserId,
        certificate_uuid: certificateUuid,
        lease_owner: leaseOwner,
        lifecycle: 'BUILDING'
      }] };
    }
    if (name === 'pay_workbench_settled_certificate_build_start_v8') {
      return { ok: true, replayed: true, lifecycle: 'BUILDING', certificate_uuid: certificateUuid,
        append_complete: false, next_after_ordinal: null };
    }
    if (name === 'pay_workbench_settled_certificate_build_append_page_v8') {
      return { ok: true, replayed: false, has_more: false, next_after_ordinal: 0,
        page_receipt_sha256: digest };
    }
    if (name === 'pay_workbench_settled_certificate_seal_v8') {
      return { ok: true, sealed: true, lifecycle: 'SEALED_CURRENT',
        certification_id: `WORKBENCH_SETTLED_CERTIFICATION_V2:${digest}`,
        overall_digest_sha256: digest };
    }
    throw new Error(`unexpected RPC ${name}`);
  };
  const context = vm.createContext({ sbRpc });
  vm.runInContext(`${broker.slice(start, end)}\nglobalThis.runSweep = advanceReadyBankingPayWorkbenchCertificatesV8;`, context);
  const result = await context.runSweep({}, { limit: 1 });
  assert.equal(result.ok, true);
  assert.equal(result.completed, 1);
  assert.deepEqual(calls.map(call => call.name), [
    'pay_workbench_settled_certificate_due_claim_v8',
    'pay_workbench_settled_certificate_build_start_v8',
    'pay_workbench_settled_certificate_build_append_page_v8',
    'pay_workbench_settled_certificate_seal_v8'
  ]);
  assert.equal(calls[2].args.p_lease_owner, leaseOwner);
  assert.equal(calls[2].args.p_requested_limit, 256);
  assert.equal(result.results[0].maximum_requested_page_size, 256);
  assert.equal(result.results[0].maximum_build_emission_rows, 64);
});

test('due claim skips sealed and terminal generations while prioritising resumable builds', () => {
  assert.match(build, /CREATE OR REPLACE FUNCTION public\.pay_workbench_settled_certificate_due_claim_v8\(\s*p_limit integer/i);
  assert.match(build, /SET search_path = ''/i);
  assert.match(build, /statement_timeout', '15000'/i);
  assert.match(build, /lock_timeout', '1500'/i);
  assert.match(build, /active\.lifecycle = 'BUILDING'[\s\S]*?lease_expires_at_utc/i);
  assert.match(build, /active\.certificate_uuid IS NULL[\s\S]*?NOT EXISTS[\s\S]*?historical\.authority_fence_generation = session\.authority_fence_generation/i);
  assert.match(build, /ORDER BY CASE WHEN active\.lifecycle = 'BUILDING' THEN 0 ELSE 1 END/i);
  assert.match(build, /FOR UPDATE OF session SKIP LOCKED/i);
  assert.match(build, /WORKBENCH_SETTLED_CERTIFICATE_V8:' \|\| v_due\.session_id::text/i);
  assert.match(build, /WORKBENCH_CERTIFICATE_SWEEP_V8:' \|\| pg_catalog\.gen_random_uuid\(\)::text/i);
  assert.match(build, /exact_current_sealed_skipped', true/i);
  assert.match(build, /exact_historical_terminal_skipped', true/i);
  assert.doesNotMatch(build.slice(
    build.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_due_claim_v8'),
    build.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_lifecycle_v8')
  ), /FOR (?:UPDATE|SHARE)[\s\S]{0,80}(?:candidate|publication)/i);
});

test('unfinished certificates use small durable queue successors without changing page or timeout budgets', () => {
  assert.match(broker, /function parseBankingPayWorkbenchCertificateContinuationV8\(/i);
  assert.match(broker, /WORKBENCH_SETTLED_CERTIFICATE_CONTINUATION_V8/i);
  assert.match(broker, /continuationSequence\s*>\s*1024/i);
  assert.match(broker, /50,000 \/ 64 is 782 pages/i);
  assert.match(broker, /BANKING_PAY_CONTINUATION_QUEUE\.send\(message, \{ delaySeconds: 1 \}\)/i);
  assert.match(broker, /continuationSequence:\s*certificateWake\.continuation_sequence/i);
  assert.match(broker, /maxAppendPages:\s*8/i);
  assert.match(broker, /maxSealSteps:\s*4/i);
  assert.match(broker, /maxRuntimeMs:\s*20000/i);
  assert.match(broker, /successor_enqueued:\s*certificateResult\.durable_continuation\?\.enqueued === true/i);
  assert.match(broker, /message\.ack\(\)[\s\S]{0,80}return/i);
  assert.doesNotMatch(broker, /WORKBENCH_CERTIFICATE[\s\S]{0,200}(?:statement_timeout[^\n]*[2-9][0-9]{4}|lock_timeout[^\n]*[2-9][0-9]{3})/i);
});

test('the service-owned issuer derives an exact nine-key reference plus bounded policy-preserving scope facts', () => {
  const issuerStart = readers.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_reference_issue_v8');
  const admitStart = readers.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_operation_admit_v8');
  assert.ok(issuerStart >= 0 && admitStart > issuerStart);
  const issuer = readers.slice(issuerStart, admitStart);
  assert.match(issuer, /COUNT\(\*\) FROM pg_catalog\.jsonb_object_keys\(p_same_week_paye_override\)\)<>11/i);
  assert.match(issuer, /banking_pay_workbench_settled_certificate_channel_manifests_v8/i);
  assert.match(issuer, /banking_pay_workbench_settled_cert_filter_scope_manifest_v8/i);
  assert.match(issuer, /'manifest_digest_sha256',v_channel\.manifest_digest_sha256/i);
  assert.match(issuer, /'filter_context_digest_sha256',v_filter\.filter_context_digest_sha256/i);
  assert.doesNotMatch(issuer, /selected_preview_row_ids|canonical_preview_lines|jsonb_agg|array_agg/i);
  assert.match(readers, /CREATE OR REPLACE FUNCTION public\.pay_workbench_settled_certificate_current_reference_issue_v8\([\s\S]*?p_progress_counter_version bigint/i);
  assert.match(issuer, /'certificate_reference',v_certificate_reference/i);
  assert.match(issuer, /'pre_admission_scope_facts',pg_catalog\.jsonb_build_object/i);
  assert.match(issuer, /'certificate_uuid',v_header\.certificate_uuid/i);
  assert.match(issuer, /'workbench_session_id',v_header\.workbench_session_id/i);
  assert.match(issuer, /'selected_ready_total',v_all_manifest\.constituent_count/i);
  assert.match(issuer, /'selected_ready_for_request',v_requested_count/i);
  assert.match(issuer, /'selected_ready_paye',v_paye_manifest\.constituent_count/i);
  assert.match(issuer, /'selected_ready_umbrella',v_umbrella_manifest\.constituent_count/i);
  assert.match(issuer, /'pay_week_start',v_pay_week_start::text/i);
  assert.match(issuer, /public\._pay_week_start_monday\(v_header\.pay_date\)/i);
  assert.match(issuer, /certificate\.workbench_session_id=p_workbench_session_id[\s\S]*?certificate\.authority_fence_generation=v_session\.authority_fence_generation[\s\S]*?certificate\.lifecycle='SEALED_CURRENT'/i);
  assert.match(issuer, /WORKBENCH_CERTIFICATE_MULTIPLE_CURRENT/i);
  assert.match(readers, /REVOKE ALL ON FUNCTION public\.pay_workbench_settled_certificate_reference_issue_v8\(text,text,text,jsonb\) FROM PUBLIC, anon, authenticated, service_role/i);
  assert.match(readers, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_settled_certificate_reference_issue_v8\(text,text,text,jsonb\) TO postgres/i);
  assert.match(readers, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_settled_certificate_current_reference_issue_v8\(uuid,bigint,bigint,text,text,jsonb\) TO postgres, service_role/i);
  assert.match(readers, /private\.workbench_settled_cert_same_week_override_validate_v8\(jsonb\)/i);
  assert.doesNotMatch(readers, /pay_workbench_settled_certificate_same_week_override_validate_v8/i);
  assert.match(migration, /CREATE UNIQUE INDEX banking_pay_wb_cert_v8_exact_active_authority_uq[\s\S]*?WHERE lifecycle IN \('BUILDING','SEALED_CURRENT'\)/i);
  assert.doesNotMatch(issuer, /pay_batch_items|banking_pay_operation_candidate_scope|payment_provider|settlement|remittance/i);
});

test('certified start atomically creates the operation then admits its exact stored reference', () => {
  const operationCall = certifiedStart.indexOf('FROM public.banking_pay_operation_start(');
  const contextCheck = certifiedStart.indexOf('WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH');
  const admissionCall = certifiedStart.indexOf('private.pay_workbench_settled_certificate_operation_admit_v8(');
  assert.ok(operationCall >= 0 && contextCheck > operationCall && admissionCall > contextCheck);
  assert.match(certifiedStart, /SET statement_timeout = '6000ms'/i);
  assert.match(certifiedStart, /SET lock_timeout = '1000ms'/i);
  assert.match(certifiedStart, /workbench_settled_certificate_reference_v8/i);
  assert.match(certifiedStart, /REVOKE ALL ON FUNCTION public\.banking_pay_draft_certified_operation_start_v8\(jsonb,uuid,text\) FROM authenticated/i);
  assert.match(certifiedStart, /GRANT EXECUTE ON FUNCTION public\.banking_pay_draft_certified_operation_start_v8\(jsonb,uuid,text\) TO service_role/i);
  assert.doesNotMatch(certifiedStart, /pay_batch_items|payment_provider|settlement|remittance/i);
});

test('admission is owner-only internally and returns the FK identity with STAGING state', () => {
  assert.match(readers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_settled_certificate_operation_admit_v8/i);
  assert.match(readers, /Supported owner-internal join for the H2 database-owned initializer/i);
  assert.match(readers, /persisted operation-link state column is link_state/i);
  assert.match(readers, /link_state='STAGING' to link_state='FROZEN'/i);
  assert.match(readers, /already-FROZEN row is replay and every other identity\/state fails closed/i);
  assert.match(readers, /freeze_state is only the external readback alias/i);
  assert.match(readers, /'contract','WORKBENCH_SETTLED_CERTIFICATE_OPERATION_ADMISSION_V8'/i);
  assert.match(readers, /'certificate_uuid',v_header\.certificate_uuid/i);
  assert.match(readers, /'lifecycle',v_header\.lifecycle,'freeze_state','STAGING'/i);
  assert.match(readers, /'STAGING',v_candidate_filter_id,v_client_filter_id/i);
  assert.match(readers, /v_compact_operation_projection:=pg_catalog\.jsonb_build_object\(\s*'pay_channel_scope',v_pay_channel_scope,\s*'draft_scope',v_pay_channel_scope,\s*'same_week_paye_override',v_override/i);
  assert.match(readers, /input_json=COALESCE\(operation\.input_json,'\{\}'::jsonb\)\|\|v_compact_operation_projection/i);
  assert.match(readers, /v_operation\.input_json->>'pay_channel_scope' IS DISTINCT FROM v_pay_channel_scope/i);
  assert.match(readers, /v_operation\.input_json->>'draft_scope' IS DISTINCT FROM v_pay_channel_scope/i);
  assert.match(readers, /v_operation\.input_json->'same_week_paye_override' IS DISTINCT FROM v_override/i);
  assert.match(readers, /WORKBENCH_SETTLED_CERTIFICATE_OPERATION_PROJECTION_V1/i);
  assert.doesNotMatch(readers, /v_compact_operation_projection[\s\S]{0,300}(?:selected_preview_row_ids|canonical_preview_lines|jsonb_agg|array_agg)/i);
  assert.match(readers, /REVOKE ALL ON FUNCTION private\.pay_workbench_settled_certificate_operation_admit_v8\(uuid\) FROM PUBLIC, anon, authenticated, service_role/i);
  assert.doesNotMatch(readers, /GRANT EXECUTE ON FUNCTION private\.pay_workbench_settled_certificate_operation_admit_v8\(uuid\) TO service_role/i);
});

test('partition pages distinguish member-stream totals from partition totals and digest authority', () => {
  const start = readers.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_partition_page_v8');
  const end = readers.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_component_page_v8', start);
  assert.ok(start >= 0 && end > start);
  const partition = readers.slice(start, end);
  assert.match(partition, /'member_stream_total_count',v_manifest\.constituent_count/i);
  assert.match(partition, /'partition_count',v_manifest\.partition_count/i);
  assert.match(partition, /'selected_partitions_digest_sha256',v_manifest\.selected_partitions_digest_sha256/i);
  for (const field of [
    'stream_ordinal', 'partition_ordinal', 'member_ordinal', 'constituent_ordinal',
    'stable_identity_digest_sha256', 'candidate_id', 'resolved_pay_channel',
    'constituent_count', 'canonical_amount_ex_vat_total', 'partition_digest_sha256',
  ]) assert.match(partition, new RegExp(`member\\.${field}|partition\\.${field}`, 'i'));
});

test('page receipts hash compact row authorities instead of re-hashing a maximum response', () => {
  assert.equal((readers.match(/'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_V2'/g) || []).length, 3);
  assert.equal((readers.match(/'page_receipt',v_receipt_core/g) || []).length, 3);
  assert.equal((readers.match(/stable_stringify_v8\(v_receipt_core\)/g) || []).length, 3);
  assert.doesNotMatch(readers, /stable_stringify_v8\(v_response_core\)/i);
  assert.match(readers, /'constituent_digest_sha256',v_row#>>'\{constituent,constituent_digest_sha256\}'/i);
  assert.match(readers, /'partition_digest_sha256',v_member\.partition_digest_sha256/i);
  assert.match(readers, /'expected_digest_sha256',v_row->>'expected_digest_sha256'/i);
  assert.match(readers, /v_phase='CONSTITUENTS'[\s\S]*?LIMIT LEAST\(p_limit,64\)/i);
  assert.match(readers, /v_byte_count\+pg_catalog\.octet_length\(v_piece\)>65536/i);
  assert.match(readers, /extensions\.digest\(pg_catalog\.convert_to\(COALESCE\(p_text, ''\), 'UTF8'\), 'sha256'\)/i);
  assert.match(build, /LIMIT LEAST\(p_requested_limit,64\) \+ 1/i);
  assert.match(build, /LIMIT LEAST\(p_requested_limit,64\)/i);
});

test('every H1 V8 relation name is explicit and within PostgreSQL identifier limits', () => {
  const names = [...migration.matchAll(/CREATE TABLE\s+"private"\."([^"]+)"/g)].map(match => match[1]);
  assert.equal(names.length, 21);
  assert.equal(new Set(names).size, names.length);
  for (const name of names) assert.ok(Buffer.byteLength(name, 'utf8') <= 63, name);
  assert.match(migration, /banking_pay_workbench_settled_cert_source_reservations_v8/i);
  assert.match(migration, /banking_pay_workbench_settled_cert_filter_scope_manifest_v8/i);
  assert.match(migration, /source_row_digest_sha256" text NULL CHECK/i);
  assert.match(migration, /banking_pay_wb_preview_cert_v8_universe_page_idx[\s\S]*?session_id, session_version, row_ordinal, id/i);
  assert.equal((migration.match(/'CASE_TOTAL','FINANCE_COMPONENT'/g) || []).length, 0);
  assert.equal((financeKeyMigration.match(/'CASE_TOTAL',\s*'FINANCE_COMPONENT'/g) || []).length, 2);
  assert.match(build, /stream_kind='UNIVERSE_CAPTURE'/i);
});

test('operation links bind admission, channel, filters and exact same-week proof', () => {
  for (const column of [
    'admission_request_digest_sha256',
    'channel_manifest_digest_sha256',
    'same_week_paye_override_used',
    'same_week_paye_override_pay_date',
    'same_week_paye_override_pay_week_start',
    'same_week_paye_override_pay_week_end',
    'same_week_paye_override_digest_sha256',
  ]) assert.match(migration, new RegExp(`"${column}"`, 'i'));
  assert.match(migration, /"link_state" text NOT NULL CHECK \(link_state IN \('ADMITTED','STAGING','FROZEN','TERMINAL_FAILED','TERMINAL_COMPLETE'\)\)/i);
  assert.match(migration, /FOREIGN KEY \(operation_id\) REFERENCES public\.banking_pay_operations\(id\)/i);
});

test('certificate pages are staging-only and cannot be reread after the H2 frozen handoff', () => {
  assert.match(readers, /pre-freeze staging readers/i);
  assert.match(readers, /once H2 atomically records[\s\S]*?link_state FROZEN,[\s\S]*?no further certificate paging or filter reads are allowed/i);
  const stagingFence = /IF v_link\.link_state NOT IN \('ADMITTED','STAGING'\) OR v_header\.lifecycle<>'SEALED_CURRENT' THEN/g;
  assert.equal([...readers.matchAll(stagingFence)].length, 4);
  assert.doesNotMatch(readers, /v_link\.freeze_state|link\.freeze_state/i);
});

test('certificate implementation preserves fixed budgets, one session fence and PostgreSQL first-use syntax safety', () => {
  assert.match(operationStart, /SET statement_timeout TO '6000ms'/i);
  assert.match(operationStart, /SET lock_timeout TO '1000ms'/i);
  assert.doesNotMatch(operationStart, /set_config\('lock_timeout','3000'/i);
  assert.doesNotMatch(operationStart, /set_config\('lock_timeout',\s*'3s'/i);
  assert.match(operationStartClosure, /\\ir 04082026_1154_banking_pay_operation_start\.sql/i);
  assert.doesNotMatch(operationStartClosure, /CREATE OR REPLACE FUNCTION/i);
  assert.match(fence, /authority_fence_generation/i);
  assert.doesNotMatch(`${migration}\n${build}\n${readers}\n${certifiedStart}\n${fence}\n${operationStart}\n${publisher}`,
    /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('the release seals the exact H1 catalogue and rollback-contained first-use proofs', () => {
  assert.equal(catalogManifest.project_ref, 'provider-neutral');
  assert.equal(catalogManifest.function_count, 39);
  assert.equal(catalogManifest.functions.length, 39);
  assert.equal(new Set(catalogManifest.functions.map(item =>
    `${item.schema}.${item.name}(${item.identity_arguments})`)).size, 39);
  assert.match(catalogVerifier, /logicalOwner/);
  assert.match(catalogVerifier, /H1 V8 catalogue verified/);
  assert.match(preapplyGenerator, /banking_pay_workbench_settled_certificate_v8_catalog_manifest\.json/);
  assert.match(sourceWorkflow, /verify_banking_pay_workbench_settled_certificate_v8_catalog\.mjs/);
  for (const proof of [
    'supabase/verification/02092026_2390_banking_pay_workbench_settled_certificate_v8_verification.sql',
    'tests/02092026_2392_banking_pay_workbench_settled_certificate_v8_runtime_verification.sql',
    'tests/03092026_1000_banking_pay_workbench_targeted_zero_output_runtime_verification.sql',
  ]) {
    assert.equal(release.verificationFiles.filter(file => file === proof).length, 1, proof);
    assert.equal(release.newVerificationFiles.filter(file => file === proof).length, 1, proof);
  }
});

test('managed-upgrade certificate proof is isolated from unrelated Workbench history without weakening the gate', () => {
  assert.match(runtimeVerification,
    /v_scope_generation\s*:=\s*public\.pay_workbench_scope_current_generation_v1\(\)/i);
  assert.match(runtimeVerification,
    /pg_catalog\.jsonb_build_object\('candidate_id',v_candidate_id::text\)/i);
  assert.match(runtimeVerification,
    /ARRAY\[v_candidate_id\],[\s\S]{0,120}v_scope_generation,v_scope_generation,v_scope_generation/i);
  assert.match(runtimeVerification,
    /v_economic_build_id,v_candidate_id,v_session_id,1,v_snapshot_id,v_source_build_run_id,[\s\S]{0,80}v_scope_generation,1,'COMPLETE','COMPLETE'/i);
  assert.doesNotMatch(runtimeVerification,
    /DATE '2099-04-03',DATE '2099-03-29','\{\}'::jsonb,v_prefix/i);
  assert.match(build, /v_gate\s*:=\s*private\.pay_workbench_modal_draft_gate_v2/i);
  assert.match(build, /WORKBENCH_CERTIFICATE_DRAFT_GATE_REJECTED/i);
});

test('the Workbench session-open catalogue owner contains the sole exact current definition', () => {
  const entry = catalogManifest.functions.find(item =>
    item.schema === 'public'
    && item.name === 'pay_workbench_session_open'
    && item.identity_arguments.startsWith('p_actor_user_id uuid,'));
  assert.ok(entry, 'missing pay_workbench_session_open catalogue entry');
  assert.deepEqual(entry.source_files, [
    'supabase/repeatable/05092026_1652_banking_pay_workbench_session_open_current_owner_v8.sql',
  ]);
  const owner = read(entry.source_files[0]);
  assert.equal((owner.match(/CREATE OR REPLACE FUNCTION public\.pay_workbench_session_open\(/g) || []).length, 1);
  assert.match(owner, /\\set ON_ERROR_STOP on[\s\S]*begin;[\s\S]*Exact current-owner marker only; established session-open decisions are unchanged\.[\s\S]*commit;/i);
  assert.match(owner, /p_refresh_job_ids jsonb DEFAULT '\[\]'::jsonb\)\s*\n RETURNS jsonb/i);
  assert.match(owner, /SECURITY DEFINER\s*\n SET search_path TO 'public'/i);
  assert.match(owner, /ALTER FUNCTION public\.pay_workbench_session_open\(uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb\) OWNER TO postgres/i);
  assert.match(owner, /REVOKE ALL ON FUNCTION public\.pay_workbench_session_open\(uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb\) FROM PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin/i);
  assert.match(owner, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_open\(uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb\) TO postgres/i);
});
