const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const dirtyRuntime = fs.readFileSync(
  path.join(root, 'supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql'),
  'utf8',
);
const repair = fs.readFileSync(
  path.join(root, 'supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql'),
  'utf8',
);
const verification = fs.readFileSync(
  path.join(root, 'supabase/verification/31082026_0014_banking_pay_dirty_apply_family_authority_repair_verification.sql'),
  'utf8',
);
const worker = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');

test('dirty apply proves the final family scope and reissues one canonical authority before session fan-out', () => {
  const start = dirtyRuntime.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(',
  );
  assert.notEqual(start, -1);
  const body = dirtyRuntime.slice(start);

  const familyNormalisation = body.indexOf(
    'public._pay_workbench_normalise_timesheet_rotation_scope_payload',
  );
  const proof = body.indexOf('v_preceding_scope_authority_reusable :=', familyNormalisation);
  const reissue = body.indexOf('private.pay_workbench_scope_invalidate_v1(', proof);
  const sessionScan = body.indexOf('FOR v_session_row IN', reissue);
  const canonicalEnqueue = body.indexOf('public.pay_workbench_enqueue_candidate_refresh(', sessionScan);

  assert.ok(familyNormalisation > -1);
  assert.ok(proof > familyNormalisation);
  assert.ok(reissue > proof);
  assert.ok(sessionScan > reissue);
  assert.ok(canonicalEnqueue > sessionScan);
  assert.match(body.slice(familyNormalisation, reissue), /banking_pay_scope_change_transactions/i);
  assert.match(body.slice(familyNormalisation, reissue), /banking_pay_workbench_timesheet_scope_state/i);
  assert.match(body.slice(familyNormalisation, reissue), /scope_change_generation/i);
  assert.match(body.slice(reissue, sessionScan), /skip_candidate_job_enqueue['"\s,:]+true/i);
  assert.match(body.slice(reissue, sessionScan), /PAY_WORKBENCH_EFFECTIVE_SCOPE_REISSUE_NOT_STAGED/i);
  assert.match(body.slice(reissue, sessionScan), /public\._change_bump\('pay_candidate:'/i);
  assert.match(body.slice(reissue, sessionScan), /preinvalidated_scope_reissue_pending_finalization/i);
  assert.match(body.slice(reissue, sessionScan), /SET status='QUEUED'/i);
  assert.doesNotMatch(body.slice(reissue, sessionScan), /SET CONSTRAINTS/i);
  assert.match(body, /preinvalidated_scope_reissued/i);
});

test('repair targets only the two proved terminal dirty-apply failures and retains DEAD history', () => {
  const closureFiles = [
    '07082026_1016_banking_pay_targeted_delta_runtime.sql',
    '28082026_1424_banking_pay_modal_selection_owner_bridge.sql',
    '29082026_0613_banking_pay_replaced_candidate_owner_repair_v1.sql',
    '30082026_1232_candidate_qr_document_revision_order_v1.sql',
    '29082026_0326_banking_pay_release_authority_repair_v1.sql',
  ];
  let previousIndex = -1;
  for (const file of closureFiles) {
    const index = repair.indexOf(`\\ir ${file}`);
    assert.ok(index > previousIndex, `missing or misordered final authority closure: ${file}`);
    previousIndex = index;
  }
  assert.match(repair, /CREATE OR REPLACE FUNCTION public\.pay_workbench_repair_invalid_dirty_apply_jobs_v1\(/i);
  assert.match(repair, /PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED/i);
  assert.match(repair, /PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT/i);
  assert.match(repair, /invalid_job\.status='DEAD'/i);
  assert.match(repair, /successful_job\.status='SUCCEEDED'/i);
  assert.match(repair, /pg_try_advisory_xact_lock/i);
  assert.match(repair, /private\.pay_workbench_scope_invalidate_v1\(/i);
  assert.match(repair, /ARRAY\[NULL::uuid\]/i);
  assert.match(repair, /successor_job\.status IN \('QUEUED','RUNNING'\)/i);
  assert.match(repair, /v_successor_tx_state IS DISTINCT FROM 'PENDING'/i);
  assert.match(repair, /v_registry_tx_token IS DISTINCT FROM v_successor\.scope_change_tx_token/i);
  assert.match(repair, /successor_finalization_staged/i);
  assert.match(repair, /PAY_WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_POSTCONDITION_FAILED/i);
  assert.doesNotMatch(repair, /UPDATE public\.banking_pay_workbench_jobs AS invalid_job[\s\S]*SET status/i);
  assert.match(repair, /FROM PUBLIC,anon,authenticated/i);
  assert.match(repair, /TO postgres,service_role/i);
  const restoredServiceOnlyAuthorities = [
    'public._pay_active_settled_components(uuid[])',
    'public.bulk_authorise_dataset_v1(jsonb)',
    'public.bulk_authorise_row_context_v1(jsonb)',
    'public.bulk_process_dataset_v1(jsonb)',
    'public.bulk_process_row_context_v1(jsonb)',
    'public.bulk_timesheet_row_patch_v1(jsonb)',
    'public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb)',
    'public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)',
    'public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid)',
    'public.pay_timesheet_summary_pay_state_refresh_trigger()',
    'public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer)',
    'public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer)',
    'public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)',
    'public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer)',
    'public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamptz,text)',
    'public.pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb)',
    'public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb)',
    'public.pay_workbench_session_set_selected_rows(uuid,jsonb,uuid)',
    'public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer)',
    'public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz)',
    'public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)',
    'public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text)',
    'public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)',
    'public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)',
  ];
  for (const identity of restoredServiceOnlyAuthorities) {
    assert.ok(
      repair.includes(`REVOKE ALL ON FUNCTION ${identity} FROM PUBLIC,anon,authenticated,service_role;`),
      `missing exact browser-role revoke for ${identity}`,
    );
    assert.ok(
      repair.includes(`GRANT EXECUTE ON FUNCTION ${identity} TO service_role;`),
      `missing exact service-role grant for ${identity}`,
    );
    assert.ok(
      verification.includes(`'${identity}'::regprocedure`),
      `missing exact installed ACL verification for ${identity}`,
    );
  }
  for (const hash of [
    '89543b82378468b1ae43534f5a4b1a200ffc60ffbef76196398b7f7d6521792f',
    '363aeab20aed70b8396793808f9a2263766e984d66914317bdf0a767e6e0f360',
    '7d622194f7bca877bf8420cb6f10f9ad46a69bad118c5f8fb9ed16810492d98c',
    '090fcbd7a66ade81f107635c360a038a514a5c26358c0b4aa716bdea91245347',
  ]) assert.match(verification, new RegExp(hash));
  assert.match(verification, /BANKING_PAY_FINAL_AUTHORITY_CLOSURE_MISMATCH/);
});

test('Workbench drain runs the bounded dirty-apply repair before ordinary claims', () => {
  const drainStart = worker.indexOf('async function drainBankingPayWorkbenchJobs');
  assert.notEqual(drainStart, -1);
  const repairCall = worker.indexOf("'pay_workbench_repair_invalid_dirty_apply_jobs_v1'", drainStart);
  const firstClaim = worker.indexOf("'pay_workbench_worker_drain_chunk_revalidated_v1'", repairCall);
  assert.ok(repairCall > drainStart);
  assert.ok(firstClaim > repairCall);
  assert.match(worker.slice(repairCall, firstClaim), /all_state_transitions_proven/i);
  assert.match(worker.slice(repairCall, firstClaim), /remaining_invalid_unrepaired_count/i);
  assert.match(worker.slice(repairCall, firstClaim), /WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_FAILED/i);
});
