import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, '..');

const sourcePaths = {
  monolith: path.join(repoRoot, 'supabase', 'repeatable', '26052026_2100HRS_NEW_FUNCTIONS.sql'),
  preBank: path.join(repoRoot, 'supabase', 'repeatable', '04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql'),
  noMoney: path.join(repoRoot, 'supabase', 'repeatable', '04082026_1158_pay_no_money_unwind_apply_work_item.sql'),
  selectionPrepare: path.join(repoRoot, 'supabase', 'repeatable', '05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql'),
  expandWork: path.join(repoRoot, 'supabase', 'repeatable', '04082026_1208_pay_payment_correction_expand_work.sql'),
  integrityCheck: path.join(repoRoot, 'supabase', 'repeatable', '04082026_1148_pay_payment_correction_integrity_check_v1.sql'),
  statusPage: path.join(repoRoot, 'supabase', 'repeatable', '04082026_1146_pay_batch_payment_status_page_v1.sql')
};

const expectedSourceHashes = {
  monolith: '8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f',
  preBank: '4a40fc4911382946dc5ba26d25d5963e552f01e9cada7280b2b4e69db427e43b',
  noMoney: '2934fff3ee503cbf4a8336d449944bcf6c125fb7de87c3a7cd88aafe2b73b8cc',
  selectionPrepare: '47d79ef8f50a6a313e2ac20d0ff9b6e4d6ec5ee6c99a7d0be376724564b10634',
  expandWork: '7d944938263dd27a77bc24632a48e2ff9283e01a004a74a3337e790711f2ad51',
  integrityCheck: '4c99dfb5c62225bd72cc92dd1fb5162668e581c58a5424f47e1b9392189fa7e4',
  statusPage: 'c0f38a0555482ad78021c33ecc398bbaf835f0ae30b3dce9bb396e33c74a7bee'
};

const outputPaths = {
  admission: path.join(repoRoot, 'supabase', 'repeatable', '07092026_2136_banking_pay_unpaid_cancellation_sourceless_admission_v1.sql'),
  apply: path.join(repoRoot, 'supabase', 'repeatable', '07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql'),
  alert: path.join(repoRoot, 'supabase', 'repeatable', '07092026_1933_banking_pay_unpaid_cancellation_investigation_alert_v1.sql'),
  communicationPrepare: path.join(repoRoot, 'supabase', 'repeatable', '07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql'),
  communicationExpand: path.join(repoRoot, 'supabase', 'repeatable', '07092026_2014_banking_pay_unpaid_cancellation_communication_v2_expand_v1.sql'),
  integrityCheck: path.join(repoRoot, 'supabase', 'repeatable', '07092026_2021_banking_pay_payment_correction_integrity_communication_v2_v1.sql'),
  statusAdmission: path.join(repoRoot, 'supabase', 'repeatable', '07092026_2135_banking_pay_unpaid_cancellation_status_admission_v1.sql')
};

const checkOnly = process.argv.includes('--check');

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

function lf(value) {
  return value.replaceAll('\r\n', '\n');
}

function occurrences(source, needle) {
  return source.split(needle).length - 1;
}

export function replaceExactly(source, before, after, label, expectedCount = 1) {
  assert.equal(occurrences(source, before), expectedCount, `${label}: exact source count changed`);
  // Use a replacement callback so SQL bytes such as the regex terminator `$'`
  // are preserved literally instead of being interpreted as JavaScript's
  // special "suffix after the match" replacement token.
  return source.replace(before, () => after);
}

export function replaceRangeExactly(source, startMarker, endMarker, replacement, label) {
  const startCount = occurrences(source, startMarker);
  assert.equal(
    startCount,
    1,
    `${label}: start marker count changed (first=${source.indexOf(startMarker)}, last=${source.lastIndexOf(startMarker)})`
  );
  assert.equal(occurrences(source, endMarker), 1, `${label}: end marker count changed`);
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start + startMarker.length);
  assert.ok(end > start, `${label}: invalid marker order`);
  return source.slice(0, start) + replacement + source.slice(end);
}

export function extractFunction(source, identity) {
  const marker = `CREATE OR REPLACE FUNCTION ${identity}(`;
  assert.equal(occurrences(source, marker), 1, `${identity}: definition owner count changed`);
  const start = source.indexOf(marker);
  const bodyStart = source.indexOf('AS $function$', start);
  assert.ok(bodyStart > start, `${identity}: body marker missing`);
  const endMarker = '\n$function$;';
  const end = source.indexOf(endMarker, bodyStart);
  assert.ok(end > bodyStart, `${identity}: terminator missing`);
  return source.slice(start, end + endMarker.length);
}

function wrapRepeatable(title, definitions, acl, preDefinitions = '') {
  return [
    `-- ${title}`,
    '-- Generated from exact current owners by scripts/generate-banking-pay-unpaid-cancellation-sourceless-v1.mjs.',
    '-- Financial, tax, VAT, payment-channel, provider, settlement and remittance policy remain unchanged.',
    '',
    '\\set ON_ERROR_STOP on',
    '',
    'begin;',
    '',
    ...(preDefinitions.trim() ? [preDefinitions.trim(), ''] : []),
    definitions.trim(),
    '',
    acl.trim(),
    '',
    'commit;',
    ''
  ].join('\n');
}

export function patchDiagnostic(definition) {
  let source = definition;
  source = replaceExactly(
    source,
    "  v_manual_carry_forward_blocker_count integer := 0;\n  v_freshness_blocker_count integer := 0;",
    "  v_manual_carry_forward_blocker_count integer := 0;\n  v_source_less_ambiguous_count integer := 0;\n  v_source_restoration_investigation_required boolean := false;\n  v_freshness_blocker_count integer := 0;",
    'diagnostic investigation declarations'
  );
  source = replaceExactly(
    source,
    "  v_manual_carry_forward_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb)), 0);\n  v_freshness_blocker_count :=",
    "  v_manual_carry_forward_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb)), 0);\n  v_source_less_ambiguous_count := COALESCE(NULLIF(v_manual_adjustment_result ->> 'source_less_ambiguous_count', '')::integer, 0);\n  v_freshness_blocker_count :=",
    'diagnostic detector count'
  );
  const oldBlock = `  IF v_manual_carry_forward_blocker_count > 0 OR v_can_carry_forward_automatically IS NOT TRUE THEN
    v_blockers := v_blockers || COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb);
  END IF;`;
  const newBlock = `  -- Source restoration remains truthful: ambiguous rows cannot be reconstructed.
  -- For a complete, proved-unpaid scope this is advisory to financial cancellation;
  -- the immutable correction evidence is surfaced for Banking investigation afterward.
  v_source_restoration_investigation_required :=
    v_source_less_ambiguous_count > 0
    AND v_scope_is_full
    AND v_scope_type IN ('BATCH', 'CANDIDATES')
    AND (v_is_local_not_sent OR v_has_terminal_no_money)
    AND v_has_paid_or_settled IS NOT TRUE
    AND v_has_provider_unknown IS NOT TRUE
    AND v_has_provider_pending IS NOT TRUE
    AND v_open_provider_submit_count = 0
    AND v_partial_scope_blocker_count = 0
    AND v_freshness_blocker_count = 0;

  IF v_manual_carry_forward_blocker_count > 0 OR v_can_carry_forward_automatically IS NOT TRUE THEN
    IF v_source_restoration_investigation_required THEN
      v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
        'code', 'SOURCE_RESTORATION_INVESTIGATION_REQUIRED_AFTER_CANCELLATION',
        'message', 'The unpaid payment can be cancelled, but ambiguous source-less adjustment evidence must be investigated and is not reconstructed.',
        'source_less_ambiguous_count', v_source_less_ambiguous_count,
        'carry_forward_blockers', COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb),
        'policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
      ));
    ELSE
      v_blockers := v_blockers || COALESCE(v_manual_adjustment_result -> 'carry_forward_blockers', '[]'::jsonb);
    END IF;
  END IF;`;
  source = replaceExactly(source, oldBlock, newBlock, 'diagnostic advisory admission');
  source = replaceExactly(
    source,
    "    WHEN v_manual_carry_forward_blocker_count > 0 THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'",
    "    WHEN v_manual_carry_forward_blocker_count > 0 AND v_source_restoration_investigation_required IS NOT TRUE THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'",
    'diagnostic alert blocker classification'
  );
  source = replaceExactly(
    source,
    "    'no_money_applied_count', v_no_money_applied_count\n  )::text);",
    "    'no_money_applied_count', v_no_money_applied_count,\n    'source_restoration_investigation_required', v_source_restoration_investigation_required,\n    'source_less_ambiguous_count', v_source_less_ambiguous_count\n  )::text);",
    'diagnostic status signature'
  );
  source = replaceExactly(
    source,
    "    'open_provider_submit_count', v_open_provider_submit_count,\n    'actor_user_id'",
    "    'open_provider_submit_count', v_open_provider_submit_count,\n    'source_restoration_investigation_required', v_source_restoration_investigation_required,\n    'source_less_ambiguous_count', v_source_less_ambiguous_count,\n    'actor_user_id'",
    'diagnostic support details'
  );
  source = replaceExactly(
    source,
    "    'can_carry_forward_automatically', v_can_carry_forward_automatically\n  );",
    "    'can_carry_forward_automatically', v_can_carry_forward_automatically,\n    'source_less_ambiguous_count', v_source_less_ambiguous_count,\n    'source_restoration_investigation_required', v_source_restoration_investigation_required,\n    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'\n  );",
    'diagnostic result fields'
  );
  return source;
}

export function patchClassifier(definition) {
  let source = definition;
  source = replaceExactly(
    source,
    "  v_safe_to_auto_apply boolean := false;\n  v_diagnostic_context text",
    "  v_safe_to_auto_apply boolean := false;\n  v_source_restoration_investigation_required boolean := false;\n  v_diagnostic_context text",
    'classifier investigation declaration'
  );
  source = replaceExactly(
    source,
    "  v_recommended_action := NULLIF(btrim(COALESCE(v_diagnostic_json->>'recommended_action', '')), '');\n  v_manual_result :=",
    "  v_recommended_action := NULLIF(btrim(COALESCE(v_diagnostic_json->>'recommended_action', '')), '');\n  v_source_restoration_investigation_required := COALESCE(NULLIF(v_diagnostic_json->>'source_restoration_investigation_required', '')::boolean, false);\n  v_manual_result :=",
    'classifier investigation assignment'
  );
  source = replaceExactly(
    source,
    "    'manual_adjustment_support_details_json', COALESCE(v_diagnostic_json->'manual_adjustment_support_details_json', '{}'::jsonb)\n  );",
    "    'manual_adjustment_support_details_json', COALESCE(v_diagnostic_json->'manual_adjustment_support_details_json', '{}'::jsonb),\n    'source_less_ambiguous_count', COALESCE(NULLIF(v_diagnostic_json->>'source_less_ambiguous_count', '')::integer, 0),\n    'source_restoration_investigation_required', v_source_restoration_investigation_required,\n    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'\n  );",
    'classifier manual result fields'
  );
  source = replaceExactly(
    source,
    "  IF COALESCE(NULLIF(v_diagnostic_json#>>'{manual_adjustment_support_details_json,source_less_ambiguous_count}', '')::integer, 0) > 0\n     OR COALESCE(jsonb_array_length(COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb)), 0) > 0 THEN",
    "  IF (COALESCE(NULLIF(v_diagnostic_json->>'source_less_ambiguous_count', '')::integer, 0) > 0\n     OR COALESCE(jsonb_array_length(COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb)), 0) > 0)\n     AND v_source_restoration_investigation_required IS NOT TRUE THEN",
    'classifier advisory blocker'
  );
  source = replaceExactly(
    source,
    "    'manual_adjustment_carry_forward', v_manual_result,\n    'resolved_full_payment_scope_json'",
    "    'manual_adjustment_carry_forward', v_manual_result,\n    'source_restoration_investigation_required', v_source_restoration_investigation_required,\n    'source_less_ambiguous_count', COALESCE(NULLIF(v_diagnostic_json->>'source_less_ambiguous_count', '')::integer, 0),\n    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION',\n    'resolved_full_payment_scope_json'",
    'classifier result fields'
  );
  return source;
}

const statusAdvisoryProof = `    ), candidate_source_less_manual_advisory_index AS MATERIALIZED (
        SELECT candidate_provider_precedence_index.*,
               (
                 candidate_provider_precedence_index.manual_carry_forward_blocked IS TRUE
                 AND candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE
                 AND candidate_provider_precedence_index.complete_candidate_instruction_scope
                 AND candidate_provider_precedence_index.removed IS NOT TRUE
                 AND candidate_provider_precedence_index.active_item_count > 0
                 AND candidate_provider_precedence_index.paid_or_settled IS NOT TRUE
                 AND candidate_provider_precedence_index.ambiguous IS NOT TRUE
                 AND candidate_provider_precedence_index.provider_pending_non_final IS NOT TRUE
                 AND candidate_provider_precedence_index.provider_outcome_unknown IS NOT TRUE
                 AND provider_facts.provider_submission_in_progress IS NOT TRUE
                 AND provider_facts.provider_outcome_unknown IS NOT TRUE
                 AND (
                   (
                     candidate_provider_precedence_index.canonical_provider_state = 'NO_TRANSFER_EVIDENCE'
                     AND candidate_provider_precedence_index.provider_request_sent IS NOT TRUE
                     AND candidate_provider_precedence_index.provider_external_id_present IS NOT TRUE
                     AND candidate_provider_precedence_index.provider_outage IS NOT TRUE
                     AND candidate_provider_precedence_index.terminal_no_money IS NOT TRUE
                   )
                   OR (
                     -- A historical outage observation cannot mask separately
                     -- proved terminal no-money evidence for this exact scope.
                     candidate_provider_precedence_index.canonical_provider_state = 'TERMINAL_NO_MONEY'
                     AND candidate_provider_precedence_index.terminal_no_money IS TRUE
                   )
                 )
               ) AS source_less_manual_adjustment_advisory
        FROM candidate_provider_precedence_index
        CROSS JOIN batch_provider_operation_facts AS provider_facts`;

const statusAdvisoryAllowance = `                  AND (
                    candidate_provider_precedence_index.manual_carry_forward_blocked IS NOT TRUE
                    OR candidate_provider_precedence_index.source_less_manual_adjustment_advisory
                  )`;

export function assertStatusAdmissionSafety(definition) {
  assert.equal(
    occurrences(definition, 'CREATE OR REPLACE FUNCTION public.pay_batch_payment_status_page_v1('),
    1,
    'status admission: exact function identity changed'
  );
  assert.equal(occurrences(definition, statusAdvisoryProof), 1, 'status admission: advisory proof changed');
  const advisoryTerminalBranch = statusAdvisoryProof.slice(
    statusAdvisoryProof.indexOf("canonical_provider_state = 'TERMINAL_NO_MONEY'")
  );
  assert.equal(
    occurrences(advisoryTerminalBranch, 'provider_outage'),
    0,
    'status admission: a historical outage must not veto proved terminal no-money release'
  );
  assert.equal(occurrences(definition, statusAdvisoryAllowance), 2, 'status admission: exact two eligibility allowances changed');
  assert.equal(
    occurrences(definition, 'candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE'),
    3,
    'status admission: genuine carry-forward freshness blockers must remain hard'
  );
  assert.equal(
    occurrences(definition, 'candidate_provider_precedence_index.complete_candidate_instruction_scope'),
    3,
    'status admission: complete Candidate instruction scope must remain mandatory'
  );
  assert.equal(
    occurrences(definition, 'public.pay_payment_cancelability_diagnostic('),
    0,
    'status admission: per-row diagnostic calls are prohibited'
  );
  assert.match(definition, /\nSTABLE\nPARALLEL RESTRICTED\nSECURITY DEFINER\nSET search_path TO pg_catalog, private, extensions, pg_temp\nSET statement_timeout TO '5000ms'\n/);
  return definition;
}

export function patchStatusAdmission(definition) {
  let source = definition;
  assert.equal(
    occurrences(source, 'candidate_provider_precedence_index.manual_carry_forward_blocked IS NOT TRUE'),
    2,
    'status admission: historical hard-veto count changed'
  );
  assert.equal(
    occurrences(source, 'candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE'),
    2,
    'status admission: historical freshness-fence count changed'
  );
  assert.equal(
    occurrences(source, 'candidate_provider_precedence_index.complete_candidate_instruction_scope'),
    2,
    'status admission: historical completeness-fence count changed'
  );
  source = replaceExactly(
    source,
    '    ), candidate_release_eligibility_index AS MATERIALIZED (',
    `${statusAdvisoryProof}\n    ), candidate_release_eligibility_index AS MATERIALIZED (`,
    'status admission advisory index'
  );
  source = replaceExactly(
    source,
    '        FROM candidate_provider_precedence_index\n        CROSS JOIN batch_provider_operation_facts AS provider_facts\n    ), candidate_classified_index AS MATERIALIZED (',
    '        FROM candidate_source_less_manual_advisory_index AS candidate_provider_precedence_index\n        CROSS JOIN batch_provider_operation_facts AS provider_facts\n    ), candidate_classified_index AS MATERIALIZED (',
    'status admission advisory eligibility source'
  );
  const historicalManualVeto = '                  AND candidate_provider_precedence_index.manual_carry_forward_blocked IS NOT TRUE';
  assert.equal(
    occurrences(source, historicalManualVeto),
    2,
    'status admission advisory eligibility: exact source count changed'
  );
  source = source.replaceAll(historicalManualVeto, statusAdvisoryAllowance);
  return assertStatusAdmissionSafety(source);
}

export function patchPlan(definition) {
  let source = definition;
  const historicalMailReferenceCount = occurrences(source, 'public.mail_outbox');
  const legacyMailStart = '  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_mail;';
  const legacyMailEnd = '  v_work_item_count := CASE';
  const legacyMailStartIndex = source.indexOf(legacyMailStart);
  const legacyMailEndIndex = source.indexOf(legacyMailEnd, legacyMailStartIndex);
  assert.ok(legacyMailStartIndex >= 0 && legacyMailEndIndex > legacyMailStartIndex, 'plan: historical mail boundary changed');
  const legacyMailBlock = source.slice(legacyMailStartIndex, legacyMailEndIndex);

  source = replaceExactly(
    source,
    "  v_mail_legacy_queued_review_count integer := 0;\n\n  v_retry_eligible_count integer := 0;",
    "  v_mail_legacy_queued_review_count integer := 0;\n  v_financial_cancellation_communication_v2 boolean := false;\n\n  v_retry_eligible_count integer := 0;",
    'plan communication V2 admission declaration'
  );
  source = replaceExactly(
    source,
    "  IF jsonb_array_length(COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb)) > 0 THEN\n    v_hard_blockers := v_hard_blockers || COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb);\n  END IF;",
    `  IF jsonb_array_length(COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb)) > 0 THEN
    IF COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false) THEN
      v_warnings := v_warnings || jsonb_build_array(jsonb_build_object(
        'code', 'SOURCE_RESTORATION_INVESTIGATION_REQUIRED_AFTER_CANCELLATION',
        'message', 'Cancellation may continue for this proved-unpaid complete scope; ambiguous source-less rows remain frozen investigation evidence and are not reconstructed.',
        'source_less_ambiguous_count', COALESCE(NULLIF(v_classification_result->>'source_less_ambiguous_count', '')::integer, 0),
        'policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
      ));
    ELSE
      v_hard_blockers := v_hard_blockers || COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb);
    END IF;
  END IF;`,
    'plan advisory blockers'
  );
  source = replaceRangeExactly(
    source,
    legacyMailStart,
    legacyMailEnd,
    `  -- Communication V2 is selected only from the backend's exact, complete,
  -- proved-unpaid cancellation classification. A caller's requested_action is
  -- never authority, and Draft removal deliberately retains the historical path.
  v_financial_cancellation_communication_v2 :=
    v_correction_context IN (
      'CORRECTION_ACTION',
      'PAYMENT_CORRECTION_ACTION',
      'USER_TRIGGERED_CORRECTION',
      'CURRENT_PAYMENT_STATUS_CORRECTION_ACTION',
      'PAYMENT_ISSUES_CORRECTION_ACTION'
    )
    AND v_scope_type IN ('BATCH', 'CANDIDATES')
    AND COALESCE(v_classification_result #>> '{resolved_full_payment_scope_json,scope_type}', '') = v_scope_type
    AND COALESCE(NULLIF(v_classification_result #>> '{resolved_full_payment_scope_json,is_full_scope}', '')::boolean, false)
    AND COALESCE(v_draft_removal_requested, false) IS NOT TRUE
    AND (
      (
        v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
        AND COALESCE(NULLIF(v_classification_result->>'can_pre_provider_cancel', '')::boolean, false)
      )
      OR (
        v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'
        AND COALESCE(NULLIF(v_classification_result->>'can_no_money_unwind', '')::boolean, false)
      )
    );

  IF v_financial_cancellation_communication_v2 THEN
    -- Financial cancellation must never depend on mail state. This owner does
    -- not read mail and therefore cannot prove that the original payment notice
    -- was SENT: QUEUED is not SENT, and no follow-up notice is authorised here.
    v_queued_unsent_count := 0;
    v_sent_notice_count := 0;
    v_mail_legacy_review_count := 0;
    v_mail_legacy_queued_review_count := 0;
    v_communication_effects := jsonb_build_object(
      'queued_unsent_to_cancel', '[]'::jsonb,
      'legacy_broad_matches_requiring_review', '[]'::jsonb,
      'sent_to_leave_as_audit', '[]'::jsonb,
      'external_correction_notice', false,
      'admin_notice_required', true,
      'queued_unsent_count', 0,
      'sent_notice_count', 0,
      'legacy_broad_review_count', 0,
      'legacy_broad_queued_review_count', 0,
      'selected_scope_json', v_selected_mail_scope_json,
      'financial_cancellation_independent', true,
      'communication_cleanup_contract_version', 2,
      'mail_outbox_mutation_count', 0,
      'follow_up_cancellation_notice_permitted', false,
      'follow_up_cancellation_notice_requires_proved_original_sent', true
    );
  ELSE
${legacyMailBlock}  END IF;

`,
    'plan communication decoupling'
  );
  source = replaceExactly(
    source,
    "    'ambiguous_manual_adjustment_blockers', COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb),",
    "    'ambiguous_manual_adjustment_blockers', COALESCE(v_classification_result->'carry_forward_blockers', '[]'::jsonb),\n    'source_restoration_investigation_required', COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false),\n    'source_less_ambiguous_count', COALESCE(NULLIF(v_classification_result->>'source_less_ambiguous_count', '')::integer, 0),\n    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION',",
    'plan result investigation fields'
  );
  assert.equal(
    occurrences(source, 'public.mail_outbox'),
    historicalMailReferenceCount,
    'plan: historical non-cancellation mail behavior changed'
  );
  assert.equal(occurrences(source, legacyMailBlock), 1, 'plan: historical mail block bytes changed');
  const communicationV2Admission = source.slice(
    source.indexOf('  v_financial_cancellation_communication_v2 :='),
    source.indexOf('  IF v_financial_cancellation_communication_v2 THEN')
  );
  assert.doesNotMatch(communicationV2Admission, /requested_action/i, 'plan: caller action selected communication V2');
  assert.match(communicationV2Admission, /resolved_full_payment_scope_json,is_full_scope/);
  assert.match(communicationV2Admission, /v_draft_removal_requested, false\) IS NOT TRUE/);
  return assertPlanCommunicationSafety(source);
}

export function assertPlanCommunicationSafety(definition) {
  assert.equal(
    occurrences(definition, '  v_financial_cancellation_communication_v2 :=\n'),
    1,
    'plan: communication V2 admission assignment changed'
  );
  assert.equal(
    occurrences(definition, '  IF v_financial_cancellation_communication_v2 THEN\n'),
    1,
    'plan: communication V2 branch changed'
  );
  const admissionStart = definition.indexOf('  v_financial_cancellation_communication_v2 :=\n');
  const admissionEnd = definition.indexOf('  IF v_financial_cancellation_communication_v2 THEN\n', admissionStart);
  assert.ok(admissionStart >= 0 && admissionEnd > admissionStart, 'plan: communication V2 admission range missing');
  const admission = definition.slice(admissionStart, admissionEnd);
  assert.match(admission, /v_correction_context IN \(\n      'CORRECTION_ACTION',\n      'PAYMENT_CORRECTION_ACTION',\n      'USER_TRIGGERED_CORRECTION',\n      'CURRENT_PAYMENT_STATUS_CORRECTION_ACTION',\n      'PAYMENT_ISSUES_CORRECTION_ACTION'\n    \)/);
  assert.doesNotMatch(admission, /CORRECTION_REVIEW|DRAFT_REMOVE_FROM_BATCH|requested_action/i);
  assert.match(admission, /v_scope_type IN \('BATCH', 'CANDIDATES'\)/);
  assert.match(admission, /resolved_full_payment_scope_json,scope_type}'[^\n]+ = v_scope_type/);
  assert.match(admission, /resolved_full_payment_scope_json,is_full_scope}/);
  assert.match(admission, /v_draft_removal_requested, false\) IS NOT TRUE/);
  assert.match(admission, /v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'[\s\S]+can_pre_provider_cancel/);
  assert.match(admission, /v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'[\s\S]+can_no_money_unwind/);

  const v2Start = admissionEnd;
  const v2End = definition.indexOf('  ELSE\n  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_mail;', v2Start);
  assert.ok(v2End > v2Start, 'plan: exact legacy ELSE boundary missing');
  const v2Branch = definition.slice(v2Start, v2End);
  assert.doesNotMatch(v2Branch, /public\.mail_outbox/);
  assert.match(v2Branch, /'follow_up_cancellation_notice_permitted', false/);
  assert.match(v2Branch, /'follow_up_cancellation_notice_requires_proved_original_sent', true/);
  return definition;
}

export function patchSelectionPrepare(definition) {
  let source = definition;
  source = replaceExactly(
    source,
    "    v_candidate_scope_contract_raw text;",
    "    v_candidate_scope_contract_raw text;\n    v_communication_cleanup_contract_raw text;\n    v_fresh_candidate_scope_assignment boolean := false;",
    'selection prepare communication declarations'
  );
  source = replaceExactly(
    source,
    "            v_candidate_scope_contract_version := 2;\n        ELSE",
    "            v_candidate_scope_contract_version := 2;\n            v_fresh_candidate_scope_assignment := true;\n        ELSE",
    'selection prepare fresh assignment marker'
  );
  source = replaceExactly(
    source,
    `    v_communication_cleanup_contract_version := CASE
        WHEN v_candidate_scope_contract_version = 2 THEN 1
        ELSE NULL::integer
    END;`,
    `    IF v_candidate_scope_contract_version = 1 THEN
        v_communication_cleanup_contract_version := NULL::integer;
    ELSIF v_fresh_candidate_scope_assignment THEN
        -- Communication contract V2 is assigned only to a genuinely fresh
        -- candidate-scope V2 request before any membership or page exists.
        v_communication_cleanup_contract_version := 2;
    ELSE
        v_communication_cleanup_contract_raw := NULLIF(
            pg_catalog.btrim(COALESCE(v_request.plan_json->>'communication_cleanup_contract_version', '')),
            ''
        );
        IF v_communication_cleanup_contract_raw IN ('1', '2') THEN
            v_communication_cleanup_contract_version := v_communication_cleanup_contract_raw::integer;
        ELSE
            RAISE EXCEPTION 'PAYMENT_CORRECTION_COMMUNICATION_CLEANUP_CONTRACT_MISMATCH'
                USING ERRCODE = 'P0001',
                      DETAIL = pg_catalog.jsonb_build_object(
                          'code', 'PAYMENT_CORRECTION_COMMUNICATION_CLEANUP_CONTRACT_MISMATCH',
                          'candidate_scope_contract_version', v_candidate_scope_contract_version,
                          'communication_cleanup_contract_version', v_communication_cleanup_contract_raw,
                          'correction_request_id', v_request.id
                      )::text;
        END IF;
    END IF;`,
    'selection prepare exact communication version'
  );

  const mailObservationStart = `        -- Communications are observed independently. V1 retains the exact
        -- historical combined-count/hash semantics. V2 deliberately keeps
        -- volatile delivery state outside the financial scope contract.`;
  const mailObservationEnd = `        IF v_item_count > v_max_items_per_candidate`;
  const mailStartIndex = source.indexOf(mailObservationStart);
  const mailEndIndex = source.indexOf(mailObservationEnd, mailStartIndex);
  assert.ok(mailStartIndex >= 0 && mailEndIndex > mailStartIndex, 'selection prepare mail observation boundary changed');
  const legacyMailObservation = source.slice(mailStartIndex, mailEndIndex);
  source = replaceRangeExactly(
    source,
    mailObservationStart,
    mailObservationEnd,
    `        IF v_communication_cleanup_contract_version = 2 THEN
            -- Fresh communication V2 never reads, hashes, vetoes or mutates mail.
            v_matching_queued_count := 0;
        ELSE
${legacyMailObservation.split('\n').map((line) => `    ${line}`).join('\n')}        END IF;

`,
    'selection prepare communication V2 mail-free branch'
  );

  assert.equal(
    occurrences(source, "'communication_cleanup_contract_version', 1"),
    3,
    'selection prepare hard-coded communication markers changed'
  );
  source = source.replaceAll(
    "'communication_cleanup_contract_version', 1",
    "'communication_cleanup_contract_version', v_communication_cleanup_contract_version"
  );
  assert.equal(
    occurrences(source, "'communication_cleanup_contract_version', v_communication_cleanup_contract_version"),
    6,
    'selection prepare communication version propagation incomplete'
  );
  return source;
}

export function patchExpandWork(definition) {
  let source = definition;
  source = replaceExactly(
    source,
    "  v_candidate_scope_contract_raw text;\n  v_source_row_count_semantics text := 'FINANCIAL_AND_QUEUED_COMMUNICATIONS';",
    "  v_candidate_scope_contract_raw text;\n  v_communication_cleanup_contract_version integer := NULL::integer;\n  v_communication_cleanup_contract_raw text;\n  v_source_row_count_semantics text := 'FINANCIAL_AND_QUEUED_COMMUNICATIONS';",
    'expand communication declarations'
  );

  const contractStart = `  v_candidate_scope_contract_raw := NULLIF(`;
  const contractEnd = `  SELECT COALESCE(settings_row.banking_pay_draft_overlay_fast_cancel_v1_enabled,false)`;
  source = replaceRangeExactly(
    source,
    contractStart,
    contractEnd,
    `  v_candidate_scope_contract_raw := NULLIF(
    BTRIM(COALESCE(v_request.plan_json->>'candidate_scope_contract_version', '')),
    ''
  );
  IF v_candidate_scope_contract_raw IS NULL OR v_candidate_scope_contract_raw = '1' THEN
    v_candidate_scope_contract_version := 1;
    v_communication_cleanup_contract_version := NULL::integer;
    v_source_row_count_semantics := 'FINANCIAL_AND_QUEUED_COMMUNICATIONS';
  ELSIF v_candidate_scope_contract_raw = '2' THEN
    v_candidate_scope_contract_version := 2;
    v_source_row_count_semantics := 'FINANCIAL_ONLY';
    v_communication_cleanup_contract_raw := NULLIF(
      BTRIM(COALESCE(v_request.plan_json->>'communication_cleanup_contract_version', '')),
      ''
    );
    IF v_request.plan_json->>'candidate_scope_hash_version' IS DISTINCT FROM '2'
       OR v_request.plan_json->>'source_row_count_semantics' IS DISTINCT FROM 'FINANCIAL_ONLY'
       OR v_communication_cleanup_contract_raw NOT IN ('1', '2') THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH',
          'correction_request_id', p_correction_request_id,
          'candidate_scope_contract_version', v_candidate_scope_contract_raw,
          'communication_cleanup_contract_version', v_communication_cleanup_contract_raw
        )::text;
    END IF;
    v_communication_cleanup_contract_version := v_communication_cleanup_contract_raw::integer;
  ELSE
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED',
        'correction_request_id', p_correction_request_id,
        'candidate_scope_contract_version', v_candidate_scope_contract_raw
      )::text;
  END IF;

`,
    'expand exact communication contract validation'
  );

  source = replaceExactly(
    source,
    "'communication_cleanup_contract_version', 1",
    "'communication_cleanup_contract_version', v_communication_cleanup_contract_version",
    'expand work communication propagation'
  );
  source = replaceExactly(
    source,
    "work_row.selection_json->>'communication_cleanup_contract_version' IS DISTINCT FROM '1'",
    "work_row.selection_json->>'communication_cleanup_contract_version' IS DISTINCT FROM v_communication_cleanup_contract_version::text",
    'expand work communication final check'
  );
  source = replaceExactly(
    source,
    "         'source_row_count_semantics', v_source_row_count_semantics\n      )",
    `         'source_row_count_semantics', v_source_row_count_semantics
      ) || CASE WHEN v_candidate_scope_contract_version = 2 THEN
        pg_catalog.jsonb_build_object(
          'communication_cleanup_contract_version', v_communication_cleanup_contract_version
        )
      ELSE '{}'::jsonb END`,
    'expand work result metadata'
  );
  return source;
}

export function patchIntegrityCheck(definition) {
  let source = definition;
  source = replaceExactly(
    source,
    "    v_recalculated_selection_hash text;\n    v_expected_selection_hash text;",
    `    v_recalculated_selection_hash text;
    v_expected_selection_hash text;
    v_candidate_scope_contract_raw text;
    v_communication_cleanup_contract_raw text;
    v_communication_cleanup_contract_version integer := NULL::integer;
    v_selection_contract_valid boolean := true;`,
    'integrity communication declarations'
  );

  const hashStart = `        v_recalculated_selection_hash := private.pay_payment_correction_sha256_v1(`;
  const hashEnd = `        v_expected_selection_hash := v_request.selection_hash;`;
  source = replaceRangeExactly(
    source,
    hashStart,
    hashEnd,
    `        v_candidate_scope_contract_raw := NULLIF(
            pg_catalog.btrim(COALESCE(v_request.plan_json->>'candidate_scope_contract_version', '')),
            ''
        );
        v_communication_cleanup_contract_raw := NULLIF(
            pg_catalog.btrim(COALESCE(v_request.plan_json->>'communication_cleanup_contract_version', '')),
            ''
        );

        IF v_candidate_scope_contract_raw IS NULL
           AND v_request.plan_json->>'candidate_scope_hash_version' IS NULL
           AND v_request.plan_json->>'source_row_count_semantics' IS NULL
           AND v_communication_cleanup_contract_raw IS NULL THEN
            -- Legacy V1 signed only the original base field set.
            v_communication_cleanup_contract_version := NULL::integer;
        ELSIF v_candidate_scope_contract_raw = '1'
          AND v_request.plan_json->>'candidate_scope_hash_version' = '1'
          AND v_request.plan_json->>'source_row_count_semantics' = 'FINANCIAL_AND_QUEUED_COMMUNICATIONS'
          AND v_communication_cleanup_contract_raw IS NULL THEN
            -- A bounded V1 preparation page can expose these exact temporary
            -- markers before the final legacy plan restores the old field set.
            v_communication_cleanup_contract_version := NULL::integer;
        ELSIF v_candidate_scope_contract_raw = '2'
          AND v_request.plan_json->>'candidate_scope_hash_version' = '2'
          AND v_request.plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
          AND v_communication_cleanup_contract_raw IN ('1', '2') THEN
            v_communication_cleanup_contract_version := v_communication_cleanup_contract_raw::integer;
        ELSE
            v_selection_contract_valid := false;
            v_failure_categories := v_failure_categories
                || pg_catalog.jsonb_build_array('SELECTION_CONTRACT_MISMATCH');
        END IF;

        IF v_selection_contract_valid THEN
            v_recalculated_selection_hash := private.pay_payment_correction_sha256_v1(
                pg_catalog.jsonb_build_object(
                    'version', 2,
                    'pay_batch_id', v_request.pay_batch_id,
                    'requested_action', COALESCE(
                        v_request.plan_json ->> 'requested_action',
                        v_request.selection_json ->> 'requested_action',
                        v_request.selection_json ->> 'action'
                    ),
                    'active_batch_scope_hash',
                        v_request.plan_json ->> 'active_batch_scope_hash',
                    'selected_candidate_count', v_selected_candidate_count,
                    'selected_active_item_count', v_selected_active_item_count,
                    'selected_source_row_count', v_selected_source_row_count,
                    'selected_amount_pence',
                        pg_catalog.round(v_selected_amount * 100)::bigint,
                    'selected_chain_hash', v_request.plan_json->>'selected_chain_hash',
                    'unselected_chain_hash', v_request.plan_json->>'unselected_chain_hash',
                    'prepare_page_count', (v_request.plan_json->>'prepare_page_count')::integer
                ) || CASE
                    WHEN v_candidate_scope_contract_raw = '2' THEN
                        pg_catalog.jsonb_build_object(
                            'candidate_scope_contract_version', 2,
                            'candidate_scope_hash_version', 2,
                            'source_row_count_semantics', 'FINANCIAL_ONLY',
                            'communication_cleanup_contract_version', v_communication_cleanup_contract_version
                        )
                    ELSE '{}'::jsonb
                END
            );
        END IF;
`,
    'integrity version-exact selection hash'
  );
  source = replaceExactly(
    source,
    `        IF v_expected_selection_hash IS NULL
           OR v_recalculated_selection_hash IS DISTINCT FROM v_expected_selection_hash THEN`,
    `        IF v_selection_contract_valid
           AND v_expected_selection_hash IS NULL
           AND v_request.status = 'PLANNING' THEN
            v_warnings := v_warnings
                || pg_catalog.jsonb_build_array('SELECTION_PREPARATION_IN_PROGRESS');
        ELSIF v_selection_contract_valid
           AND (
               v_expected_selection_hash IS NULL
               OR v_recalculated_selection_hash IS DISTINCT FROM v_expected_selection_hash
           ) THEN`,
    'integrity skip hash after typed contract failure'
  );
  return source;
}

export function patchApply(definition, config) {
  let source = definition;
  const historicalMailReferenceCount = occurrences(source, 'public.mail_outbox');
  source = replaceExactly(
    source,
    "  v_carry_forward_released_count integer := 0;",
    "  v_carry_forward_released_count integer := 0;\n  v_source_restoration_investigation_count integer := 0;\n  v_communication_cleanup_contract_version integer := 1;\n  v_communication_cleanup_contract_plan_raw text;\n  v_communication_cleanup_contract_work_raw text;",
    `${config.label} investigation declaration`
  );

  source = replaceRangeExactly(
    source,
    `  v_candidate_scope_contract_raw := NULLIF(BTRIM(COALESCE(`,
    config.selectedTableStart,
    `  v_candidate_scope_contract_raw := NULLIF(BTRIM(COALESCE(
    v_work_item.selection_json->>'candidate_scope_contract_version',
    v_request.plan_json->>'candidate_scope_contract_version',
    ''
  )), '');
  IF v_candidate_scope_contract_raw IS NULL OR v_candidate_scope_contract_raw = '1' THEN
    -- Legacy V1 retains its exact signed financial-plus-queued-mail contract.
    v_candidate_scope_contract_version := 1;
    v_candidate_scope_hash_version := 1;
    v_communication_cleanup_contract_version := 1;
    v_source_row_count_semantics := 'FINANCIAL_AND_QUEUED_COMMUNICATIONS';
  ELSIF v_candidate_scope_contract_raw = '2' THEN
    v_candidate_scope_contract_version := 2;
    v_candidate_scope_hash_version := 2;
    v_source_row_count_semantics := 'FINANCIAL_ONLY';
    v_communication_cleanup_contract_plan_raw := NULLIF(BTRIM(COALESCE(
      v_request.plan_json->>'communication_cleanup_contract_version', ''
    )), '');
    v_communication_cleanup_contract_work_raw := NULLIF(BTRIM(COALESCE(
      v_work_item.selection_json->>'communication_cleanup_contract_version', ''
    )), '');
    IF v_request.plan_json->>'candidate_scope_contract_version' IS DISTINCT FROM '2'
       OR v_work_item.selection_json->>'candidate_scope_contract_version' IS DISTINCT FROM '2'
       OR v_request.plan_json->>'candidate_scope_hash_version' IS DISTINCT FROM '2'
       OR v_work_item.selection_json->>'candidate_scope_hash_version' IS DISTINCT FROM '2'
       OR v_request.plan_json->>'source_row_count_semantics' IS DISTINCT FROM 'FINANCIAL_ONLY'
       OR v_work_item.selection_json->>'source_row_count_semantics' IS DISTINCT FROM 'FINANCIAL_ONLY'
       OR v_communication_cleanup_contract_plan_raw NOT IN ('1', '2')
       OR v_communication_cleanup_contract_work_raw IS DISTINCT FROM v_communication_cleanup_contract_plan_raw THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH',
          'work_item_id', p_work_item_id,
          'correction_request_id', v_request.id,
          'communication_cleanup_contract_version', v_communication_cleanup_contract_plan_raw
        )::text;
    END IF;
    v_communication_cleanup_contract_version := v_communication_cleanup_contract_plan_raw::integer;
  ELSE
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED',
        'work_item_id', p_work_item_id,
        'candidate_scope_contract_version', v_candidate_scope_contract_raw
      )::text;
  END IF;

`,
    `${config.label} exact communication contract validation`
  );

  const capacityStartIndex = source.indexOf(config.capacityMailStart);
  const capacityEndIndex = source.indexOf(config.capacityMailEnd, capacityStartIndex);
  assert.ok(capacityStartIndex >= 0 && capacityEndIndex > capacityStartIndex, `${config.label}: capacity mail boundary changed`);
  const legacyCapacityBlock = source.slice(capacityStartIndex, capacityEndIndex);
  source = replaceRangeExactly(
    source,
    config.capacityMailStart,
    config.capacityMailEnd,
    `  IF v_communication_cleanup_contract_version = 1 THEN
${legacyCapacityBlock.split('\n').map((line) => `  ${line}`).join('\n')}  ELSE
    -- Fresh communication V2 is financial-only: mail is not read, locked,
    -- hashed, vetoed or mutated by cancellation.
    v_matching_queued_count := 0;
    v_unsafe_queued_count := 0;
    v_already_sent_untouched_count := 0;
    v_other_terminal_untouched_count := 0;
  END IF;

`,
    `${config.label} versioned capacity communication behavior`
  );

  source = replaceExactly(
    source,
    config.manualBlockCondition,
    `${config.manualBlockCondition.slice(0, -5).replace('  IF ', '  IF (')}\n  )\n     AND COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false) IS NOT TRUE THEN`,
    `${config.label} advisory manual blocker`
  );

  const membershipCommunicationNeedle =
    "request_work.selection_json->>'communication_cleanup_contract_version' = '1'";
  assert.equal(
    occurrences(source, membershipCommunicationNeedle),
    config.membershipCommunicationCheckCount,
    `${config.label}: membership communication check count changed`
  );
  source = source.replaceAll(
    membershipCommunicationNeedle,
    "request_work.selection_json->>'communication_cleanup_contract_version' = v_communication_cleanup_contract_version::text"
  );

  source = replaceExactly(
    source,
    `  GET DIAGNOSTICS v_inserted_correction_item_count = ROW_COUNT;`,
    `  GET DIAGNOSTICS v_inserted_correction_item_count = ROW_COUNT;

  -- Safe source-less adjustments have already followed the unchanged carry-
  -- forward owner. Ambiguous rows are never guessed or recreated; their exact
  -- frozen item facts remain on the APPLIED correction ledger for investigation.
  WITH ambiguous_source_less_items AS (
    SELECT
      (classified_item.value->>'pay_batch_item_id')::uuid AS pay_batch_item_id,
      COALESCE(
        NULLIF(BTRIM(classified_item.value->>'ambiguity_reason'), ''),
        'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS'
      ) AS ambiguity_reason
    FROM jsonb_array_elements(COALESCE(
      v_manual_adjustment_result #> '{manual_adjustment_support_details_json,classified_items}',
      '[]'::jsonb
    )) AS classified_item(value)
    WHERE classified_item.value->>'classification' = 'SOURCE_LESS_AMBIGUOUS'
      AND COALESCE(classified_item.value->>'pay_batch_item_id', '')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ), expected_markers AS (
    SELECT
      ambiguous_source_less_items.pay_batch_item_id,
      jsonb_build_object(
        'status', 'NEEDS_INVESTIGATION',
        'reason', ambiguous_source_less_items.ambiguity_reason,
        'policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION',
        'alert_kind', 'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',
        'source_reconstruction_performed', false,
        'source_carry_forward_created', false
      ) AS marker
    FROM ambiguous_source_less_items
  ), marked_rows AS (
    UPDATE public.pay_payment_correction_items AS correction_item
    SET after_snapshot_json = COALESCE(correction_item.after_snapshot_json, '{}'::jsonb)
          || jsonb_build_object('source_restoration', expected_markers.marker)
    FROM expected_markers
    WHERE correction_item.correction_request_id = v_work_item.correction_request_id
      AND correction_item.pay_batch_id = v_work_item.pay_batch_id
      AND correction_item.pay_batch_item_id = expected_markers.pay_batch_item_id
      AND correction_item.correction_item_kind = '${config.correctionKind}'
      AND correction_item.status = 'APPLIED'
      AND correction_item.after_snapshot_json #> '{source_restoration}' IS DISTINCT FROM expected_markers.marker
    RETURNING correction_item.id
  )
  SELECT count(*)::integer
  INTO v_source_restoration_investigation_count
  FROM marked_rows;`,
    `${config.label} immutable investigation marker`
  );

  const applyStartIndex = source.indexOf(config.applyMailStart);
  const applyEndIndex = source.indexOf(config.applyMailEnd, applyStartIndex);
  assert.ok(applyStartIndex >= 0 && applyEndIndex > applyStartIndex, `${config.label}: apply mail boundary changed`);
  const legacyApplyBlock = source.slice(applyStartIndex, applyEndIndex);
  source = replaceRangeExactly(
    source,
    config.applyMailStart,
    config.applyMailEnd,
    `  IF v_communication_cleanup_contract_version = 1 THEN
${legacyApplyBlock.split('\n').map((line) => `  ${line}`).join('\n')}  ELSE
    v_cancelled_mail_count := 0;
    v_communications_review_required_count := 0;
    v_mail_scope_matching := jsonb_build_object(
      'exact_cancelled', 0,
      'legacy_review', 0,
      'selected_scope_json', v_mail_selected_scope_json,
      'matches', '[]'::jsonb,
      'financial_cancellation_independent', true,
      'mail_outbox_read_performed', false,
      'mail_outbox_mutation_count', 0,
      'follow_up_cancellation_notice_permitted', false,
      'follow_up_cancellation_notice_requires_proved_original_sent', true
    );
  END IF;

`,
    `${config.label} versioned apply communication behavior`
  );

  const communicationResultNeedle = `'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN 1 ELSE NULL::integer END`;
  assert.equal(
    occurrences(source, communicationResultNeedle),
    config.communicationResultCount,
    `${config.label}: communication result field count changed`
  );
  source = source.replaceAll(
    communicationResultNeedle,
    `'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN v_communication_cleanup_contract_version ELSE NULL::integer END`
  );

  const resultNeedle = "    'communications_review_required', v_communications_review_required_count";
  assert.equal(occurrences(source, resultNeedle), config.resultFieldCount, `${config.label}: result field count changed`);
  assert.equal(occurrences(source, `${resultNeedle},`), config.resultFieldCommaCount, `${config.label}: result separator count changed`);
  // Normalize the owned separator first, then add the backward-compatible
  // fields with the exact trailing separator required by the original object.
  source = source.replaceAll(`${resultNeedle},`, resultNeedle);
  source = source.replaceAll(
    resultNeedle,
    `${resultNeedle},\n    'financial_cancellation_independent_of_mail', v_communication_cleanup_contract_version = 2,\n    'mail_outbox_read_performed', v_communication_cleanup_contract_version = 1,\n    'mail_outbox_mutation_count', CASE WHEN v_communication_cleanup_contract_version = 1 THEN v_cancelled_mail_count ELSE 0 END,\n    'follow_up_cancellation_notice_permitted', false,\n    'follow_up_cancellation_notice_requires_proved_original_sent', v_communication_cleanup_contract_version = 2,\n    'source_restoration_investigation_required', COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false),\n    'source_restoration_investigation_count', COALESCE(v_source_restoration_investigation_count, 0),\n    'source_restoration_policy', 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'${config.resultFieldNeedsTrailingComma ? ',' : ''}`
  );

  if (config.splitNoMoneyResultObject === true) {
    const unsplitResultBoundary = `'candidate_scope_hash_version', v_candidate_scope_hash_version,
    'source_row_count_semantics', v_source_row_count_semantics,
    'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN v_communication_cleanup_contract_version ELSE NULL::integer END,
    'matching_queued_count', v_matching_queued_count,`;
    const splitResultBoundary = `'candidate_scope_hash_version', v_candidate_scope_hash_version,
    'source_row_count_semantics', v_source_row_count_semantics,
    'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN v_communication_cleanup_contract_version ELSE NULL::integer END
  ) || jsonb_build_object(
    'matching_queued_count', v_matching_queued_count,`;
    source = replaceExactly(
      source,
      unsplitResultBoundary,
      splitResultBoundary,
      `${config.label} bounded result-object arity`
    );
  }

  assert.equal(
    occurrences(source, 'public.mail_outbox'),
    historicalMailReferenceCount,
    `${config.label}: legacy communication behavior changed`
  );
  assert.ok(
    source.includes('IF v_communication_cleanup_contract_version = 1 THEN'),
    `${config.label}: communication version branch missing`
  );
  assert.equal(
    occurrences(source, "'follow_up_cancellation_notice_permitted', true"),
    0,
    `${config.label}: unproved follow-up cancellation notice was permitted`
  );
  assert.ok(
    occurrences(source, "'follow_up_cancellation_notice_requires_proved_original_sent'") >= 2,
    `${config.label}: authoritative original-SENT prerequisite missing`
  );
  if (config.splitNoMoneyResultObject === true) {
    assertNoMoneyResultObjectAritySafety(source);
  }
  return source;
}

export function assertNoMoneyResultObjectAritySafety(definition) {
  const resultStart = definition.indexOf('  v_result := jsonb_build_object(');
  const resultEnd = definition.indexOf(
    '\n\n  UPDATE public.pay_payment_correction_work_items AS applied_work_item',
    resultStart
  );
  assert.ok(resultStart >= 0 && resultEnd > resultStart, 'no-money apply: result-object boundary changed');
  const resultExpression = definition.slice(resultStart, resultEnd);
  const pieces = resultExpression.split(/\n  \) \|\| jsonb_build_object\(\n/);
  const pairCounts = pieces.map((piece) => (
    [...piece.matchAll(/^    '([^']+)',/gm)].length
  ));
  assert.deepEqual(
    pairCounts,
    [43, 11, 20, 8],
    'no-money apply: result-object pieces must remain below PostgreSQL\'s 100-argument limit'
  );
  const orderedKeys = pieces.flatMap((piece) => (
    [...piece.matchAll(/^    '([^']+)',/gm)].map((match) => match[1])
  ));
  assert.equal(orderedKeys.length, 82, 'no-money apply: result-object field count changed');
  assert.equal(new Set(orderedKeys).size, 82, 'no-money apply: duplicate result-object field');
  assert.match(
    resultExpression,
    /'communication_cleanup_contract_version', CASE\n      WHEN v_candidate_scope_contract_version = 2 THEN v_communication_cleanup_contract_version ELSE NULL::integer END\n  \) \|\| jsonb_build_object\(\n    'matching_queued_count', v_matching_queued_count,/,
    'no-money apply: exact bounded structural split is missing'
  );
  return definition;
}

export function assertAlertCompletenessSafety(definition) {
  assert.match(
    definition,
    /ELSIF p_limit = 0 THEN\n    v_limit := NULL::integer;/,
    'alerts: p_limit=0 must retain its established complete-detail meaning'
  );
  const investigationStart = definition.indexOf('  source_restoration_investigation_rows AS MATERIALIZED (');
  const investigationEnd = definition.indexOf('  source_restoration_investigation_alerts AS MATERIALIZED (', investigationStart);
  assert.ok(investigationStart >= 0 && investigationEnd > investigationStart, 'alerts: investigation source CTE boundaries changed');
  const investigationRows = definition.slice(investigationStart, investigationEnd);
  assert.doesNotMatch(
    investigationRows,
    /^\s+LIMIT\b/im,
    'alerts: investigation source must not truncate the authoritative count/hash universe'
  );
  assert.match(
    definition,
    /limited_alerts AS MATERIALIZED \([\s\S]+?LIMIT v_limit\n  \),/,
    'alerts: established detail-page limit missing'
  );
  const signalStart = definition.indexOf('  signal_aggregate AS MATERIALIZED (');
  const signalEnd = definition.indexOf('  aggregate_result AS MATERIALIZED (', signalStart);
  assert.ok(signalStart >= 0 && signalEnd > signalStart, 'alerts: global signal CTE boundaries changed');
  assert.match(
    definition.slice(signalStart, signalEnd),
    /^    FROM alert_rows$/m,
    'alerts: global signal must continue to use the complete alert rows'
  );
}

export function patchAlerts(definition) {
  let source = definition;

  source = replaceExactly(
    source,
    "    END > 0\n       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS%'",
    "    END > 0\n       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS%'\n    )\n      AND COALESCE(NULLIF(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'source_restoration_investigation_required', '')::boolean, false) IS NOT TRUE",
    'old blocking alert excludes advisory cases'
  );
  source = replaceExactly(
    source,
    "    FROM grouped_banking_pay_diagnostic_scope\n    WHERE CASE",
    "    FROM grouped_banking_pay_diagnostic_scope\n    WHERE (CASE",
    'old blocking alert parenthesis'
  );

  const investigationCtes = `  source_restoration_investigation_rows AS MATERIALIZED (
    SELECT
      correction_item.id AS correction_item_id,
      correction_item.pay_batch_id,
      correction_item.pay_batch_item_id,
      correction_item.pay_batch_candidate_id,
      correction_item.candidate_id,
      correction_item.correction_item_kind,
      correction_item.item_type,
      correction_item.amount_ex_vat,
      correction_item.amount_vat,
      correction_item.amount_inc_vat,
      correction_item.economic_key_type,
      correction_item.economic_key_value,
      correction_item.before_snapshot_json,
      correction_item.after_snapshot_json #> '{source_restoration}' AS source_restoration,
      batch_candidate.candidate_display_name,
      batch_candidate.candidate_tms_ref,
      correction_item.applied_at_utc
    FROM public.pay_payment_correction_items AS correction_item
    LEFT JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = correction_item.pay_batch_candidate_id
     AND batch_candidate.pay_batch_id = correction_item.pay_batch_id
    WHERE correction_item.status = 'APPLIED'
      AND correction_item.pay_batch_item_id IS NOT NULL
      AND correction_item.after_snapshot_json #>> '{source_restoration,status}' = 'NEEDS_INVESTIGATION'
      AND correction_item.after_snapshot_json #>> '{source_restoration,policy}' = 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION'
      AND (v_entity_kind IS NULL OR v_entity_kind = 'pay_batch_item')
      AND (p_entity_id IS NULL OR correction_item.pay_batch_item_id = p_entity_id)
      AND (
        COALESCE(p_include_acknowledged, false)
        OR NOT EXISTS (
          SELECT 1
          FROM public.banking_alert_acknowledgements AS investigation_ack
          WHERE investigation_ack.acknowledged_by_user_id = p_actor_user_id
            AND UPPER(BTRIM(COALESCE(investigation_ack.acknowledge_scope, 'USER'))) = 'USER'
            AND UPPER(BTRIM(COALESCE(investigation_ack.alert_kind, ''))) = 'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED'
            AND LOWER(BTRIM(COALESCE(investigation_ack.entity_kind, ''))) = 'pay_batch_item'
            AND investigation_ack.entity_id = correction_item.pay_batch_item_id
            AND investigation_ack.acknowledged_at_utc >= COALESCE(correction_item.applied_at_utc, '-infinity'::timestamptz)
        )
      )
    -- Do not apply the display-page limit here.  The established function
    -- computes its global unacknowledged count and signal hashes from the full
    -- current alert universe, then applies v_limit only to the returned detail
    -- rows.  Prematurely limiting this source would make those global fields
    -- incomplete (including when p_limit = 0 deliberately requests all rows).
  ),
  source_restoration_investigation_alerts AS MATERIALIZED (
    SELECT
      'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED'::text AS alert_kind,
      'ACTION_REQUIRED'::text AS severity,
      91::integer AS severity_rank,
      'pay_batch_item'::text AS entity_kind,
      investigation_row.pay_batch_item_id AS entity_id,
      investigation_row.pay_batch_id,
      'pay_payment_correction_item'::text AS payload_source_kind,
      investigation_row.correction_item_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'alert_kind', 'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',
        'issue_kind', 'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',
        'failure_reason_group', 'MANUAL_ADJUSTMENT_BLOCKER',
        'stable_issue_key', investigation_row.pay_batch_id::text || ':' || investigation_row.pay_batch_item_id::text || ':MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',
        'dedupe_key', investigation_row.pay_batch_id::text || ':' || investigation_row.pay_batch_item_id::text || ':MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',
        'payload_source_kind', 'pay_payment_correction_item',
        'payload_source_id', investigation_row.correction_item_id::text,
        'pay_batch_id', investigation_row.pay_batch_id::text,
        'pay_batch_item_id', investigation_row.pay_batch_item_id::text,
        'pay_batch_candidate_id', CASE WHEN investigation_row.pay_batch_candidate_id IS NULL THEN NULL ELSE investigation_row.pay_batch_candidate_id::text END,
        'candidate_id', CASE WHEN investigation_row.candidate_id IS NULL THEN NULL ELSE investigation_row.candidate_id::text END,
        'candidate_display_name', investigation_row.candidate_display_name,
        'candidate_tms_ref', investigation_row.candidate_tms_ref,
        'correction_item_kind', investigation_row.correction_item_kind,
        'item_type', investigation_row.item_type,
        'description', investigation_row.before_snapshot_json->>'description',
        'pay_channel', investigation_row.before_snapshot_json->>'pay_channel',
        'paye_treatment', investigation_row.before_snapshot_json->>'paye_treatment',
        'amount_ex_vat', investigation_row.amount_ex_vat,
        'amount_vat', investigation_row.amount_vat,
        'amount_inc_vat', investigation_row.amount_inc_vat,
        'economic_key_type', investigation_row.economic_key_type,
        'economic_key_value', investigation_row.economic_key_value,
        'source_link_status', 'MISSING_OR_AMBIGUOUS',
        'source_restoration_status', investigation_row.source_restoration->>'status',
        'source_restoration_reason', investigation_row.source_restoration->>'reason',
        'source_restoration_policy', investigation_row.source_restoration->>'policy',
        'source_reconstruction_performed', false,
        'source_carry_forward_created', false,
        'financial_cancellation_completed', true,
        'link_target', 'banking_pay_batch',
        'link_tab', 'current_payment_status',
        'user_label', 'Cancelled payment adjustment needs investigation',
        'user_description', 'The unpaid payment was cancelled safely. This frozen manual adjustment was not recreated because its original source link is missing or ambiguous.',
        'required_user_action', 'Open Current Payment Status and investigate the preserved adjustment evidence.'
      )) AS fingerprint_payload_json,
      'Cancelled payment adjustment needs investigation'::text AS label,
      'Cancelled payment adjustment needs investigation'::text AS title,
      'The unpaid payment was cancelled safely. This frozen manual adjustment was not recreated because its original source link is missing or ambiguous.'::text AS description,
      'Open Current Payment Status and investigate the preserved adjustment evidence.'::text AS action_guidance,
      investigation_row.applied_at_utc AS sort_at_utc
    FROM source_restoration_investigation_rows AS investigation_row
  ),
`;
  source = replaceExactly(
    source,
    '  latest_success_events AS MATERIALIZED (',
    `${investigationCtes}  latest_success_events AS MATERIALIZED (`,
    'investigation alert CTEs'
  );
  source = replaceExactly(
    source,
    '    UNION ALL\n    SELECT * FROM success_event_alerts\n  ),',
    "    UNION ALL\n    SELECT * FROM source_restoration_investigation_alerts\n    UNION ALL\n    SELECT * FROM success_event_alerts\n  ),",
    'investigation alert union'
  );
  assertAlertCompletenessSafety(source);
  return source;
}

export function patchPreference(definition, label) {
  return replaceExactly(
    definition,
    "    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',\n    'PAID_SETTLED_RECOVERY_REQUIRED',",
    "    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',\n    'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',\n    'PAID_SETTLED_RECOVERY_REQUIRED',",
    `${label} allowed alert kind`
  );
}

const finalActivationCutoverGuard = `-- Final activation is deliberately a zero-active cutover. The request lock is
-- acquired first, matching current request-start order (request, then operation),
-- and both locks are held through the function replacements and COMMIT.
LOCK TABLE public.pay_payment_correction_requests IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE public.banking_pay_operations IN SHARE ROW EXCLUSIVE MODE;

DO $payment_correction_zero_active_cutover$
DECLARE
  v_nonterminal_request_count integer := 0;
  v_nonterminal_operation_count integer := 0;
BEGIN
  -- Exact request terminal vocabulary from pay_payment_correction_requests_status_chk.
  -- NULL, blank, unexpected, or every admitted nonterminal status fails closed.
  SELECT pg_catalog.count(*)::integer
  INTO v_nonterminal_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(request_row.status)) NOT IN (
       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
       'FAILED', 'REJECTED', 'CANCELLED'
     );

  -- Exact operation terminal vocabulary from the current active-idempotency
  -- authority. REVIEW_REQUIRED is terminal/non-runnable, but remains audit data.
  SELECT pg_catalog.count(*)::integer
  INTO v_nonterminal_operation_count
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND (
      operation_row.status IS NULL
      OR pg_catalog.upper(pg_catalog.btrim(operation_row.status)) NOT IN (
        'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'
      )
    );

  IF v_nonterminal_request_count <> 0 OR v_nonterminal_operation_count <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED',
              'nonterminal_request_count', v_nonterminal_request_count,
              'nonterminal_operation_count', v_nonterminal_operation_count
            )::text;
  END IF;
END;
$payment_correction_zero_active_cutover$;`;

export function assertFinalActivationCutoverSafety(artifact) {
  assert.equal(occurrences(artifact, '\nbegin;\n'), 1, 'final activation: outer BEGIN count changed');
  assert.equal(occurrences(artifact, '\ncommit;\n'), 1, 'final activation: outer COMMIT count changed');
  assert.equal(
    occurrences(artifact, 'LOCK TABLE public.pay_payment_correction_requests IN SHARE ROW EXCLUSIVE MODE;'),
    1,
    'final activation: request lock changed'
  );
  assert.equal(
    occurrences(artifact, 'LOCK TABLE public.banking_pay_operations IN SHARE ROW EXCLUSIVE MODE;'),
    1,
    'final activation: operation lock changed'
  );
  const requestLock = artifact.indexOf('LOCK TABLE public.pay_payment_correction_requests');
  const operationLock = artifact.indexOf('LOCK TABLE public.banking_pay_operations');
  const firstDefinition = artifact.indexOf('CREATE OR REPLACE FUNCTION public.pay_payment_cancelability_diagnostic');
  assert.ok(requestLock > 0 && operationLock > requestLock && firstDefinition > operationLock, 'final activation: lock/definition order changed');
  assert.match(artifact, /request_row\.status IS NULL[\s\S]+NOT IN \(\n       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',\n       'FAILED', 'REJECTED', 'CANCELLED'\n     \)/);
  assert.match(artifact, /operation_row\.operation_type = 'PAYMENT_CORRECTION'[\s\S]+operation_row\.status IS NULL[\s\S]+NOT IN \(\n        'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'\n      \)/);
  assert.equal(occurrences(artifact, 'PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED'), 2);
  assert.doesNotMatch(artifact.slice(requestLock, firstDefinition), /UPDATE|INSERT|DELETE|MERGE|TRUNCATE/i);
  return artifact;
}

export function materialise() {
  const buffers = Object.fromEntries(Object.entries(sourcePaths).map(([key, filePath]) => [key, fs.readFileSync(filePath)]));
  for (const [key, expectedHash] of Object.entries(expectedSourceHashes)) {
    assert.equal(sha256(buffers[key]), expectedHash, `${key}: historical/current owner bytes changed`);
  }
  const sources = Object.fromEntries(Object.entries(buffers).map(([key, value]) => [key, lf(value.toString('utf8'))]));

  const diagnostic = patchDiagnostic(extractFunction(sources.monolith, 'public.pay_payment_cancelability_diagnostic'));
  const classifier = patchClassifier(extractFunction(sources.monolith, 'public._pay_payment_movement_classify'));
  const plan = patchPlan(extractFunction(sources.monolith, 'public.pay_payment_correction_plan'));

  const preBank = patchApply(
    extractFunction(sources.preBank, 'public.pay_pre_bank_cancel_apply_work_item'),
    {
      label: 'pre-bank apply',
      correctionKind: 'PRE_BANK_CANCEL',
      selectedTableStart: '  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_selected;',
      capacityMailStart: '  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_capacity_mail_scope;',
      capacityMailEnd: '  PERFORM 1\n  FROM public.pay_batch_items AS locked_instruction_items',
      applyMailStart: '  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_mail_scope_matches;',
      applyMailEnd: '  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)',
      manualBlockCondition: "  IF jsonb_array_length(COALESCE(v_manual_adjustment_result->'carry_forward_blockers', '[]'::jsonb)) > 0\n     OR COALESCE((v_manual_adjustment_result->>'can_carry_forward_automatically')::boolean, true) IS NOT TRUE THEN",
      resultFieldCount: 1,
      resultFieldCommaCount: 0,
      resultFieldNeedsTrailingComma: false,
      communicationResultCount: 1,
      membershipCommunicationCheckCount: 1
    }
  );
  const noMoney = patchApply(
    extractFunction(sources.noMoney, 'public.pay_no_money_unwind_apply_work_item'),
    {
      label: 'no-money apply',
      correctionKind: 'NO_MONEY_UNWIND',
      selectedTableStart: '  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_selected;',
      capacityMailStart: '  DROP TABLE IF EXISTS pg_temp._tmp_no_money_capacity_mail_scope;',
      capacityMailEnd: '  PERFORM 1\n  FROM public.pay_batch_items AS locked_instruction_items',
      applyMailStart: '  DROP TABLE IF EXISTS pg_temp._tmp_no_money_unwind_mail_scope_matches;',
      applyMailEnd: '  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)',
      manualBlockCondition: "  IF jsonb_array_length(COALESCE(v_manual_adjustment_result->'carry_forward_blockers', '[]'::jsonb)) > 0\n     OR COALESCE((v_manual_adjustment_result->>'can_carry_forward_automatically')::boolean, true) IS NOT TRUE THEN",
      resultFieldCount: 2,
      resultFieldCommaCount: 2,
      resultFieldNeedsTrailingComma: true,
      communicationResultCount: 2,
      membershipCommunicationCheckCount: 0,
      splitNoMoneyResultObject: true
    }
  );

  const selectionPrepare = patchSelectionPrepare(
    extractFunction(sources.selectionPrepare, 'public.pay_payment_correction_selection_prepare_chunk_v1')
  );
  const expandWork = patchExpandWork(
    extractFunction(sources.expandWork, 'public.pay_payment_correction_expand_work')
  );
  const integrityCheck = patchIntegrityCheck(
    extractFunction(sources.integrityCheck, 'public.pay_payment_correction_integrity_check_v1')
  );
  const statusAdmission = patchStatusAdmission(
    extractFunction(sources.statusPage, 'public.pay_batch_payment_status_page_v1')
  );

  const activeAlerts = patchAlerts(extractFunction(sources.monolith, 'public.banking_alerts_active_for_user'));
  const preferencesGet = patchPreference(extractFunction(sources.monolith, 'public.banking_alert_preferences_get'), 'preferences get');
  const preferencesUpdate = patchPreference(extractFunction(sources.monolith, 'public.banking_alert_preferences_update'), 'preferences update');

  const artifacts = {
    admission: wrapRepeatable(
      'Source-less unpaid cancellation admission: keep restoration truth but admit only complete proved-unpaid scopes.',
      [diagnostic, classifier, plan].join('\n\n'),
      `ALTER FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text) TO service_role;

ALTER FUNCTION public._pay_payment_movement_classify(uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_movement_classify(uuid,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public._pay_payment_movement_classify(uuid,jsonb) TO service_role;

ALTER FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text) TO service_role;`,
      finalActivationCutoverGuard
    ),
    apply: wrapRepeatable(
      'Source-less unpaid cancellation apply: preserve ambiguous frozen facts and decouple financial cancellation from mail.',
      [preBank, noMoney].join('\n\n'),
      `ALTER FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_pre_bank_cancel_apply_work_item(uuid,uuid) TO service_role;

ALTER FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid) TO service_role;`
    ),
    alert: wrapRepeatable(
      'Source-less cancellation investigation alert: one action-required alert per preserved correction item.',
      [activeAlerts, preferencesGet, preferencesUpdate].join('\n\n'),
      `ALTER FUNCTION public.banking_alerts_active_for_user(uuid,text,uuid,boolean,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_alerts_active_for_user(uuid,text,uuid,boolean,integer,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.banking_alerts_active_for_user(uuid,text,uuid,boolean,integer,text) TO service_role;

ALTER FUNCTION public.banking_alert_preferences_get(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_alert_preferences_get(uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_preferences_get(uuid) TO service_role;

ALTER FUNCTION public.banking_alert_preferences_update(uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_alert_preferences_update(uuid,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.banking_alert_preferences_update(uuid,jsonb) TO service_role;`
    ),
    communicationPrepare: wrapRepeatable(
      'Fresh unpaid cancellation communication V2: assign mail-independent authority before any membership exists.',
      selectionPrepare,
      `ALTER FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid) TO service_role;`
    ),
    communicationExpand: wrapRepeatable(
      'Unpaid cancellation communication V2 expansion: propagate the exact frozen communication contract.',
      expandWork,
      `ALTER FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_expand_work(uuid,uuid) TO service_role;`
    ),
    integrityCheck: wrapRepeatable(
      'Payment correction integrity: reconstruct each frozen selection hash using its exact versioned field set.',
      integrityCheck,
      `ALTER FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer) TO service_role;`
    ),
    statusAdmission: wrapRepeatable(
      'Source-less unpaid cancellation status admission: surface safe cancellation actions without weakening payment or freshness fences.',
      statusAdmission,
      `ALTER FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb) TO service_role;`
    )
  };

  assertFinalActivationCutoverSafety(artifacts.admission);

  return artifacts;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const artifacts = materialise();
  for (const [key, outputPath] of Object.entries(outputPaths)) {
    if (checkOnly) {
      assert.equal(fs.existsSync(outputPath), true, `${key}: generated output missing`);
      assert.equal(lf(fs.readFileSync(outputPath, 'utf8')), artifacts[key], `${key}: generated output drift`);
    } else {
      fs.writeFileSync(outputPath, artifacts[key]);
    }
  }

  console.log(JSON.stringify({
    ok: true,
    mode: checkOnly ? 'CHECK' : 'WRITE',
    sources: Object.fromEntries(Object.entries(sourcePaths).map(([key, filePath]) => [key, {
      path: path.relative(repoRoot, filePath).replaceAll('\\', '/'),
      sha256: expectedSourceHashes[key]
    }])),
    outputs: Object.fromEntries(Object.entries(outputPaths).map(([key, filePath]) => [key, {
      path: path.relative(repoRoot, filePath).replaceAll('\\', '/'),
      sha256: sha256(Buffer.from(artifacts[key])),
      bytes: Buffer.byteLength(artifacts[key])
    }]))
  }, null, 2));
}
