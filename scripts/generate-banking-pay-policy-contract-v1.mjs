import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

const root = process.cwd();
const outputArg = process.argv.find(value => value.startsWith('--output-dir='));
const outDir = outputArg
  ? path.resolve(root, outputArg.slice('--output-dir='.length))
  : path.join(root, 'codex_outputs', 'banking-pay-create-draft-policy-v1');
const frontendRoot = process.env.BANKING_MODAL_FRONTEND_ROOT || 'C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/TEST-Frontend';
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const fileSha = relative => sha256(fs.readFileSync(path.join(root, relative)));
const git = (...args) => execFileSync('git', args, { cwd: root, encoding: 'utf8' }).trim();
const source = (pathName, lines, symbol, proves) => ({ path: pathName, lines, symbol, proves });
const installedGitCommit = 'a849a25e5391b893f91aa4e5ada2c9794ba9244b';
const backendGitCommit = git('rev-parse', 'HEAD');
const backendTree = git('rev-parse', 'HEAD^{tree}');
execFileSync('git', ['merge-base', '--is-ancestor', installedGitCommit, backendGitCommit], { cwd: root, stdio: 'pipe' });
const pathsChangedSinceInstalledRelease = git('diff', '--name-only', `${installedGitCommit}..${backendGitCommit}`).split(/\r?\n/).filter(Boolean);
const bankingOrDraftPathsChangedSinceInstalledRelease = pathsChangedSinceInstalledRelease.filter(file =>
  file.startsWith('supabase/') || file === 'broker/src/index.js' || /banking|pay_|draft|timesheet/i.test(file)
);

const sourceFiles = {
  preview: 'supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql',
  pairScope: 'supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql',
  pairTransition: 'supabase/repeatable/21072026_1235_08_timesheet_correction_pair_transition_v1.sql',
  pairPreview: 'supabase/repeatable/02082026_2014_timesheet_correction_pair_lifecycle_preview_v1.sql',
  pairResidual: 'supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql',
  pairImportClaim: 'supabase/repeatable/21072026_1820_00a_import_apply_operation_claim_v2.sql',
  prepare: 'supabase/repeatable/19072026_2344_banking_pay_shared_selection_guard.sql',
  scopeSeed: 'supabase/repeatable/21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql',
  allocationSeed: 'supabase/repeatable/08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql',
  finance: 'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql',
  finalizer: 'supabase/repeatable/01092026_1459_banking_pay_signed_recovery_draft_v1.sql',
  correctionSelectedItems: 'supabase/repeatable/09082026_1403_pay_payment_correction_selected_items_draft_scope.sql',
  correctionSelectionPrepare: 'supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql',
  correctionRequestStart: 'supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql',
  correctionExpandWork: 'supabase/repeatable/04082026_1208_pay_payment_correction_expand_work.sql',
  correctionProcessChunk: 'supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql',
  preBankCancelApply: 'supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql',
  semanticReadyHelpers: 'supabase/repeatable/09082026_0712_banking_pay_semantic_ready_helpers.sql',
  batchCancel: 'supabase/repeatable/04082026_1206_pay_batch_cancel.sql',
  monolith: 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql',
  worker: 'broker/src/index.js',
  currentContract: 'supabase/release/current-contract.json'
};

for (const file of Object.values(sourceFiles)) {
  if (!fs.existsSync(path.join(root, file))) throw new Error(`Required source is missing: ${file}`);
}

const installedRoutines = {
  'public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)': '7ecd15aeaf8f0e21f5d834ba8603bcf8956e8452c5efdc4a7203ae178a633d3f',
  'public.pay_workbench_prepare_draft(uuid,uuid,jsonb,text,text,boolean,boolean,uuid,timestamptz,uuid,boolean,boolean)': '5a879bc899cff1b1f08a7da0f4cfc1705ef9d3065697f22a20c9b8609ffedfb3',
  'public.pay_workbench_prepare_draft_scope_seed(uuid,uuid,uuid,jsonb,text,jsonb)': '36e242fb406949ffe4112288d34a271bb32c4a5b61cd883c8e3fc7cb2ed2ecf7',
  'public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb)': 'ffe1c3815d078c2d03eb3875e98a7672b03b993e0b01709ac6eee855431d3924',
  'public.pay_batch_insert_candidates_from_preview(uuid,uuid,uuid,jsonb)': 'd5002d6ca7c4ae0f22978cbbf24bb035b95707af89be26c23581815bc3c2071f',
  'public.pay_batch_insert_items_from_preview(uuid,uuid,uuid,jsonb)': '4d0bfd29ac818046d9c9883e1e385578e8f4df7bc6b72aa0f419a50ebcbbc02f',
  'public.pay_batch_apply_finance_adjustments(uuid,text,uuid,numeric,date,uuid,jsonb)': '9e30eb496c9c5bb5b50ab4da12cd3ccc936b170ded5c9789b401702b14054ec9',
  'public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb)': '27b35e2b7f8472315e3e790e13ec150610c6cc3ced0fc59c3cd9927e1c5de875',
  'public.pay_batch_populate_candidate_summaries(uuid,text,uuid,uuid,jsonb)': '29b38712b6bc811f77d322a2ce63ff161026e2cebadec786ae2a66bf8680993d',
  'public.pay_batch_create_timesheet_snapshots(uuid,uuid,uuid,jsonb)': '8972c7224c49d6dbf6d11886af6b3490d8c40368e5a0dda2f1fdb7a60a5cc56f',
  'public.pay_batch_build_item_breakdowns(uuid,uuid,uuid,jsonb)': '620b46dfca3e2eaa20d12a34af63463ae71788d20369b5794863de76c5e1019a',
  'public.pay_batch_assert_integrity(uuid,uuid,uuid)': '6ab0b9b8fc5f2ceab2798db318da0ff00edc3c21da2bb97eefc6d0cf68b0d8ac',
  'public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer)': '4b3e403373a1eb004a061edc196ee56d5b5b8c7199a6bab49e9459534e7e3fb9',
  'public.timesheet_correction_pair_transition_v1(uuid,text,uuid,uuid,text,boolean,integer)': '819f0f7e85f4b111347e52fe5b5c2c7014c53776583623a518a92ddf524b1bc6',
  'public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer)': '8ecf420f5a6f25d6e7e28a052e1594b7aa2e033b81f85526b79cca09a2c13a71',
  'public.pay_correction_chain_residual_v1(uuid,uuid,text,uuid,uuid,integer)': 'ab3f069966cbc77e4a26c8fb7e89cc91195200de7aa17e588e6384437d51a87f',
  'public.pay_set_paye_net_manual(uuid,jsonb,uuid)': '67bd40c49284fad3c22177987829506b1e36c1edcd392826c000bdbabab68cec',
  'public.pay_batch_execution_summary_get(uuid,uuid,text)': '74a7e6845c67592bd7aff10aa67894f9ace795a4852979df9a4a0ce0d79b8207',
  'public._pay_batch_bank_payment_projection_rows(uuid,text)': '4ed334ff8314f9e7c6325650b431fafb8c8db0a03ed9d7ebfcc7bcadff89384d',
  'public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb)': '14ef0f93702c8d07a3cce7f572a0a0e9f5da677b61c473e557cb4a52859052dd',
  'public.pay_bank_csv_export_summary_get(uuid,text,uuid)': 'a3f3d9858e2de5963d8df34535cf9576f2e363daef8286cbdc9b086fe18623d0',
  'public.pay_execute_bank_transfer_scope_seed(uuid,uuid,text,uuid,boolean)': '8dfdca201c80304b9278365f205070304acf637e96503c22e255824afd91bc35',
  'public.pay_execute_bank_transfer_chunk_prepare(uuid,uuid,jsonb,uuid)': '9b276ea60596c256380ad7d8d97ad9c223b66c05069e1b9943703a55d752b67c',
  'public.pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer)': '64419bd47ef2ad466fc18bf7f2b0c5749795c61cb25bff2edd4d871d266410ff',
  'public.pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb)': '0e5c3e38a314945f67ca716c5e6897ca9ed9730d7bbfd8e39342e31bdf26a83f',
  'public.pay_operation_remittance_scope_seed(uuid,uuid,text,uuid)': '140c6bada203f524cfce636c1295453ffe935ae910bf1b1745c92cd00a44ea33',
  'public.pay_remittance_build(uuid,text,uuid,jsonb,integer,jsonb)': 'fb6d237cee972eaa2d454439c2d8af32111da9bb448ebc2252d8d20e25ad3c1c',
  'public.pay_remittance_maybe_queue_for_trigger(uuid,text,text,uuid,boolean,uuid,boolean)': '1745985bd2a722750514f77a5ce5f4ae5744b23dc2c4f0c37f3a8e53faad54db',
  'public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb)': '104d0460c50d4ccdb3e37a6e74d7a8de7a134445e66f3d2c05b15fd65298efb5',
  'public._pay_payment_correction_selected_items(uuid,jsonb,boolean)': 'dc89247906e064fea22d15ebdce07925118aa4fd4624c0dbb5ed9fa0dc3d247d',
  'public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid)': '784e3ee63dd7783a969c4c03956d3a021c441aaf94f9df0f328d643cdefaba42',
  'public.pay_payment_correction_expand_work(uuid,uuid)': '3bcf21c8b46a64ce8dee0e9752606056149bc590669ea6065e674c50bdc705a7',
  'public.pay_payment_correction_process_chunk(uuid,integer,text,uuid)': '34b334b3ad89d9de684e0fb86fe1b4f09ba3c4fc7802c53730a1a45e51ecf75a',
  'private.pay_pre_bank_cancel_apply_work_page_v1(uuid,uuid[],uuid,jsonb)': 'ede8dfe93aca76d294b59f4ab59211020fec2ac139d6f753a4c9ce1759eb4b46',
  'private.pay_workbench_draft_overlay_remove_page_v1(uuid,uuid,uuid,uuid,integer,integer,uuid,jsonb)': 'c1868e301765ce99d99e69359b99a8433a358e8742d5f835bf77cd2a7524e2c7',
  'public.pay_pre_bank_cancel_apply_work_item(uuid,uuid)': '026ace84cc320562ea9d08631b68f9517dbcdbb3a3c9165b40bdcd1c087a6d22',
  'public.pay_settled_payment_reversal_apply_work_item(uuid,uuid)': 'd985e3af4f75e30654d7952b98172a826f5ec93909581d34f4eacee83656c296',
  'public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text)': 'c2df38403908cefa577c4e593223ca9dda1b708d4e0e1075d395ea2056d07843',
  'public.pay_batch_cancel(uuid,uuid,text,uuid,uuid)': '10c3ffcaab90dddc256d29ab3df82b1d01d6eaf3a302688a6ba82f04234fb573'
};

const grossNetRules = [
  {
    rule_id: 'PAYE_GROSS_ADD', meaning: 'Taxable value belongs inside payroll gross. The saved PAYE net already reflects it, so the bank amount must not add it again.',
    applicable_sign: 'POSITIVE', bank_scalar_owner: 'saved PAYE net', forbidden: 'Adding the item again after imported net.'
  },
  {
    rule_id: 'PAYE_GROSS_DEDUCT', meaning: 'Taxable deduction belongs inside payroll gross. The saved PAYE net already reflects it, so the bank amount must not deduct it again.',
    applicable_sign: 'NEGATIVE', bank_scalar_owner: 'saved PAYE net', forbidden: 'Deducting the item again after imported net.'
  },
  {
    rule_id: 'PAYE_NET_ADD', meaning: 'Fixed or non-taxable credit is added after the imported payroll net.',
    applicable_sign: 'POSITIVE', bank_scalar_owner: 'saved PAYE net plus certified NET_ADD items', forbidden: 'Putting it into payroll gross or applying it twice.'
  },
  {
    rule_id: 'PAYE_NET_DEDUCT', meaning: 'Fixed or non-taxable deduction is taken after the imported payroll net.',
    applicable_sign: 'NEGATIVE', bank_scalar_owner: 'saved PAYE net less certified NET_DEDUCT items', forbidden: 'Putting it into payroll gross or applying it twice.'
  },
  {
    rule_id: 'UMBRELLA_NONE', meaning: 'PAYE gross/net treatment does not apply. Existing Umbrella ex-VAT, VAT, inclusive amount, payee and channel owners remain authoritative.',
    applicable_sign: 'EITHER', bank_scalar_owner: 'frozen Umbrella payment projection', forbidden: 'Reusing PAYE net arithmetic or inventing VAT.'
  }
];

const vocabularyRegistry = {
  pay_channels: ['PAYE', 'UMBRELLA'],
  paye_treatments: ['GROSS_ADD', 'GROSS_DEDUCT', 'NET_ADD', 'NET_DEDUCT', 'NONE'],
  readiness_sections: ['READY_TO_PAY', 'CASES_RESOLUTIONS', 'BLOCKED_FOR_PAY'],
  visible_payment_line_types: [
    'TIMESHEET_PAYMENT', 'LOAN_PAYOUT', 'PAYMENT_ADVANCE_REPAYMENT', 'OVERPAYMENT_RECOVERY',
    'UNDERPAYMENT_PAYMENT', 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT', 'MANUAL_DEBT_RECOVERY',
    'MANUAL_ADJUSTMENT_CARRY_FORWARD'
  ],
  deliberately_hidden_or_frozen_aliases: ['LOAN_REPAYMENT', 'MANUAL_CREDIT_PAYOUT'],
  frozen_item_types: [
    'SEGMENT_DELTA', 'EXPENSE_DELTA', 'MILEAGE_DELTA', 'LOAN_PAYOUT', 'LOAN_REPAYMENT',
    'OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT', 'MANUAL_CREDIT_PAYOUT', 'MANUAL_DEBT_RECOVERY'
  ],
  finance_case_types: ['PAYMENT_ADVANCE', 'OVERPAYMENT', 'UNDERPAYMENT', 'MANUAL_CREDIT_ADJUSTMENT', 'MANUAL_DEBT_ADJUSTMENT'],
  economic_key_types: ['TS_DAY', 'TS_TOTAL', 'EXPENSE_CODE', 'ADDITIONAL_CODE', 'ADJUSTMENT_CODE'],
  expense_codes: ['EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE'],
  correction_member_kinds: ['CHANGED_HOURS_REVERSAL', 'CHANGED_HOURS_REPLACEMENT', 'CANCELLATION_REVERSAL', 'CANCELLATION_REPLACEMENT'],
  correction_shapes: ['REVERSAL_REPLACEMENT', 'REVERSAL_ONLY'],
  lifecycle_actions: ['AUTHORISE', 'UNAUTHORISE', 'PROCESS', 'UNPROCESS'],
  mapping_rule: 'Every token is owned by its source layer. A rewrite may translate only through the explicit visible→frozen mappings in the payment-family rows; it may not merge, rename or infer aliases.'
};

const commonDraftArtifacts = [
  'pay_batches', 'pay_batch_candidates', 'pay_batch_items', 'banking_pay_operation_candidate_allocation_rows',
  'pay_advance_reservations', 'timesheet_financial_retention', 'pay_batch_timesheet_snapshots',
  'pay_batch_item_breakdowns', 'pay_advances/pay_finance_case_components/pay_finance_case_events effects', 'operation-scoped expected-effect attestations',
  'post-Draft authority', 'source-publication and fast-reversion lineage', 'operation result and created batch IDs',
  'audit records and grouped ownership evidence'
];

const family = (family_id, title, body) => ({ family_id, title, ...body });
const families = [
  family('ordinary_timesheet_components', 'Ordinary Timesheet hours and rate components', {
    real_world_event: 'A processed, authorised Timesheet has an unpaid current entitlement.',
    visible_aliases: ['TIMESHEET_PAYMENT'], frozen_item_types: ['SEGMENT_DELTA'],
    identity: ['physical Timesheet ID', 'segment/component source identity', 'economic key TS_DAY or TS_TOTAL', 'rate/revision/source fingerprint'],
    selection: 'Only Ready components are selectable. Action Required, Blocked, snoozed, ineligible, superseded and already-active-Draft components are excluded.',
    amount_owner: 'Current pre-Draft entitlement minus settled baseline minus active source reservation, at exact component identity.',
    variants: [
      { channel: 'PAYE', sign: 'source residual', paye_treatment: 'GROSS_ADD for ordinary positive earnings', vat: 'not an Umbrella VAT calculation' },
      { channel: 'UMBRELLA', sign: 'source residual', paye_treatment: 'NONE', vat: 'existing frozen Umbrella ex-VAT/VAT/inclusive owners' }
    ],
    draft_result: 'One or more frozen SEGMENT_DELTA items retain the exact component/economic identities; totals are a consequence, not the identity.',
    fail_closed: ['missing or ambiguous rate/payment method', 'negative ordinary parent without certified recovery shape', 'stale source revision', 'duplicate/missing selected identity'],
    source_evidence: [
      source(sourceFiles.preview, '1330-1449', 'canonical_timesheet_presentation_rows', 'TIMESHEET_PAYMENT readiness, PAYE treatment and amount'),
      source(sourceFiles.preview, '2270-2564', 'timesheet_allocation_component_lines', 'component-level economic identity and frozen item routing')
    ]
  }),
  family('paired_timesheet_reversal_replacement', 'Paired Timesheets: reversal plus replacement', {
    real_world_event: 'An import-authoritative changed-hours correction replaces an earlier Timesheet by creating one reversal leg and one replacement leg under one correction operation.',
    visible_aliases: ['CHANGED_HOURS_REVERSAL', 'CHANGED_HOURS_REPLACEMENT'], frozen_item_types: ['SEGMENT_DELTA'],
    identity: ['root Timesheet ID', 'correction operation ID', 'correction chain ID', 'ordered member Timesheet IDs', 'pair/chain fingerprint', 'per-leg policy fingerprint', 'TS_DAY economic key'],
    selection: 'The correction unit is valid only when it has exactly one reversal and one replacement with the expected envelope, count, roles and fingerprints. Authorise/unauthorise/process/unprocess transitions are atomic across both legs.',
    amount_owner: 'The correction residual owner sums current TS_DAY truth, subtracts settled baseline and active reservations, then accounts for settled/reserved correction finance movements. It does not include expenses, mileage, additional codes, loans or unrelated overpayments.',
    variants: [
      { channel: 'PAYE', sign: 'net correction residual by TS_DAY', paye_treatment: 'TAXABLE_CHANNEL_SENSITIVE; target resolution required if source channel differs', vat: 'PAYE target amount is never inferred from Umbrella' },
      { channel: 'UMBRELLA', sign: 'net correction residual by TS_DAY', paye_treatment: 'NONE', vat: 'target ex-VAT/VAT amount requires existing channel authority; never converted by guess' }
    ],
    draft_result: 'The pair is not collapsed into one invented payment. Frozen constituents preserve the ordered pair lineage and component identity, while the latest positive leg may act as the carrier Timesheet.',
    cancellation_reversion: 'Reversion must restore/release both the frozen component lineage and the pair/chain evidence consistently; never revive just one leg as an unrelated payable row.',
    fail_closed: ['broken pair', 'duplicate role', 'member-count mismatch', 'cycle/depth overflow', 'mixed worker or mixed client', 'stale chain or leg fingerprint', 'paid/invoiced lifecycle conflict', 'unresolved cross-channel target amount', 'reservation overrun'],
    test_occurrence: 'No qualifying current business pair rows were returned by the bounded Miget TEST aggregate on 2026-09-02; source and deterministic fixture coverage are therefore mandatory.',
    source_evidence: [
      source(sourceFiles.pairScope, '41-236', 'timesheet_correction_chain_scope_v1', 'bounded chain, exact unit shape, fingerprints and member order'),
      source(sourceFiles.pairTransition, '41-197', 'timesheet_correction_pair_transition_v1', 'atomic lifecycle, per-leg evidence and paid/invoiced guards'),
      source(sourceFiles.pairResidual, '105-338', 'pay_correction_chain_residual_v1', 'single-worker/client/channel and policy-anchor validation'),
      source(sourceFiles.pairResidual, '340-1300', 'pay_correction_chain_residual_v1', 'TS_DAY residual, reservations, settlements, stable identity and draftability')
    ]
  }),
  family('paired_timesheet_reversal_only', 'Paired Timesheet lifecycle: reversal-only cancellation correction', {
    real_world_event: 'An import-authoritative cancellation removes previously accepted hours without a positive replacement leg.',
    visible_aliases: ['CANCELLATION_REVERSAL', 'REVERSAL_ONLY'], frozen_item_types: ['SEGMENT_DELTA or certified recovery output selected by existing owner'],
    identity: ['root Timesheet ID', 'one reversal member', 'correction operation/chain IDs', 'envelope and leg fingerprints', 'TS_DAY key'],
    selection: 'Expected member count is one, reversal count is one and replacement count is zero. The same atomic lifecycle and stale/paid/invoiced guards apply.',
    amount_owner: 'The same correction residual owner; no second cancellation arithmetic is permitted.',
    variants: [
      { channel: 'PAYE', sign: 'source-proved residual, often recovery/zero after baselines', paye_treatment: 'existing signed recovery and PAYE owners only', vat: 'not guessed' },
      { channel: 'UMBRELLA', sign: 'source-proved residual', paye_treatment: 'NONE', vat: 'existing frozen Umbrella authority only' }
    ],
    draft_result: 'Only the actual residual is frozen. A reversal row is not automatically converted into a payment or recovery merely because it is negative.',
    fail_closed: ['unexpected replacement', 'wrong expected count/roles', 'unproved signed recovery evidence', 'stale envelope/leg', 'mixed worker/client/channel'],
    member_deletion_policy: {
      canonical_reversal_only: 'A source cancellation creates REVERSAL_ONLY directly with one reversal and no replacement.',
      remove_positive_replacement_only: 'User-confirmed valid outcome, but current standard delete does not implement it: the changed-hours pair is presently one two-row delete target. A future upstream transition must atomically remove the replacement and republish the survivor as a fully certified REVERSAL_ONLY unit before Draft.',
      remove_negative_reversal_only: 'Strictly prohibited. A positive-only unit has no reversal and fails the exact correction-chain validator.',
      remove_both: 'Valid cancellation of the correction when the existing delete/retention owner permits it. The current standard delete targets both members; retained financial history may require archive rather than physical deletion.',
      draft_safety_boundary: 'Never make Create Draft accept a REVERSAL_REPLACEMENT unit with a missing member. That is corruption, not a valid reversal-only correction.'
    },
    source_evidence: [
      source(sourceFiles.pairScope, '101-157', 'timesheet_correction_chain_scope_v1', 'REVERSAL_ONLY exact shape'),
      source(sourceFiles.pairResidual, '1-26', 'pay_correction_chain_residual_v1', 'narrow residual scope and exclusions'),
      source(sourceFiles.pairImportClaim, '45-104', '_import_apply_operation_claim_core_v2', 'CANCELLATION maps only to REVERSAL_ONLY; CHANGED_HOURS maps only to REVERSAL_REPLACEMENT'),
      source(sourceFiles.monolith, '182957-183312', 'timesheet_standard_delete_preview_v1', 'current changed-hours pair is an exact two-row delete target and malformed/single-child shapes block'),
      source(sourceFiles.monolith, '153787-154375', 'timesheet_standard_delete_apply_v1', 'apply consumes and deletes the exact preview target set; it does not reclassify one surviving member')
    ]
  }),
  family('timesheet_expenses_and_mileage', 'Timesheet expenses, travel, accommodation, other and mileage', {
    real_world_event: 'An authorised Timesheet contains an unpaid expense component.',
    visible_aliases: ['EXPENSES', 'TRAVEL', 'ACCOMMODATION', 'OTHER', 'MILEAGE'], frozen_item_types: ['EXPENSE_DELTA', 'MILEAGE_DELTA'],
    identity: ['Timesheet ID', 'EXPENSE_CODE economic key', 'expense code', 'source-basis fingerprint'],
    selection: 'Each Ready expense component is selected independently. A blocked/snoozed expense does not disappear an unrelated Ready hours or expense component.',
    amount_owner: 'Current component amount ex VAT after settled baseline and active reservation.',
    variants: [
      { channel: 'PAYE', sign: 'source residual', paye_treatment: 'GROSS_ADD in the current canonical component projection', vat: 'no Umbrella VAT computation' },
      { channel: 'UMBRELLA', sign: 'source residual', paye_treatment: 'NONE', vat: 'preserve component ex-VAT and existing Umbrella VAT snapshot/rate' }
    ],
    draft_result: 'MILEAGE maps to MILEAGE_DELTA; the other expense codes map to EXPENSE_DELTA. Source keys and expense code remain distinct.',
    fail_closed: ['missing expense code/fingerprint', 'unsupported expense code', 'stale source basis', 'snoozed or blocked component'],
    source_evidence: [
      source(sourceFiles.preview, '2270-2564', 'timesheet_allocation_component_lines', 'EXPENSE_CODE identity, MILEAGE routing and channel treatment'),
      source(sourceFiles.preview, '3169-3227', 'candidate totals', 'separate expense-code totals')
    ]
  }),
  family('additional_and_adjustment_components', 'Additional-code and adjustment-code Timesheet components', {
    real_world_event: 'A Timesheet has a separately identified additional or adjustment component.',
    visible_aliases: ['ADDITIONAL_CODE', 'ADJUSTMENT_CODE'], frozen_item_types: ['EXPENSE_DELTA or existing component-specific output'],
    identity: ['Timesheet ID', 'component key type', 'component key value', 'source/revision fingerprint'],
    selection: 'Each component is independent even when another component shares a date or total.',
    amount_owner: 'Current component entitlement less exact settled baseline and reservation.',
    variants: [
      { channel: 'PAYE', sign: 'source residual', paye_treatment: 'existing component treatment; no reclassification by label', vat: 'not inferred' },
      { channel: 'UMBRELLA', sign: 'source residual', paye_treatment: 'NONE', vat: 'existing component VAT authority' }
    ],
    draft_result: 'Stable key and source identity survive; a label collision must not turn a component into a finance case.',
    fail_closed: ['ambiguous key', 'duplicate exact identity', 'stale fingerprint'],
    source_evidence: [source(sourceFiles.preview, '2270-2564', 'timesheet_allocation_component_lines', 'key-type-specific identity and item routing')]
  }),
  family('forced_advance_this_payment', 'Forced Timesheet “advance this payment”', {
    real_world_event: 'An authorised override permits a particular otherwise-timed Timesheet payment into this pay run.',
    visible_aliases: ['ADVANCE_THIS_PAYMENT'], frozen_item_types: ['ordinary Timesheet component item types'],
    identity: ['Timesheet/component identity', 'override ID', 'reason', 'actor and lifecycle evidence'],
    selection: 'The override changes timing eligibility only; it does not change the money, channel, rate, tax or component identity.',
    amount_owner: 'The same Timesheet/component financial owner as an ordinary payment.',
    variants: [
      { channel: 'PAYE', sign: 'unchanged source residual', paye_treatment: 'unchanged ordinary PAYE treatment', vat: 'unchanged' },
      { channel: 'UMBRELLA', sign: 'unchanged source residual', paye_treatment: 'NONE', vat: 'unchanged Umbrella authority' }
    ],
    draft_result: 'Ordinary frozen artifacts plus the existing override evidence; no loan or finance-case item is invented.',
    fail_closed: ['cancelled/stale override', 'wrong Timesheet', 'override without authority'],
    source_evidence: [source(sourceFiles.preview, '1330-1449', 'canonical_timesheet_presentation_rows', 'is_advanced, override ID and reason without amount rewrite')]
  }),
  family('prior_paid_part_paid_and_superseded', 'Already-paid, part-paid, reserved and superseded source treatment', {
    real_world_event: 'Some or all of a source component has already been paid, reserved or superseded.',
    visible_aliases: ['component residual or absence'], frozen_item_types: ['the original family item type for the remaining exact residual'],
    identity: ['same physical/economic component identity', 'settled baseline', 'active reservation', 'supersession/current-source fingerprint'],
    selection: 'Fully settled or superseded authority is absent from Ready. A part-paid component contributes only its exact remaining amount. Cancelling an untouched Draft may release it to appear once again.',
    amount_owner: 'truth minus settled baseline minus active reservation, with source-owned recovery/underpayment movements where applicable.',
    variants: [{ channel: 'PAYE or UMBRELLA', sign: 'residual sign', paye_treatment: 'unchanged from original family', vat: 'unchanged from original family' }],
    draft_result: 'No duplicate full payment; the exact residual and lineage are frozen.',
    fail_closed: ['reservation overrun', 'same identity already active in another Draft', 'stale supersession/current revision'],
    source_evidence: [source(sourceFiles.pairResidual, '343-467', 'raw_outstanding', 'truth minus baseline minus reservation equation')]
  })
];

const financeFamilies = [
  {
    family_id: 'payment_advance_payout', title: 'Payment advance / loan payout', case_type: 'PAYMENT_ADVANCE',
    production_condition: 'Lifecycle is not PAID.', visible_alias: 'LOAN_PAYOUT', frozen_item_type: 'LOAN_PAYOUT', direction: 'CREDIT', sign: 'POSITIVE',
    paye: 'NET_ADD', umbrella: 'NONE with existing ex-VAT/VAT/channel/payee authority', headroom: 'not a recovery deduction',
    meaning: 'The agency pays the worker an advance before it becomes a repayment schedule.'
  },
  {
    family_id: 'payment_advance_repayment', title: 'Payment advance repayment', case_type: 'PAYMENT_ADVANCE',
    production_condition: 'Lifecycle is PAID and a repayment is due.', visible_alias: 'PAYMENT_ADVANCE_REPAYMENT', frozen_item_type: 'LOAN_REPAYMENT', direction: 'DEDUCTION', sign: 'NEGATIVE',
    paye: 'NET_DEDUCT', umbrella: 'NONE with existing ex-VAT/VAT/channel/payee authority', headroom: 'bounded by ordinary positive headroom and case residual',
    meaning: 'The worker repays a previously paid advance. LOAN_REPAYMENT is the hidden/frozen item vocabulary, not a second visible product.'
  },
  {
    family_id: 'overpayment_recovery', title: 'Overpayment recovery', case_type: 'OVERPAYMENT',
    production_condition: 'A valid overpayment balance remains recoverable.', visible_alias: 'OVERPAYMENT_RECOVERY', frozen_item_type: 'OVERPAYMENT_RECOVERY', direction: 'DEDUCTION', sign: 'NEGATIVE',
    paye: 'TAXABLE → GROSS_DEDUCT; NON_TAXABLE → NET_DEDUCT', umbrella: 'NONE with existing ex-VAT/VAT/channel/payee authority', headroom: 'zero/exact/partial recovery against candidate+channel ordinary positive headroom in deterministic case order',
    meaning: 'The worker owes back a previous overpayment, but no more than current policy permits in this run.'
  },
  {
    family_id: 'underpayment_payment', title: 'Underpayment payment', case_type: 'UNDERPAYMENT',
    production_condition: 'A valid underpayment balance remains due.', visible_alias: 'UNDERPAYMENT_PAYMENT', frozen_item_type: 'UNDERPAYMENT_PAYMENT', direction: 'CREDIT', sign: 'POSITIVE',
    paye: 'TAXABLE → GROSS_ADD; NON_TAXABLE → NET_ADD', umbrella: 'NONE with existing ex-VAT/VAT/channel/payee authority', headroom: 'not a recovery deduction',
    meaning: 'The agency owes the worker an amount previously underpaid.'
  },
  {
    family_id: 'manual_credit_adjustment', title: 'Manual credit adjustment payment', case_type: 'MANUAL_CREDIT_ADJUSTMENT',
    production_condition: 'An authorised manual credit remains payable.', visible_alias: 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT', frozen_item_type: 'MANUAL_CREDIT_PAYOUT', direction: 'CREDIT', sign: 'POSITIVE',
    paye: 'TAXABLE → GROSS_ADD; NON_TAXABLE → NET_ADD', umbrella: 'NONE with existing ex-VAT/VAT/channel/payee authority', headroom: 'not a recovery deduction',
    meaning: 'An authorised manual correction increases what the worker is paid. The visible and frozen names deliberately differ.'
  },
  {
    family_id: 'manual_debt_adjustment', title: 'Manual debt adjustment recovery', case_type: 'MANUAL_DEBT_ADJUSTMENT',
    production_condition: 'An authorised manual debt remains recoverable.', visible_alias: 'MANUAL_DEBT_RECOVERY', frozen_item_type: 'MANUAL_DEBT_RECOVERY', direction: 'DEDUCTION', sign: 'NEGATIVE',
    paye: 'TAXABLE → GROSS_DEDUCT; NON_TAXABLE → NET_DEDUCT', umbrella: 'NONE with existing ex-VAT/VAT/channel/payee authority', headroom: 'zero/exact/partial recovery against candidate+channel ordinary positive headroom in deterministic case order',
    meaning: 'An authorised manual correction reduces what the worker is paid.'
  }
].map(row => family(row.family_id, row.title, {
  real_world_event: row.meaning,
  visible_aliases: [row.visible_alias], frozen_item_types: [row.frozen_item_type],
  identity: ['finance case ID', 'finance component ID', 'case type', 'component key type/value', 'linked Timesheet when present', 'source basis fingerprint'],
  selection: `Ready only when ${row.production_condition} It remains blocked when snoozed, unresolved, invalid, zero due or otherwise case-blocked.`,
  amount_owner: 'The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.',
  variants: [
    { channel: 'PAYE', sign: row.sign, paye_treatment: row.paye, vat: 'PAYE scalar rules apply' },
    { channel: 'UMBRELLA', sign: row.sign, paye_treatment: row.umbrella, vat: 'existing Umbrella ex-VAT/VAT/inclusive amount, payee and channel rules' }
  ],
  headroom: row.headroom,
  draft_result: `Visible ${row.visible_alias} becomes frozen ${row.frozen_item_type}; allocation linkage and finance-case/component lineage remain exact.`,
  fail_closed: ['unsupported or hidden visible alias', 'wrong finance case/component/linkage', 'stale case residual', 'channel mismatch', 'snoozed/blocked/unresolved case', 'incorrect pre-finance ordinary item link'],
  source_evidence: [
    source(sourceFiles.preview, '1771-1970', 'finance_case_lines', 'case type → visible alias, direction, sign and PAYE treatment'),
    source(sourceFiles.preview, '2792-2974', 'canonical_preview_lines finance branch', 'stable visible identity, headroom, readiness and source_ref'),
    source(sourceFiles.finance, '1-end', 'pay_batch_apply_finance_adjustments', 'unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner')
  ]
}));
families.push(...financeFamilies);

families.push(
  family('manual_adjustment_carry_forward', 'Manual adjustment carry-forward after correction/reversion', {
    real_world_event: 'A prior correction produces a certified amount that must be carried into a later Draft.',
    visible_aliases: ['MANUAL_ADJUSTMENT_CARRY_FORWARD'], frozen_item_types: ['source-owned carry-forward item type'],
    identity: ['carry-forward ID', 'operation source key', 'source batch/item/transfer/correction lineage', 'candidate/channel/payee'],
    selection: 'Only READY/current unconsumed carry-forward authority is draftable.',
    amount_owner: 'Stored signed ex-VAT/VAT/inclusive amounts and stored tax treatment from the correction owner.',
    variants: [{ channel: 'PAYE or UMBRELLA', sign: 'CREDIT when positive; DEBIT when negative', paye_treatment: 'stored source treatment', vat: 'stored source VAT treatment' }],
    draft_result: 'The stored source economics and complete correction lineage are frozen without reinterpretation.',
    fail_closed: ['missing source key', 'already consumed target', 'candidate/channel/payee mismatch', 'stale correction lineage'],
    source_evidence: [source(sourceFiles.preview, '2978-3070', 'manual carry-forward canonical lines', 'identity, signed values, tax/VAT and source lineage')]
  }),
  family('signed_non_charge_recovery', 'Signed recovery/return from frozen historical component evidence', {
    real_world_event: 'A historical frozen document proves an exact signed non-charge movement for a component.',
    visible_aliases: ['certified signed recovery evidence'], frozen_item_types: ['existing recovery/return type chosen by owner'],
    identity: ['full economic key', 'amount/bucket/revision/target/conversion', 'sealed digest', 'decisive component ID'],
    selection: 'Cardinality applies only after full signed pre-signature filtering; ordinary same-key components do not count as signed evidence.',
    amount_owner: 'Existing signed-recovery classifier and recovery owner; no label or total-based inference.',
    variants: [{ channel: 'PAYE or UMBRELLA', sign: 'positive return or negative recovery as proved', paye_treatment: 'existing frozen evidence', vat: 'existing frozen evidence' }],
    draft_result: 'Exact decisive component shape/digest is carried into finalisation and reservation evidence.',
    fail_closed: ['zero or multiple decisive signed matches when one is required', 'tampered/missing digest', 'ordinary same-key false positive', 'amount/key/revision/conversion mismatch'],
    source_evidence: [source(sourceFiles.finalizer, '1-end', 'pay_batch_signed_non_charge_recovery_evidence_v1 and finalizer', 'signed classification and frozen evidence checks')]
  })
);

const equivalenceClassRows = [
  ['ordinary_one_segment_paye','ordinary_timesheet_components','one Ready TS_DAY component','PAYE'],
  ['ordinary_one_segment_umbrella','ordinary_timesheet_components','one Ready TS_DAY component','UMBRELLA'],
  ['ordinary_multi_segment_paye','ordinary_timesheet_components','multiple independently keyed Ready TS_DAY components','PAYE'],
  ['ordinary_multi_segment_umbrella','ordinary_timesheet_components','multiple independently keyed Ready TS_DAY components','UMBRELLA'],
  ['ordinary_multiple_rate_families','ordinary_timesheet_components','DAY/NIGHT/SAT/SUN/BH/additional rates remain distinct','PAYE+UMBRELLA'],
  ['ordinary_same_key_multiple_components','ordinary_timesheet_components','multiple rate/correction components may share TS_DAY or TS_TOTAL and remain distinct physical constituents','PAYE+UMBRELLA'],
  ['saved_rate_resolution','ordinary_timesheet_components','one current saved resolution binds exact component/source fingerprint and target amount','PAYE+UMBRELLA'],
  ['saved_payment_method_resolution','ordinary_timesheet_components','one current saved pay-channel resolution is consumed; the Draft never guesses a channel','PAYE↔UMBRELLA'],
  ['paired_reversal_replacement_paye','paired_timesheet_reversal_replacement','exact two-leg pair, source and target PAYE','PAYE'],
  ['paired_reversal_replacement_umbrella','paired_timesheet_reversal_replacement','exact two-leg pair, source and target Umbrella','UMBRELLA'],
  ['paired_cross_channel_resolution','paired_timesheet_reversal_replacement','source method differs from target and requires one fresh saved target resolution','PAYE↔UMBRELLA'],
  ['paired_broken_or_duplicate_leg','paired_timesheet_reversal_replacement','missing/duplicate role must fail closed','ALL'],
  ['paired_stale_fingerprint','paired_timesheet_reversal_replacement','stale chain/envelope/leg fingerprint must fail closed','ALL'],
  ['paired_mixed_candidate_or_client','paired_timesheet_reversal_replacement','pair members span worker/client and must fail closed','ALL'],
  ['paired_paid_or_invoiced_conflict','paired_timesheet_reversal_replacement','partial lifecycle mutation must fail closed','ALL'],
  ['paired_transition_replay','paired_timesheet_reversal_replacement','repeat of the same complete lifecycle transition is idempotent; mixed partial state fails closed','ALL'],
  ['paired_draft_response_loss_replay','paired_timesheet_reversal_replacement','lost Draft response reuses the same ordered chain/component identity without duplicating either leg','PAYE+UMBRELLA'],
  ['paired_reversal_only_paye','paired_timesheet_reversal_only','one reversal/no replacement with proved residual','PAYE'],
  ['paired_reversal_only_umbrella','paired_timesheet_reversal_only','one reversal/no replacement with proved residual','UMBRELLA'],
  ['expense_expenses_paye','timesheet_expenses_and_mileage','EXPENSES code','PAYE'],
  ['expense_travel_paye','timesheet_expenses_and_mileage','TRAVEL code','PAYE'],
  ['expense_accommodation_paye','timesheet_expenses_and_mileage','ACCOMMODATION code','PAYE'],
  ['expense_other_paye','timesheet_expenses_and_mileage','OTHER code','PAYE'],
  ['mileage_paye','timesheet_expenses_and_mileage','MILEAGE maps to MILEAGE_DELTA','PAYE'],
  ['expense_vat_umbrella','timesheet_expenses_and_mileage','VAT-bearing Umbrella expense preserves ex-VAT/VAT/inclusive values','UMBRELLA'],
  ['expense_non_vat_umbrella','timesheet_expenses_and_mileage','zero/non-VAT Umbrella expense preserves source VAT evidence','UMBRELLA'],
  ['mileage_umbrella','timesheet_expenses_and_mileage','MILEAGE maps to MILEAGE_DELTA with Umbrella authority','UMBRELLA'],
  ['additional_code_component','additional_and_adjustment_components','ADDITIONAL_CODE exact key','PAYE+UMBRELLA'],
  ['adjustment_code_component','additional_and_adjustment_components','ADJUSTMENT_CODE exact key','PAYE+UMBRELLA'],
  ['forced_advance_paye','forced_advance_this_payment','timing override retains ordinary economics','PAYE'],
  ['forced_advance_umbrella','forced_advance_this_payment','timing override retains ordinary economics','UMBRELLA'],
  ['part_paid_residual','prior_paid_part_paid_and_superseded','only exact unpaid residual remains','PAYE+UMBRELLA'],
  ['fully_settled_absent','prior_paid_part_paid_and_superseded','fully settled component absent from Ready','PAYE+UMBRELLA'],
  ['active_reservation_residual','prior_paid_part_paid_and_superseded','active reservation subtracted exactly','PAYE+UMBRELLA'],
  ['superseded_absent','prior_paid_part_paid_and_superseded','superseded physical identity absent unless current residual authority says otherwise','PAYE+UMBRELLA'],
  ['cancelled_untouched_reappears_once','prior_paid_part_paid_and_superseded','released untouched Draft source returns once','PAYE+UMBRELLA'],
  ['advance_payout_paye','payment_advance_payout','positive initial payout','PAYE NET_ADD'],
  ['advance_payout_umbrella','payment_advance_payout','positive initial payout','UMBRELLA'],
  ['advance_repayment_paye','payment_advance_repayment','visible PAYMENT_ADVANCE_REPAYMENT freezes LOAN_REPAYMENT','PAYE NET_DEDUCT'],
  ['advance_repayment_umbrella','payment_advance_repayment','visible PAYMENT_ADVANCE_REPAYMENT freezes LOAN_REPAYMENT','UMBRELLA'],
  ['advance_part_repaid_residual','payment_advance_repayment','only remaining scheduled residual','PAYE+UMBRELLA'],
  ['advance_paid_off_absent','payment_advance_repayment','no remaining due; absent/blocked','PAYE+UMBRELLA'],
  ['advance_cancelled_absent','payment_advance_repayment','cancelled authority not selectable','PAYE+UMBRELLA'],
  ['advance_voided_reappears_once','payment_advance_repayment','certified release permits one correct reappearance','PAYE+UMBRELLA'],
  ['overpayment_taxable_paye','overpayment_recovery','taxable recovery','PAYE GROSS_DEDUCT'],
  ['overpayment_nontaxable_paye','overpayment_recovery','non-taxable recovery','PAYE NET_DEDUCT'],
  ['overpayment_umbrella','overpayment_recovery','Umbrella recovery','UMBRELLA'],
  ['overpayment_zero_headroom','overpayment_recovery','no ordinary positive headroom; zero draftable recovery','PAYE+UMBRELLA'],
  ['overpayment_exact_headroom','overpayment_recovery','recovery exactly consumes allowed headroom','PAYE+UMBRELLA'],
  ['overpayment_partial_headroom','overpayment_recovery','partial recovery only; residual remains','PAYE+UMBRELLA'],
  ['underpayment_taxable_paye','underpayment_payment','taxable credit','PAYE GROSS_ADD'],
  ['underpayment_nontaxable_paye','underpayment_payment','non-taxable credit','PAYE NET_ADD'],
  ['underpayment_umbrella','underpayment_payment','Umbrella credit','UMBRELLA'],
  ['manual_credit_taxable_paye','manual_credit_adjustment','taxable credit','PAYE GROSS_ADD'],
  ['manual_credit_nontaxable_paye','manual_credit_adjustment','non-taxable credit','PAYE NET_ADD'],
  ['manual_credit_umbrella','manual_credit_adjustment','Umbrella credit','UMBRELLA'],
  ['manual_debt_taxable_paye','manual_debt_adjustment','taxable deduction','PAYE GROSS_DEDUCT'],
  ['manual_debt_nontaxable_paye','manual_debt_adjustment','non-taxable deduction','PAYE NET_DEDUCT'],
  ['manual_debt_umbrella','manual_debt_adjustment','Umbrella deduction','UMBRELLA'],
  ['manual_debt_zero_headroom','manual_debt_adjustment','zero permitted recovery','PAYE+UMBRELLA'],
  ['manual_debt_partial_headroom','manual_debt_adjustment','partial permitted recovery','PAYE+UMBRELLA'],
  ['mixed_recoveries_deterministic_order','overpayment_recovery','overpayment, manual debt and advance repayment share headroom in source-defined deterministic order','PAYE+UMBRELLA'],
  ['carry_forward_credit','manual_adjustment_carry_forward','positive stored carry-forward','PAYE+UMBRELLA'],
  ['carry_forward_debit','manual_adjustment_carry_forward','negative stored carry-forward','PAYE+UMBRELLA'],
  ['signed_positive_return','signed_non_charge_recovery','exact positive signed evidence','PAYE+UMBRELLA'],
  ['signed_negative_recovery','signed_non_charge_recovery','exact negative signed evidence','PAYE+UMBRELLA'],
  ['signed_mixed_ordinary_same_key','signed_non_charge_recovery','ordinary same-key components do not enter signed cardinality','PAYE+UMBRELLA'],
  ['signed_two_decisive_matches','signed_non_charge_recovery','two genuine signed matches reject','PAYE+UMBRELLA'],
  ['signed_tampered_or_incomplete','signed_non_charge_recovery','tampered/missing digest or shape rejects','PAYE+UMBRELLA'],
  ['timesheet_snoozed_excluded','ordinary_timesheet_components','active Timesheet snooze excludes exact scope','PAYE+UMBRELLA'],
  ['segment_snooze_isolation','ordinary_timesheet_components','only exact segment scope excluded; unrelated Ready components preserved','PAYE+UMBRELLA'],
  ['finance_case_snoozed_excluded','overpayment_recovery','active finance-case snooze excludes exact case','PAYE+UMBRELLA'],
  ['snooze_expiry_reappears_once','ordinary_timesheet_components','expired/released source reappears once subject to ordinary checks','PAYE+UMBRELLA'],
  ['action_required_excluded','ordinary_timesheet_components','unresolved rate/payment/case evidence excluded','PAYE+UMBRELLA'],
  ['blocked_excluded','ordinary_timesheet_components','blocked source excluded','PAYE+UMBRELLA'],
  ['active_draft_excluded','prior_paid_part_paid_and_superseded','active frozen reservation prevents duplicate Draft','PAYE+UMBRELLA'],
  ['mixed_candidates_independent','ordinary_timesheet_components','one worker failure cannot change another worker decision; operation atomicity still applies','PAYE+UMBRELLA'],
  ['mixed_channel_partitions','ordinary_timesheet_components','PAYE and Umbrella split into exact separate batch partitions','PAYE+UMBRELLA'],
  ['over_100_distinct_timesheets','ordinary_timesheet_components','complete selection beyond the historical 100-row defect threshold','PAYE+UMBRELLA'],
  ['multi_segment_over_100','ordinary_timesheet_components','more than 100 components across fewer Timesheets','PAYE+UMBRELLA'],
  ['pagination_1001','ordinary_timesheet_components','bounded pages reproduce complete 1,001 set','PAYE+UMBRELLA'],
  ['boundary_50000','ordinary_timesheet_components','bounded complete settled maximum without giant arrays','PAYE+UMBRELLA'],
  ['boundary_50001_reject','ordinary_timesheet_components','deterministic fail closed before partial state','PAYE+UMBRELLA'],
  ['duplicate_delivery_replay','ordinary_timesheet_components','same operation/receipt does not duplicate artifacts','PAYE+UMBRELLA'],
  ['response_loss_replay','ordinary_timesheet_components','lost response resumes from durable receipt without duplicate economics','PAYE+UMBRELLA'],
  ['concurrent_same_selection','ordinary_timesheet_components','one winner or exact idempotent replay; no double reservation','PAYE+UMBRELLA'],
  ['stale_selection_revision','ordinary_timesheet_components','context/source change rejects before Draft effects','PAYE+UMBRELLA'],
  ['atomic_multibatch_failure','ordinary_timesheet_components','failure in one channel/candidate partition leaves no partial authoritative Draft','PAYE+UMBRELLA']
];
const equivalenceClasses = equivalenceClassRows.map(([class_id, family_id, condition, channel]) => ({
  class_id, family_id, condition, channel,
  proof_rule: 'Compare exact selected constituent identity, sign, canonical ex-VAT amount, channel, allocation, item, reservation, materialisation and final downstream projection; totals are secondary.'
}));

const stageContract = [
  ['VALIDATE_SESSION', 'pay_workbench_prepare_draft', 'Re-read the current Workbench selection/readiness/context and preserve override rules; create no finance output.'],
  ['SYNC_SELECTED_ROWS', 'Worker operation state', 'Persist the accepted validation receipt and advance only the same certified selected-row operation; do not reconstruct selection.'],
  ['WAIT_FOR_PREVIEW_READY', 'pay_workbench_session_get_progress', 'Require the same current Workbench session to remain Ready before any Draft scope is frozen.'],
  ['SEED_CANDIDATE_SCOPE', 'pay_workbench_prepare_draft_scope_seed', 'Freeze the complete selected constituent identities by candidate and channel; exact count/digest/missing/extra checks.'],
  ['DRAIN_TSFIN', 'Worker + existing TSFIN readiness owners', 'Complete required Timesheet financial readiness for the frozen scope; no timeout relaxation or skip.'],
  ['ENSURE_PAYEE_READINESS', 'Worker + existing payee readiness owner', 'Validate existing payee/bank readiness for the frozen scope without changing payee/channel policy.'],
  ['SEED_ALLOCATION_ROWS', 'pay_workbench_prepare_draft_allocation_rows_seed', 'Create source-owned allocation facts; preserve recovery/headroom ordering.'],
  ['CREATE_BATCH_SHELLS', 'pay_batch_shell_ensure_from_operation', 'Create the same PAYE/Umbrella batch shells and statuses.'],
  ['INSERT_CANDIDATES', 'pay_batch_insert_candidates_from_preview', 'Create exact batch-candidate membership once.'],
  ['INSERT_ITEMS', 'pay_batch_insert_items_from_preview', 'Materialise ordinary items or certify the exact finance handoff; calculate no finance economics.'],
  ['APPLY_FINANCE_ADJUSTMENTS', 'pay_batch_apply_finance_adjustments', 'Existing owner materialises finance items, allocations, PAYE/Umbrella treatment and case state.'],
  ['FINALISE_RESERVATIONS', 'pay_batch_finalize_reservations_and_markers', 'Create exact reservations, retention markers, signed recovery evidence and final authority.'],
  ['POPULATE_CANDIDATE_SUMMARIES', 'pay_batch_populate_candidate_summaries', 'Freeze candidate and batch totals derived from items.'],
  ['CREATE_TIMESHEET_SNAPSHOTS', 'pay_batch_create_timesheet_snapshots', 'Freeze the same source Timesheet snapshots and lineage.'],
  ['BUILD_ITEM_BREAKDOWNS', 'pay_batch_build_item_breakdowns', 'Freeze the same item breakdowns and component evidence.'],
  ['ASSERT_INTEGRITY', 'pay_batch_assert_integrity', 'Reject incomplete/duplicate/inconsistent Draft artifacts; no hiding rows.'],
  ['POST_CREATE_REFRESH', 'Worker operation result + Workbench targeted refresh', 'Return the same created batch IDs/result fields and refresh source visibility.']
].map(([phase, owner, decision]) => ({ phase, owner, decision }));

const durableArtifactContracts = [
  ['pay_batches', 'Batch identity, PAYE/Umbrella split, Draft status, source session/context, execution/freshness/rail snapshots.'],
  ['pay_batch_candidates', 'Exact candidate membership, channel, payee, totals and stable batch relationship.'],
  ['pay_batch_items', 'Exact item type, source/economic keys, sign, ex-VAT/VAT/inclusive amounts, PAYE treatment, void/status and lineage.'],
  ['banking_pay_operation_candidate_allocation_rows', 'Exact selected source/recovery allocation identity, amount and finance/component linkage.'],
  ['pay_advance_reservations and timesheet_financial_retention', 'Exact reserved finance/component identity and sticky Timesheet protection against double payment or unsafe deletion.'],
  ['timesheet snapshots', 'Frozen post-Draft Timesheet/component/rate/payment-method evidence.'],
  ['item breakdowns', 'Frozen segment, expense, rate, finance and source-basis details.'],
  ['finance effects', 'Exact case/component balance and state transitions owned by finance routines.'],
  ['expected-effect attestations', 'Exact expected effect identities/hashes used by execution and reversion.'],
  ['post-Draft authority and fast-reversion lineage', 'Policy X frozen authority, source publication and certified cancellation/reversion evidence.'],
  ['operation result/audit', 'Same operation status, public result fields, created batch IDs, alerts and grouped ownership evidence.']
].map(([artifact, invariant]) => ({ artifact, invariant, comparison_rule: 'Compare full typed V1 and candidate rows after role-mapping only generated technical IDs; no money/status/type/key/hash normalization.' }));

const downstreamConsumers = [
  { consumer: 'Current Payment Status', must_receive: ['same rows', 'same payment lifecycle/status/action', 'same amounts and evidence'] },
  { consumer: 'PAYE Worksheet', must_receive: ['same PAYE candidates/items', 'same gross additions/deductions', 'same net additions/deductions', 'same saved net scalar and state hash'] },
  { consumer: 'Umbrella payment/remittance', must_receive: ['same payee/channel', 'same ex-VAT/VAT/inclusive totals', 'same remittance visibility/suppression'] },
  { consumer: 'Overview', must_receive: ['same beneficiary counts', 'same channel/payment totals', 'same actions/statuses'] },
  { consumer: 'Execute Payment eligibility/preview', must_receive: ['same freshness and integrity result', 'same batch/candidate/item/allocation scope', 'same bank projection and hashes'] },
  { consumer: 'Immediate/scheduled provider submission', must_receive: ['same frozen projection', 'same provider/rail environment authority', 'same idempotency/effect attestations'] },
  { consumer: 'Bank transfer and CSV settlement', must_receive: ['same payment rows/references/amounts', 'same positive/explicit-zero classification', 'same projection hash'] },
  { consumer: 'External settlement/remittance', must_receive: ['same execution/settlement lineage', 'same remittance generation or suppression'] },
  { consumer: 'Cancellation before payment', must_receive: ['same whole-batch versus whole-Candidate scope', 'same cancelability decision', 'same selected releases/voids with unrelated Candidates unchanged', 'same Workbench reappearance exactly once'] },
  { consumer: 'Executed-not-paid cancellation and certified reversion', must_receive: ['same whole-Candidate scope for local or future-dated scheduled payments not sent to the provider', 'same frozen lineage', 'same safe reversion or established fallback', 'same blocked-funds/correction behavior'] },
  { consumer: 'Frontend operation polling', must_receive: ['same operation type/status/phase/result/error fields', 'same terminal interpretation', 'no V2-specific branch'] }
];

const downstreamOwnerCensus = [
  {
    boundary: 'Batch Overview and execution eligibility',
    owner: 'public.pay_batch_execution_summary_get',
    installed_definition_sha256: installedRoutines['public.pay_batch_execution_summary_get(uuid,uuid,text)'],
    reads: ['pay_batches', 'pay_batch_candidates', 'pay_batch_items', 'transfers', 'freshness/rail/PAYE state'],
    decision: 'Produces the authoritative high-level batch, candidate, bank-out and readiness summary consumed before execution.',
    source: `${sourceFiles.monolith}: current pay_batch_execution_summary_get definition`,
    worker_calls: ['broker/src/index.js:24464', 'broker/src/index.js:44482-44486', 'broker/src/index.js:52855-52859']
  },
  {
    boundary: 'Current Payment Status',
    owner: 'public.pay_batch_payment_status_page_v1',
    installed_definition_sha256: installedRoutines['public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb)'],
    reads: ['frozen batch/candidate/item/allocation', 'bank transfers/events', 'correction/cancel/retry evidence'],
    decision: 'Returns stable paged payment rows, statuses and exact allowed actions; the frontend must not repair or infer them.',
    source: 'supabase/repeatable/04082026_1146_pay_batch_payment_status_page_v1.sql',
    worker_calls: ['broker/src/index.js:51006-51014', 'broker/src/index.js:51523-51531'],
    frontend_calls: ['TEST-Frontend/js/main.js:23757-26172']
  },
  {
    boundary: 'PAYE Worksheet save and bank scalar',
    owner: 'public.pay_set_paye_net_manual',
    installed_definition_sha256: installedRoutines['public.pay_set_paye_net_manual(uuid,jsonb,uuid)'],
    reads: ['frozen PAYE items', 'GROSS_ADD/GROSS_DEDUCT schedule', 'NET_ADD/NET_DEDUCT schedule', 'candidate membership'],
    decision: 'Validates and saves imported PAYE net amounts; gross items are already reflected while net items adjust afterward.',
    source: 'supabase/repeatable/21072026_1235_48_pay_set_paye_net_manual.sql',
    worker_calls: ['broker/src/index.js:47634-47640'],
    frontend_calls: ['TEST-Frontend/js/main.js:76629-77517']
  },
  {
    boundary: 'Frozen bank payment projection',
    owner: 'public._pay_batch_bank_payment_projection_rows',
    installed_definition_sha256: installedRoutines['public._pay_batch_bank_payment_projection_rows(uuid,text)'],
    reads: ['frozen batch/candidate/item amounts', 'saved PAYE net', 'Umbrella totals/payee', 'void/status evidence'],
    decision: 'Produces the final per-beneficiary bank amount and projection hash used by Execute and CSV.',
    source: 'supabase/repeatable/20072026_1105_resolve_paye_deduction_bank_projection.sql',
    worker_calls: ['broker/src/index.js:44402-44405', 'broker/src/index.js:52869-52872']
  },
  {
    boundary: 'Bank CSV evidence',
    owner: 'public.pay_bank_csv_export_summary_get plus frozen bank projection',
    installed_definition_sha256: installedRoutines['public.pay_bank_csv_export_summary_get(uuid,text,uuid)'],
    reads: ['bank payment projection rows/hash', 'PAYE net state hash', 'batch freshness and rail snapshots'],
    decision: 'Exports only the exact positive/explicit-zero frozen payment scope and rejects projection or row-count change.',
    source: `${sourceFiles.monolith}: pay_bank_csv_export_summary_get`,
    worker_calls: ['broker/src/index.js:52744-53116']
  },
  {
    boundary: 'Execute Payment scope',
    owner: 'public.pay_execute_bank_transfer_scope_seed',
    installed_definition_sha256: installedRoutines['public.pay_execute_bank_transfer_scope_seed(uuid,uuid,text,uuid,boolean)'],
    reads: ['frozen projection', 'candidate/channel membership', 'freshness/integrity', 'retry-blocked-funds state'],
    decision: 'Seeds the exact transfer work scope; no Create Draft version is an input.',
    source: 'supabase/repeatable/20072026_1215_align_transfer_scope_with_bank_projection.sql',
    worker_calls: ['broker/src/index.js: PAYMENT_EXECUTE orchestration']
  },
  {
    boundary: 'Bank-transfer preparation',
    owner: 'public.pay_execute_bank_transfer_chunk_prepare',
    installed_definition_sha256: installedRoutines['public.pay_execute_bank_transfer_chunk_prepare(uuid,uuid,jsonb,uuid)'],
    reads: ['row-backed execution scope', 'frozen bank projection', 'provider/rail snapshots', 'void overlays'],
    decision: 'Creates/reuses exact transfer preparation evidence without changing beneficiary or amount.',
    source: 'supabase/repeatable/12082026_1446_pay_execute_bank_transfer_chunk_prepare_voided_overlay.sql',
    worker_calls: ['broker/src/index.js: PAYMENT_EXECUTE prepare phase']
  },
  {
    boundary: 'Provider submission claim',
    owner: 'public.pay_bank_transfers_claim_provider_submit_chunk',
    installed_definition_sha256: installedRoutines['public.pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer)'],
    reads: ['prepared transfer rows', 'operation scope/lease', 'provider submission state'],
    decision: 'Claims only safe prepared transfers for unchanged provider submission.',
    source: 'supabase/repeatable/04082026_1154_pay_bank_transfers_claim_provider_submit_chunk.sql',
    worker_calls: ['broker/src/index.js: PAYMENT_EXECUTE provider phase']
  },
  {
    boundary: 'Settlement',
    owner: 'public.pay_settle_rail',
    installed_definition_sha256: installedRoutines['public.pay_settle_rail(uuid,jsonb,uuid,uuid,jsonb)'],
    reads: ['submitted/settled transfer evidence', 'exact settlement scope', 'frozen batch artifacts'],
    decision: 'Applies authoritative settlement results within existing 6000ms/1000ms budgets.',
    source: 'supabase/repeatable/04082026_1211_pay_settle_rail.sql',
    worker_calls: ['broker/src/index.js:64289-64295', 'broker/src/index.js:64460-64466']
  },
  {
    boundary: 'Remittance scope and rendering',
    owner: 'public.pay_operation_remittance_scope_seed + public.pay_remittance_build + public.pay_remittance_maybe_queue_for_trigger',
    installed_definition_sha256: [
      installedRoutines['public.pay_operation_remittance_scope_seed(uuid,uuid,text,uuid)'],
      installedRoutines['public.pay_remittance_build(uuid,text,uuid,jsonb,integer,jsonb)'],
      installedRoutines['public.pay_remittance_maybe_queue_for_trigger(uuid,text,text,uuid,boolean,uuid,boolean)']
    ],
    reads: ['frozen items/breakdowns', 'candidate/Umbrella payee', 'execution/settlement state', 'remittance settings/suppression'],
    decision: 'Generates or suppresses the same candidate/Umbrella remittance from frozen Draft and execution evidence.',
    source: `${sourceFiles.monolith}: remittance owners`,
    worker_calls: ['broker/src/index.js:196862 remittance endpoint'],
    frontend_calls: ['TEST-Frontend/js/main.js:43522-43699', 'TEST-Frontend/js/main.js:68016-68531']
  },
  {
    boundary: 'Whole-batch Draft cancellation wrapper',
    owner: 'public.pay_batch_cancel',
    installed_definition_sha256: installedRoutines['public.pay_batch_cancel(uuid,uuid,text,uuid,uuid)'],
    reads: ['one Draft pay batch', 'active batch payments', 'batch cancellation idempotency and audit'],
    decision: 'Cancels only the explicitly requested whole Draft batch. It is not the owner for a whole-Candidate selection and cannot be used to widen Candidate scope.',
    source: sourceFiles.batchCancel,
    worker_calls: ['broker/src/index.js: handleBankingPayBatchCancel']
  },
  {
    boundary: 'Whole-Candidate cancellation before payment',
    owner: 'pay_payment_cancelability_diagnostic → correction request/selection → Candidate-scoped Draft overlay or pre-bank cancellation page',
    installed_definition_sha256: [
      installedRoutines['public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text)'],
      installedRoutines['public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb)'],
      installedRoutines['public._pay_payment_correction_selected_items(uuid,jsonb,boolean)'],
      installedRoutines['public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid)'],
      installedRoutines['public.pay_payment_correction_expand_work(uuid,uuid)'],
      installedRoutines['public.pay_payment_correction_process_chunk(uuid,integer,text,uuid)'],
      installedRoutines['private.pay_workbench_draft_overlay_remove_page_v1(uuid,uuid,uuid,uuid,integer,integer,uuid,jsonb)'],
      installedRoutines['private.pay_pre_bank_cancel_apply_work_page_v1(uuid,uuid[],uuid,jsonb)'],
      installedRoutines['public.pay_pre_bank_cancel_apply_work_item(uuid,uuid)']
    ],
    reads: ['frozen Candidate/item/allocation/reservation scope', 'provider/bank/settlement evidence', 'DRAFT_CANCEL or PRE_BANK_CANCEL action', 'source and post-Draft lineage'],
    decision: 'Freezes the selected Candidate membership, then voids/releases only that Candidate whole-payment scope. Untouched Draft and future-dated scheduled/local-not-sent states take their established separate branches; unrelated Candidates must remain unchanged and payable.',
    source: `${sourceFiles.correctionSelectedItems}; ${sourceFiles.correctionSelectionPrepare}; ${sourceFiles.correctionRequestStart}; ${sourceFiles.correctionExpandWork}; ${sourceFiles.correctionProcessChunk}; ${sourceFiles.preBankCancelApply}; ${sourceFiles.semanticReadyHelpers}`,
    worker_calls: ['broker/src/index.js: handleBankingPayCorrectionPlanV1', 'broker/src/index.js: handleBankingPayCorrectionStartPreparedV1', 'broker/src/index.js: PAYMENT_CORRECTION continuation']
  },
  {
    boundary: 'Executed/settled correction and certified reversion',
    owner: 'public.pay_payment_correction_request_start + public.pay_settled_payment_reversal_apply_work_item',
    installed_definition_sha256: [
      installedRoutines['public.pay_payment_correction_request_start(uuid,jsonb,text,uuid,uuid,boolean,jsonb)'],
      installedRoutines['public.pay_settled_payment_reversal_apply_work_item(uuid,uuid)']
    ],
    reads: ['frozen source/effect/settlement lineage', 'selected payment status scope', 'accepted resolution and correction work'],
    decision: 'Uses certified frozen lineage to reverse/correct or chooses the established safe fallback; never live-reconstructs the Draft.',
    source: 'supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql plus current monolith settled reversal',
    worker_calls: ['broker/src/index.js: payment correction orchestration']
  },
  {
    boundary: 'Frontend interpretation',
    owner: 'TEST-Frontend/js/main.js current Banking Pay child/modal code',
    installed_definition_sha256: null,
    reads: ['operation response', 'Overview', 'PAYE Worksheet', 'Current Payment Status', 'remittance and correction outputs'],
    decision: 'Renders current authoritative fields and backend-resolved actions; no route-version repair is permitted.',
    source: 'TEST-Frontend/js/main.js at frontend commit e58e567f66ed8108a40e3c3e8388dbe33e0b0361',
    frontend_calls: ['23757-26172', '76629-77517', '79323-79382']
  }
];

const contract = {
  contract: 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1',
  schema_version: 1,
  purpose: 'Freeze every current Banking Pay/Create Draft financial decision and downstream artifact contract so orchestration can change without any payment-policy or outcome drift.',
  authority: {
    repository: {
      backend_git_commit: backendGitCommit,
      backend_tree: backendTree,
      current_contract_file_sha256: fileSha(sourceFiles.currentContract),
      relevant_source_file_sha256: Object.fromEntries(Object.values(sourceFiles).filter(file => file !== sourceFiles.worker && file !== sourceFiles.currentContract).map(file => [file, fileSha(file)]))
    },
    frontend_bible: {
      git_commit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: frontendRoot, encoding: 'utf8' }).trim(),
      path: path.join(frontendRoot, 'BANKING_PAY_BIBLE.md').replaceAll('\\', '/'),
      sha256: sha256(fs.readFileSync(path.join(frontendRoot, 'BANKING_PAY_BIBLE.md')))
    },
    installed_miget_test: {
      database_name: 'cloudtms_test_clone', database_user_identity: 'dee50tht', postgres_version: '17.11',
      release_id: '20260822-test-authority-upgrade-a849a25e5391', git_commit: installedGitCommit,
      read_only_capture_date: '2026-09-02', routine_raw_definition_sha256: installedRoutines,
      former_supabase_projects_used: false
    },
    source_install_reconciliation: {
      status: bankingOrDraftPathsChangedSinceInstalledRelease.length === 0
        ? 'PASS'
        : 'OPEN_FRESH_MIGET_AUDIT_REQUIRED',
      installed_commit_is_ancestor_of_reviewed_source: true,
      paths_changed_since_installed_release: pathsChangedSinceInstalledRelease,
      banking_or_draft_paths_changed_since_installed_release: bankingOrDraftPathsChangedSinceInstalledRelease,
      bounded_live_routine_hash_match_count: bankingOrDraftPathsChangedSinceInstalledRelease.length === 0
        ? Object.keys(installedRoutines).length
        : 0,
      bounded_live_routine_hash_mismatch_count: bankingOrDraftPathsChangedSinceInstalledRelease.length === 0
        ? 0
        : null,
      proof_rule: 'The current source is newer only in the listed paths. A Banking/Draft path here, or any live routine mismatch, invalidates this reconciliation and requires a fresh audit.'
    },
    bounded_current_test_occurrence_evidence: {
      correction_pair_shape_count_rows: 0,
      interpretation: 'The bounded non-sensitive Miget aggregate found no current qualifying paired-Timesheet business shape. Paired policy is therefore source/installed-owner/fixture proven; this is not evidence that the production-reachable category can be omitted.'
    }
  },
  upstream_workbench_certificate_contract: {
    contract_name: 'WORKBENCH_SETTLED_CERTIFICATION_V1',
    artifact_sha256: '569eac8f790478453ea47e75d84f3c5f0553ec36894ddb7fcde0570db85a16a8',
    artifact_bytes: 20449,
    artifact_status: 'LOCAL_CANDIDATE_NOT_INSTALLED_NOT_DEPLOYED_NOT_FINAL',
    consumer_rule: 'Consume certification_id and overall_digest_sha256 exactly; do not reconstruct Workbench selection.',
    h1_owned_input_facts: [
      'exact current session/version/progress/snapshot/generation authority',
      'complete ordered selected constituent identities and canonical ex-VAT amounts',
      'exact candidate/pay-channel partitions and partition digests',
      'Ready/Action Required/Blocked and exclusion-universe completeness proofs',
      'source publication identities and expected pre-Draft recovery/headroom/item/source-reservation facts',
      'accepted backend/database/Worker/frontend/Bible proof identities'
    ],
    prohibited_h1_outputs: [
      'Draft-produced allocation-row identity',
      'Draft item identity',
      'Draft reservation-row identity',
      'post-Draft materialisation fact',
      'Draft parity verdict'
    ],
    activation_gate: 'No populated sealed certificate instance or installed server-readable lifecycle is proved by this artifact. A future route must fail closed until those separately reviewed authorities exist.'
  },
  truth_hierarchy: [
    'Before Draft: current server-owned Workbench publication, readiness, exact selected constituent identity and existing financial owners.',
    'During Draft: the existing stage owner for that exact decision; orchestration transports identity/receipts but never recomputes money.',
    'After Draft: frozen pay batch/candidate/item/allocation/reservation/snapshot/breakdown/effect artifacts only (Policy X).',
    'Execute/cancel/revert: current downstream owners consume those frozen artifacts without a route-version branch.'
  ],
  non_negotiable_invariants: [
    'No eligibility, selection, amount, sign, gross/net, tax, VAT, channel, payee, rate, headroom, grouping, approval, hold, resolution, provider, settlement, cancellation or reversion policy change.',
    'Every selected constituent is compared by exact stable identity and amount; totals alone can never prove parity.',
    'PAYE and Umbrella remain separate authoritative channel partitions and batch outputs.',
    'Gross deductions/additions and net deductions/additions retain their deliberately different bank-scalar treatment.',
    'Visible Workbench vocabulary may deliberately differ from frozen Draft vocabulary; aliases are mapped, never normalised by guess.',
    'Paired Timesheet lifecycle is atomic, but its member and component identities are not collapsed.',
    'No Draft or Execute RPC budget may be increased, removed or bypassed to obtain a pass.',
    'Create Draft must support the settled complete selected set beyond 100, with no truncation or hidden cap; bounded pages/receipts replace giant arrays.',
    'No downstream consumer may know whether the same Draft artifacts were produced by the current or revised orchestration route.',
    'Any new field is additive and backward-compatible; no consumed field may be renamed, omitted, retyped or reinterpreted.'
  ],
  source_vocabulary_registry: vocabularyRegistry,
  gross_net_policy: grossNetRules,
  payment_families: families,
  finite_equivalence_classes: equivalenceClasses,
  create_draft_stage_contract: stageContract,
  durable_artifact_contracts: durableArtifactContracts,
  downstream_consumers: downstreamConsumers,
  downstream_owner_census: downstreamOwnerCensus,
  operation_response_contract: {
    endpoint_and_operation_type: 'unchanged DRAFT_CREATE route and operation type',
    required_fields: ['status', 'phase', 'progress', 'result/error', 'pay_batch_id', 'pay_batch_ids', 'created_pay_batch_ids', 'primary_pay_batch_id'],
    rule: 'Existing Worker/frontend polling and terminal-result interpretation must remain valid without a V2-specific compatibility branch.'
  },
  exact_parity_method: {
    fixture_rule: 'Create equivalent fresh V1 and candidate-route universes from the same deterministic source fixture and recorded seed.',
    generated_identity_rule: 'Map only generated technical IDs by stable business role. Do not normalise amounts, statuses, item types, source/economic keys, VAT, hashes or authority fields.',
    compare_every_artifact: commonDraftArtifacts,
    compare_downstream_views: downstreamConsumers.map(row => row.consumer),
    required_end_to_end_path: ['Draft creation', 'PAYE/Umbrella preparation', 'payment execution preview and authorised disposable execution', 'Current Payment Status', 'cancellation before payment', 'certified reversion or established safe fallback'],
    failure_rule: 'Any unexplained row/field/type/value/identity/state difference, V2-specific downstream branch, timeout relaxation, skipped owner or hidden row is a parity failure.'
  },
  known_execution_findings_separate_from_policy: [
    { finding: 'Original Workbench recovery failure', owner: 'HANDOVER 1', status: 'source-package complete; not a populated installed certificate in this contract', policy_delta: false },
    { finding: 'F-010 complete selected-set handoff and >100 scope incompatibility', owner: 'H2 orchestration/scope boundary', status: 'proved; final compact server-backed interface and runtime implementation remain gated', policy_delta: false },
    { finding: 'F-013 six visible finance families rejected at current scope/INSERT_ITEMS handoff', owner: 'H2 Draft handoff boundaries', status: 'proved with 20 variants; provisional local owner proof only, not installed', policy_delta: false },
    { finding: 'F-013b LOAN_REPAYMENT equality hypothesis', owner: 'policy investigation', status: 'NOT A DEFECT; deliberate visible PAYMENT_ADVANCE_REPAYMENT → frozen LOAN_REPAYMENT vocabulary', policy_delta: false },
    { finding: 'Paired Timesheet replacement-only deletion transition', owner: 'upstream Timesheet correction lifecycle', status: 'USER-CONFIRMED POLICY; current standard delete targets both changed-hours members and has no explicit replacement-only delete plus REVERSAL_ONLY republish transition; not a Create Draft validation change', policy_delta: false },
    { finding: 'Small Draft 99% latency', owner: 'Worker/PostgREST orchestration', status: 'measured as round-trip/orchestration dominant; must be improved without merging or skipping business owners', policy_delta: false }
  ],
  coverage_gates: {
    payment_family_count: families.length,
    finite_equivalence_class_count: equivalenceClasses.length,
    paired_timesheet_families: ['paired_timesheet_reversal_replacement', 'paired_timesheet_reversal_only'],
    required_paired_member_deletion_subcases: ['genuine REVERSAL_ONLY remains valid', 'delete positive replacement only requires explicit upstream reclassification and preserves negative recovery', 'delete negative reversal only is prohibited', 'delete the complete pair is valid subject to existing financial retention/archive authority'],
    required_state_cross_product: ['Ready', 'Action Required', 'Blocked', 'snoozed', 'part-paid', 'fully settled', 'superseded', 'active Draft', 'cancelled/voided/reappearing once'],
    required_channel_cross_product: ['PAYE', 'UMBRELLA', 'mixed separate partitions'],
    required_orchestration_cross_product: ['one constituent', 'over 100 distinct Timesheets', 'multi-segment', '1,001 constituents', '50,000 boundary', '50,001 deterministic rejection', 'replay', 'response loss', 'concurrency', 'stale/tampered/malformed'],
    zero_tolerance: ['unmapped production alias', 'unproved policy assertion', 'open placeholder at enablement', 'skipped acceptance case', 'mutation survivor', 'unexplained parity difference', 'downstream V2 branch']
  }
};

const schema = {
  '$schema': 'https://json-schema.org/draft/2020-12/schema',
  title: contract.contract,
  type: 'object',
  required: ['contract','schema_version','authority','upstream_workbench_certificate_contract','truth_hierarchy','non_negotiable_invariants','source_vocabulary_registry','gross_net_policy','payment_families','finite_equivalence_classes','create_draft_stage_contract','durable_artifact_contracts','downstream_consumers','downstream_owner_census','exact_parity_method','coverage_gates'],
  properties: {
    contract: { const: 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1' },
    schema_version: { const: 1 },
    payment_families: { type: 'array', minItems: 10, items: { type: 'object', required: ['family_id','title','real_world_event','visible_aliases','frozen_item_types','identity','selection','amount_owner','variants','draft_result','fail_closed','source_evidence'] } },
    finite_equivalence_classes: { type: 'array', minItems: 70, items: { type: 'object', required: ['class_id','family_id','condition','channel','proof_rule'] } },
    create_draft_stage_contract: { type: 'array', minItems: 17 },
    downstream_consumers: { type: 'array', minItems: 10 },
    downstream_owner_census: { type: 'array', minItems: 13, items: { type: 'object', required: ['boundary','owner','reads','decision','source'] } }
  }
};

const q = value => String(value).replaceAll('|', '\\|').replaceAll('\n', ' ');
const human = [];
human.push('# How Banking Pay must treat every payment');
human.push('');
human.push('This is the human-readable projection of `BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1`. It explains the current policy outcomes that any faster or revised Create Draft route must reproduce. It does **not** authorise a policy change or a payment action.');
human.push('');
human.push('## The one rule that governs everything');
human.push('');
human.push('Before Draft, CloudTMS uses the current Workbench and its existing financial owners. Create Draft freezes those exact decisions. After Draft, Execute Payment, status, settlement, cancellation and reversion use only the frozen Draft evidence. A new route may transport the work differently; it may not make a different decision.');
human.push('The HANDOVER 1 certificate is currently a source-level contract only, not a populated or installed certificate. It defines what a future Draft route must consume; it is not permission to infer or rebuild selection in the Worker.');
human.push('');
human.push('```mermaid');
human.push('flowchart TD');
human.push('  A[Current Workbench: exact Ready constituents] --> B{Ready and selected?}');
human.push('  B -- No --> X[Action Required / Blocked / Snoozed / Excluded: no Draft constituent]');
human.push('  B -- Yes --> C[Freeze exact worker + channel + source/economic identity]');
human.push('  C --> D[Existing owner creates allocations and items]');
human.push('  D --> E[Existing owner creates reservations, snapshots, breakdowns and attestations]');
human.push('  E --> F[Integrity check: every identity and penny reconciles]');
human.push('  F --> G[Draft complete]');
human.push('  G --> H[Unchanged PAYE/Umbrella preparation and Execute Payment]');
human.push('  H --> I[Unchanged status / settlement / remittance / cancellation / reversion]');
human.push('```');
human.push('');
human.push('## Gross and net: why the four PAYE labels are not interchangeable');
human.push('');
for (const rule of grossNetRules) human.push(`- **${rule.rule_id}:** ${rule.meaning} Bank amount owner: ${rule.bank_scalar_owner}`);
human.push('');
human.push('Two name changes are deliberate and must not be mistaken for duplicate products: visible **PAYMENT_ADVANCE_REPAYMENT** freezes as **LOAN_REPAYMENT**, and visible **MANUAL_CREDIT_ADJUSTMENT_PAYMENT** freezes as **MANUAL_CREDIT_PAYOUT**. The visible name describes the Workbench decision; the frozen name is the existing Draft/settlement vocabulary.');
human.push('');
human.push('## Payment-by-payment operating model');
human.push('');
human.push('| Payment family | What it means | Ready/selection rule | PAYE | Umbrella | Frozen Draft result |');
human.push('|---|---|---|---|---|---|');
for (const row of families) {
  const paye = row.variants.find(v => v.channel === 'PAYE')?.paye_treatment || row.variants.map(v => v.paye_treatment).join('; ');
  const umbrella = row.variants.find(v => v.channel === 'UMBRELLA')?.vat || row.variants.map(v => v.vat).join('; ');
  human.push(`| ${q(row.title)} | ${q(row.real_world_event)} | ${q(row.selection)} | ${q(paye)} | ${q(umbrella)} | ${q(row.draft_result)} |`);
}
human.push('');
human.push('## Paired Timesheets — the special flow');
human.push('');
human.push('```mermaid');
human.push('flowchart TD');
human.push('  P[Imported Timesheet correction] --> S{Declared correction shape}');
human.push('  S -- Reversal + replacement --> R[Original + exactly 1 negative reversal<br/>+ exactly 1 positive replacement]');
human.push('  S -- Valid reversal-only --> O[Original + exactly 1 negative reversal<br/>No replacement is invented]');
human.push('  S -- Required replacement missing,<br/>duplicate or unexpected member --> Z[BLOCKED<br/>No Draft or money effect]');
human.push('  R --> V{Exact worker, client, operation,<br/>member order and fingerprints?<br/>No paid or invoice-locked conflict?}');
human.push('  O --> V');
human.push('  V -- No --> Z');
human.push('  V -- Yes --> L[One atomic correction unit<br/>Action against either leg resolves all<br/>required members together]');
human.push('  L --> X{Unrelated overlapping Timesheet?}');
human.push('  X -- Yes --> Z');
human.push('  X -- No --> M[Calculate each TS_DAY component<br/>using existing truth, settled baseline<br/>and active reservations]');
human.push('  M --> Q{Historical source channel equals<br/>worker\'s current channel?}');
human.push('  Q -- No: PAYE to Umbrella<br/>or Umbrella to PAYE --> U[ACTION REQUIRED<br/>Resolve every component bucket separately<br/>with exact saved target evidence]');
human.push('  U -- Missing, stale or partial --> Z');
human.push('  U -- Every component exact --> N{Net residual}');
human.push('  Q -- Yes --> N');
human.push('  N -- Positive --> D[One certified correction-family constituent<br/>with ordered physical member lineage]');
human.push('  N -- Zero --> E0[Nothing payable<br/>No Draft item invented]');
human.push('  N -- Negative --> NR[Existing recovery/headroom authority<br/>must decide treatment<br/>Complete Draft proof remains open]');
human.push('  D --> F[Draft freezes the certified identity,<br/>channel, sign, amount and fingerprints]');
human.push('  F --> E[Unchanged Execute/cancel/revert owners<br/>use frozen lineage under Policy X]');
human.push('```');
human.push('');
human.push('The pair is atomic for lifecycle actions, but it is not one invented lump-sum payment. The reversal and replacement Timesheets, their policy fingerprints and their component identities remain visible to the authority. A genuine `REVERSAL_ONLY` correction is valid; a `REVERSAL_REPLACEMENT` correction that has lost its replacement is invalid and cannot be relabelled as reversal-only by Create Draft. PAYE/Umbrella changes are resolved jointly at correction-unit readiness, but each economic component must have its own exact saved target amount and fingerprints. Broken, duplicated, stale, cross-worker, cross-client, partially paid, partially invoiced, partially resolved and unrelated-overlap shapes fail closed.');
human.push('');
human.push('The member-deletion outcomes are intentionally different:');
human.push('');
human.push('- A canonical cancellation-created reversal-only unit is valid. Its exact negative residual becomes the existing overpayment-recovery obligation; without same-worker, same-channel positive headroom it remains retained outside the Draft.');
human.push('- Deleting only the positive replacement while retaining the negative reversal is a valid user-confirmed business outcome. Current source does not yet implement that transition: standard delete targets both changed-hours members. The safe correction belongs upstream and must atomically republish the survivor as a genuine `REVERSAL_ONLY` unit.');
human.push('- Deleting only the negative reversal is prohibited. A positive-only remnant has no reversal and fails closed.');
human.push('- Deleting the complete correction pair is valid when existing financial-retention checks permit deletion; where durable history must remain, the existing archive outcome still governs.');
human.push('');
human.push('Local PostgreSQL 17.11 and 18.6 evidence proves the exact canonical positive pair path, PAYE and Umbrella, cross-channel fail-closed-then-resolved behavior, atomic transition/replay, response-loss replay, broken/stale/mixed/paid/invoiced blocks, ordinary/unrelated overlap preservation, positive-only rejection, and the genuine reversal-only recovery/headroom outcome. Read-only Miget inspection confirms the installed chain, deletion and recovery owners have those same boundaries. The replacement-only deletion-to-reversal-only transition remains an explicit upstream implementation gap; Draft validation must not be weakened to conceal it.');
human.push('');
human.push(`The machine contract freezes **${equivalenceClasses.length} finite logical classes**. This is a bounded policy matrix—not a claim to enumerate infinite dates or money values.`);
human.push('');
human.push('## Create Draft stages whose decisions must not change');
human.push('');
for (const stage of stageContract) human.push(`1. **${stage.phase}** — ${stage.decision} Owner: \`${stage.owner}\`.`);
human.push('');
human.push('## What Execute Payment must see');
human.push('');
for (const row of downstreamConsumers) human.push(`- **${row.consumer}:** ${row.must_receive.join('; ')}.`);
human.push('');
human.push('No downstream screen, payment owner, settlement route or cancellation/reversion owner may contain a special branch for the revised Draft route. If it needs one, the new route has failed parity.');
human.push('');
human.push('The machine contract separately binds the installed owner and source evidence for all thirteen downstream boundaries: summary/eligibility, Current Payment Status, PAYE net, bank projection, CSV, execution scope, transfer preparation, provider claim, settlement, remittance, pre-provider cancellation, settled correction/reversion and frontend interpretation.');
human.push('');
human.push('## How zero drift is proved');
human.push('');
human.push('Two equivalent fresh fixture universes are created. The accepted route builds one Draft and the candidate route builds the other. Generated technical IDs are matched by stable business role; every other typed field, identity, status, amount, VAT value and hash is compared exactly. The same downstream projections and lifecycle are then exercised. Totals are checked, but totals can never substitute for constituent-by-constituent equality.');
human.push('');
human.push('## Known execution faults are not new policy');
human.push('');
for (const row of contract.known_execution_findings_separate_from_policy) human.push(`- **${row.finding}:** ${row.status}. Policy changed: ${row.policy_delta ? 'yes' : 'no'}.`);

const codex = [];
codex.push('# Codex implementation and zero-drift parity specification');
codex.push('');
codex.push('The adjacent canonical JSON is the controlling machine contract. This document tells an implementation agent how to use it. It does not authorise code, database, Worker, deployment, Draft or payment mutation.');
codex.push('');
codex.push('## Required implementation discipline');
codex.push('');
codex.push('1. Read the applicable `AGENTS.md`, the complete Banking Pay Bible, this document and the canonical JSON immediately before any edit.');
codex.push('2. Freeze exact source, installed Miget, Worker, frontend and Bible identities. Never infer installed behavior from source alone.');
codex.push('2a. Bind the exact WORKBENCH_SETTLED_CERTIFICATION_V1 artifact SHA-256 from the canonical JSON. Treat it as a data-free input contract until a populated, sealed and server-readable instance is separately proved; never reconstruct its selection in the Draft consumer.');
codex.push('3. Build a consumer graph first. Every current reader of every durable Draft artifact is a parity consumer, even if it is not on the Create Draft screen.');
codex.push('4. Change orchestration only. Reuse each existing financial owner and equation. A duplicated calculation is a second oracle and fails review.');
codex.push('5. Preserve every payment-family `family_id`, visible-to-frozen alias, channel variant, state rule and fail-closed condition.');
codex.push('6. Paired Timesheets are mandatory: both supported shapes, ordered member identities, atomic lifecycle, TS_DAY residual, cross-channel resolution, replay and cancellation/reversion.');
codex.push('7. Preserve the endpoint, DRAFT_CREATE operation type, result fields and frontend terminal semantics. New diagnostics may be additive only.');
codex.push('8. Never increase or bypass statement/lock budgets. Performance must come from bounded row-backed pages, fewer network round trips and restartable receipts—not one giant array or long RPC.');
codex.push('9. At the first divergent boundary, assign the existing owner, create a deterministic red fixture, make the smallest policy-neutral correction, then rerun affected classes and the complete suite.');
codex.push('10. No enablement until one fresh, complete post-correction audit has zero unexplained divergences, TODO acceptance gates, skipped acceptance cases, mutation survivors or Bible contradictions.');
codex.push('');
codex.push('## Source-to-decision index');
codex.push('');
for (const row of families) {
  codex.push(`### ${row.family_id}`);
  codex.push('');
  codex.push(`- Current policy outcome: ${row.draft_result}`);
  codex.push(`- Amount owner: ${row.amount_owner}`);
  codex.push(`- Stable identity: ${row.identity.join('; ')}.`);
  codex.push(`- Fail closed: ${row.fail_closed.join('; ')}.`);
  codex.push('- Evidence:');
  for (const ev of row.source_evidence) codex.push(`  - \`${ev.path}:${ev.lines}\` — \`${ev.symbol}\`: ${ev.proves}`);
  codex.push('');
}
codex.push('## Frozen vocabulary census');
codex.push('');
for (const [name, values] of Object.entries(vocabularyRegistry)) {
  if (Array.isArray(values)) codex.push(`- **${name}:** ${values.map(value => `\`${value}\``).join(', ')}`);
}
codex.push('');
codex.push('## Full-row parity protocol');
codex.push('');
codex.push('For every artifact family, export full typed rows in stable business order. Create a role map only for generated IDs (for example, PAYE batch for the same fixture role). Replace only those IDs and directly dependent technical timestamps that the fixture specification explicitly permits. Compare all remaining values exactly. Money remains canonical two-decimal values; item/status/key/hash/channel/tax/VAT fields are never normalised.');
codex.push('');
codex.push('The comparison must then read Current Payment Status, PAYE Worksheet, Umbrella payment evidence, Overview, Execute Payment eligibility/preview and bank projection from each Draft and compare their complete typed outputs. Finally, one disposable candidate-route Draft must use the unchanged downstream execution/cancellation/reversion path with no route-specific branch.');
codex.push('');
codex.push('## Downstream owner census');
codex.push('');
for (const row of downstreamOwnerCensus) {
  codex.push(`### ${row.boundary}`);
  codex.push('');
  codex.push(`- Owner: \`${row.owner}\``);
  codex.push(`- Decision: ${row.decision}`);
  codex.push(`- Reads: ${row.reads.join('; ')}.`);
  codex.push(`- Source: ${row.source}.`);
  if (row.installed_definition_sha256) codex.push(`- Installed Miget definition SHA-256: \`${Array.isArray(row.installed_definition_sha256) ? row.installed_definition_sha256.join('`, `') : row.installed_definition_sha256}\`.`);
  codex.push('');
}
codex.push('');
codex.push('## Finite equivalence-class ledger');
codex.push('');
for (const row of equivalenceClasses) codex.push(`- \`${row.class_id}\` → \`${row.family_id}\` (${row.channel}): ${row.condition}.`);

const lifecycle = [];
lifecycle.push('# Banking Pay: Create Draft to Execute Payment lifecycle map');
lifecycle.push('');
lifecycle.push('This is a chronological navigation map derived from `BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json`. The canonical JSON contract remains the sole policy authority. This file does not create another financial oracle, change policy, or authorise a real Draft or payment. Repository folders still named `supabase` are historical source-path names; Miget TEST is the database runtime authority.');
lifecycle.push('');
lifecycle.push('## The zero-drift promise');
lifecycle.push('');
lifecycle.push('A revised Create Draft route may change only how bounded work is transported, paged, resumed and reported. It must finish with the same frozen business rows and the same operation result. Every later Banking Pay owner must receive the same input and must make the same decision without knowing which Draft route created it.');
lifecycle.push('');
lifecycle.push('```mermaid');
lifecycle.push('flowchart TD');
lifecycle.push('  W[Certified current Workbench selection] --> D[Create Draft: freeze exact selected constituents]');
lifecycle.push('  D --> A[Same batches, candidates, items, allocations, reservations, snapshots, breakdowns and attestations]');
lifecycle.push('  A --> C{Payment channel}');
lifecycle.push('  C -- PAYE --> P[PAYE Worksheet: user saves imported net pay]');
lifecycle.push('  C -- Umbrella --> U[Use frozen Umbrella payee and ex-VAT/VAT totals]');
lifecycle.push('  P --> B[Authoritative bank-payment projection]');
lifecycle.push('  U --> B');
lifecycle.push('  B --> E[Execute Payment eligibility and preview]');
lifecycle.push('  E --> R{Execution route}');
lifecycle.push('  R -- Immediate bank --> I[Prepare and authorise now]');
lifecycle.push('  R -- Scheduled bank --> S[Prepare and authorise, then wait until schedule]');
lifecycle.push('  R -- CSV or external --> L[Prepare the same frozen scope for local settlement]');
lifecycle.push('  I --> X[Existing submission, status, settlement and remittance owners]');
lifecycle.push('  S --> X');
lifecycle.push('  L --> X');
lifecycle.push('  A --> K{Cancellation scope and lifecycle}');
lifecycle.push('  K -- Whole Draft batch --> N[Whole-batch Draft cancellation wrapper]');
lifecycle.push('  K -- One whole Candidate before execution --> O[Candidate-scoped DRAFT_CANCEL]');
lifecycle.push('  S --> T[Candidate-scoped future-dated scheduled-not-paid cancellation when still unsent]');
lifecycle.push('  X --> Q[Submitted or settled correction/certified reversion where allowed]');
lifecycle.push('```');
lifecycle.push('');
lifecycle.push('## Chronological contract');
lifecycle.push('');
lifecycle.push('### 1. Workbench hands over one exact selected universe');
lifecycle.push('');
lifecycle.push('- Input authority: the settled Workbench certificate, once populated and installed, supplies the complete ordered constituent and partition identity, not browser-loaded rows.');
lifecycle.push('- The hand-off binds session/version/generation, exact selected count, stable constituent digest, partition digest and accepted source/install/deployment identities.');
lifecycle.push('- A stale, incomplete, duplicated, missing or mismatched selection fails before any Draft finance work. Unloaded pages and Candidate independence must not hide selected Timesheets.');
lifecycle.push('- Before freezing, the current Workbench is authoritative. After freezing, the Workbench cannot silently rewrite or poison the Draft.');
lifecycle.push('');
lifecycle.push('### 2. Create Draft freezes the accepted policy outcome');
lifecycle.push('');
lifecycle.push('The current phase order and decision owners are:');
lifecycle.push('');
for (const [index, stage] of stageContract.entries()) lifecycle.push(`${index + 1}. **${stage.phase}** — ${stage.decision} Owner: \`${stage.owner}\`.`);
lifecycle.push('');
lifecycle.push('The revised route must leave all of these durable outputs equivalent to the accepted route:');
lifecycle.push('');
for (const artifact of durableArtifactContracts) lifecycle.push(`- **${artifact.artifact}:** ${artifact.comparison_rule}`);
lifecycle.push('');
lifecycle.push('### 3. Draft completion is the boundary from live to frozen authority');
lifecycle.push('');
lifecycle.push('- The completed Draft contains the same PAYE/Umbrella batch split, Candidate membership, stable source/economic keys, amounts, VAT, reservations, Timesheet snapshots, item breakdowns, finance effects, expected-effect attestations, provenance and fast-reversion lineage.');
lifecycle.push('- Overview, Current Payment Status and every execution projection read those frozen artifacts. They do not recalculate the original Workbench economics.');
lifecycle.push('- Operation response fields, created batch IDs, status, phase, error and terminal interpretation remain backward-compatible. New diagnostics may be additive only.');
lifecycle.push('');
lifecycle.push('### 4. PAYE Worksheet is an intentional intermediate step');
lifecycle.push('');
lifecycle.push('- Owner: `public.pay_set_paye_net_manual`. It applies only to PAYE batches and rejects Loans batches.');
lifecycle.push('- The user imports or enters each PAYE Candidate net amount before execution. The owner validates exact frozen Candidate membership, mutability and freshness before saving it.');
lifecycle.push('- `GROSS_ADD` and `GROSS_DEDUCT` belong inside the payroll net calculation. `NET_ADD` and `NET_DEDUCT` are applied after the imported payroll net. These four labels are not interchangeable.');
lifecycle.push('- PAYE net cannot be changed once the payment has crossed into scheduled, submitted, committed, executed, paid, settled or cancelled lifecycle states.');
lifecycle.push('- The saved net value and its state hash become part of the authoritative bank-payment projection. A missing required PAYE net blocks execution; Create Draft must never invent it.');
lifecycle.push('');
lifecycle.push('### 5. Umbrella preparation stays separate');
lifecycle.push('');
lifecycle.push('- Umbrella does not use the PAYE manual-net owner. Its frozen owner retains the exact Umbrella payee, payment channel, ex-VAT amount, VAT amount, inclusive amount, bank-details evidence and week/payment grouping.');
lifecycle.push('- PAYE tax labels must not leak into Umbrella calculations. Conversely, Umbrella VAT or payee routing must not be inferred for PAYE.');
lifecycle.push('- Mixed Drafts remain separate PAYE and Umbrella partitions even when they came from one Create Draft operation.');
lifecycle.push('');
lifecycle.push('### 6. The bank-payment projection is the Execute Payment scalar authority');
lifecycle.push('');
lifecycle.push('- Owner: `public._pay_batch_bank_payment_projection_rows`.');
lifecycle.push('- PAYE beneficiary amounts come from the frozen Draft plus the valid saved PAYE net state. Umbrella beneficiary amounts and payee identity come from the frozen Umbrella evidence.');
lifecycle.push('- Positive and explicit-zero groups, projection row counts and hashes remain exact. No Worker or frontend code may repair an amount, beneficiary or channel.');
lifecycle.push('');
lifecycle.push('### 7. Execute Payment validates the frozen result');
lifecycle.push('');
lifecycle.push('- `public.pay_batch_execution_summary_get` and `public.pay_batch_payment_status_page_v1` expose the current eligibility, blockers, rows, amounts, statuses and allowed actions.');
lifecycle.push('- The execution handler accepts the established scopes `ALL`, `PAYE`, `UMBRELLA` and `LOANS`; routes `STANDARD_BANK`, `CSV_SETTLEMENT` and `EXTERNAL_SETTLEMENT`; and schedule kinds `IMMEDIATE` and `SCHEDULED`.');
lifecycle.push('- Freshness, integrity, reauthorisation, bank/rail availability, blocked-funds retry rules, CSV currency and remittance-suppression confirmation remain unchanged.');
lifecycle.push('- The revised Draft route/version is not an execution input. There must be no route-specific branch; if Execute Payment requires one, parity has failed.');
lifecycle.push('');
lifecycle.push('### 8. Transfer scope and preparation remain row-backed and authoritative');
lifecycle.push('');
lifecycle.push('- `public.pay_execute_bank_transfer_scope_seed` selects the exact frozen Candidate/channel/payment groups and fails if required PAYE net is absent.');
lifecycle.push('- `public.pay_execute_bank_transfer_chunk_prepare` creates or reuses the exact transfer preparation evidence, including void overlays and projection hashes, without changing the beneficiary or amount.');
lifecycle.push('- Bounded pages and resumable receipts are permitted. Giant repeated arrays, hidden truncation, relaxed timeouts and partial frozen scope are prohibited.');
lifecycle.push('');
lifecycle.push('### 9. Immediate and scheduled bank payments make the same financial decision');
lifecycle.push('');
lifecycle.push('- Both routes call the unchanged preparation and authorisation owners with the same frozen projection, scope, payment date, funding account, warnings and remittance decision.');
lifecycle.push('- `IMMEDIATE` can continue to provider submission after authorisation. `SCHEDULED` records the same authorised intent and waits until the scheduled time. The difference is timing, not Candidate membership, beneficiary, amount, VAT, item or policy.');
lifecycle.push('- A stale previous authorisation request may be safely cancelled and replayed once under the same operation idempotency boundary; it must not create a second payment intent.');
lifecycle.push('');
lifecycle.push('### 10. CSV and external settlement use the same frozen projection');
lifecycle.push('');
lifecycle.push('- CSV export reads the exact bank projection and state hashes. CSV/local settlement cannot continue to a provider-submission phase.');
lifecycle.push('- External settlement requires its existing evidence/comment contract. Neither route may silently change the amount or claim a provider submission.');
lifecycle.push('- Verification may prove preparation, validation and safe routing, but it must not perform a real settlement.');
lifecycle.push('');
lifecycle.push('### 11. Submission, status, settlement and remittance remain unchanged');
lifecycle.push('');
lifecycle.push('- Provider preparation and claim consume only the existing prepared transfer scope and idempotency evidence. Tests stop before any real provider call.');
lifecycle.push('- Current Payment Status continues to show the same per-payment rows, amounts, statuses, actions, correction evidence and retry/blocked-funds state.');
lifecycle.push('- Settlement consumes the same submitted/settled transfer evidence. Remittance generation or suppression consumes the same frozen items, breakdowns, payee and execution state. Tests must not send a remittance.');
lifecycle.push('');
lifecycle.push('### 12. Cancellation before provider payment preserves the same outcome');
lifecycle.push('');
lifecycle.push('- Whole-batch and whole-Candidate cancellation are different contracts. `public.pay_batch_cancel` is the whole-batch Draft wrapper only; it must never be used as a shortcut for a Candidate selection.');
lifecycle.push('- For one whole Candidate, the Worker freezes the reviewed `CANDIDATES` scope through `pay_payment_correction_request_start`. `_pay_payment_correction_selected_items` and `pay_payment_correction_selection_prepare_chunk_v1` bind that Candidate membership to its exact active items, allocation/reservation and transfer evidence before mutation.');
lifecycle.push('- An untouched post-Draft/pre-execution Candidate follows the existing `DRAFT_CANCEL` overlay path. The Candidate-scoped overlay owner voids/releases the selected Candidate artifacts and republishes its Workbench source from frozen lineage. Every unrelated Candidate row, item, allocation, reservation, payment readiness and eventual bank projection must remain unchanged.');
lifecycle.push('- A Candidate whose future-dated payment has been authorised/scheduled but remains local and not sent is classified as `SCHEDULED_LOCAL_NOT_SENT` (or `LOCAL_PREPARED_NOT_SENT`) and follows `PRE_PROVIDER_CANCEL_AND_RECALCULATE`. `pay_payment_correction_process_chunk` invokes the bounded Candidate page and `pay_pre_bank_cancel_apply_work_item`; settled/provider-submitted evidence blocks this route.');
lifecycle.push('- Candidate cancellation must be whole-Candidate: no missing active item, finance case/component, reservation or transfer is allowed. Exact reservations/items are voided or released, summaries are recalculated, and Workbench source reappears exactly once. No live economic reconstruction or effect on another Candidate is allowed.');
lifecycle.push('');
lifecycle.push('### 13. Executed-not-paid correction and certified reversion use frozen lineage');
lifecycle.push('');
lifecycle.push('- “Executed” does not by itself mean money was paid. A future-dated scheduled/local payment that is still unsent uses the Candidate-scoped pre-provider route above. Once provider submission, completion or settlement evidence exists, that route must reject rather than pretending it is still local.');
lifecycle.push('- `public.pay_payment_correction_request_start` owns correction admission and rejects terminal or unsafe requests.');
lifecycle.push('- `public.pay_settled_payment_reversal_apply_work_item` applies a certified safe reversion only from the frozen source/effect/settlement lineage.');
lifecycle.push('- Where safe reversion is not established, the existing correction, retry, blocked-funds or manual-resolution fallback remains authoritative. A revised Draft route cannot weaken that decision.');
lifecycle.push('');
lifecycle.push('## Channel and route comparison');
lifecycle.push('');
lifecycle.push('| Point | PAYE | Umbrella | Must remain identical between Draft routes |');
lifecycle.push('|---|---|---|---|');
lifecycle.push('| Draft grouping | PAYE batch/Candidate membership | Umbrella channel/payee grouping | Exact batches and membership |');
lifecycle.push('| Before execution | User-saved PAYE net required where applicable | No PAYE-net entry; frozen ex-VAT/VAT/payee evidence | Same readiness/blockers |');
lifecycle.push('| Bank amount | Frozen Draft plus saved PAYE net; gross/net labels remain distinct | Frozen Umbrella inclusive payment evidence | Every penny and projection hash |');
lifecycle.push('| Beneficiary | Candidate destination | Frozen Umbrella payee destination | Same identity and bank-details hash |');
lifecycle.push('| Immediate versus scheduled | Same amount and scope; timing differs | Same amount and scope; timing differs | Same authorised intent |');
lifecycle.push('| Whole-Candidate cancellation/reversion | Frozen Candidate/item/allocation/reservation/effect lineage | Frozen Candidate/Umbrella/item/allocation/reservation/effect lineage | Same selected Candidate release/void/reappearance; every unrelated Candidate unchanged |');
lifecycle.push('');
lifecycle.push('## Downstream owner evidence');
lifecycle.push('');
lifecycle.push('| Boundary | Existing owner | Decision that must not change | Source |');
lifecycle.push('|---|---|---|---|');
for (const row of downstreamOwnerCensus) lifecycle.push(`| ${q(row.boundary)} | \`${q(row.owner)}\` | ${q(row.decision)} | ${q(row.source)} |`);
lifecycle.push('');
lifecycle.push('## Exact parity gate before enablement');
lifecycle.push('');
lifecycle.push('For two equivalent fresh fixture universes, compare the accepted Draft route with the revised route in stable business order. Generated technical IDs may be role-mapped only where the fixture expressly permits it. Compare everything else as typed data:');
lifecycle.push('');
lifecycle.push('- operation response, terminal phase and created batch IDs by stable role;');
lifecycle.push('- channel groups and complete Candidate/Timesheet/paired-Timesheet constituent membership;');
lifecycle.push('- item source/economic keys, allocation identities and every pence amount;');
lifecycle.push('- PAYE gross/net classifications, saved PAYE net values and state hashes;');
lifecycle.push('- Umbrella ex-VAT, VAT, inclusive totals, payee and bank-details evidence;');
lifecycle.push('- reservations, retention markers, Timesheet snapshots and item breakdowns;');
lifecycle.push('- finance effects, expected-effect attestations, post-Draft authority and fast-reversion lineage;');
lifecycle.push('- Overview, Current Payment Status, PAYE Worksheet, Execute Payment eligibility/preview and bank projection outputs;');
lifecycle.push('- immediate and scheduled authorisation preparation, CSV/external preparation, whole-batch cancellation, whole-Candidate pre-execution cancellation, whole-Candidate future-dated scheduled-not-paid cancellation and certified-reversion eligibility;');
lifecycle.push('- replay, response loss, concurrency and failure rollback with no partial Draft or external payment effect.');
lifecycle.push('');
lifecycle.push('The executable matrix must cover all finite classes in the canonical contract, including paired Timesheets and all supported cross-class combinations. Totals are secondary evidence: one missing or misclassified constituent fails parity even when totals happen to match.');
lifecycle.push('');
lifecycle.push('## Hard stop conditions');
lifecycle.push('');
lifecycle.push('- Any changed payment eligibility, amount, tax, VAT, payment method, beneficiary, grouping, approval, hold, exception or final outcome.');
lifecycle.push('- Any missing, extra, duplicated, truncated or re-ordered constituent outside the declared stable order.');
lifecycle.push('- Any downstream compatibility branch for the revised Draft route.');
lifecycle.push('- Any increased/disabled statement or lock timeout, giant pre-chunk array, unbounded scan or partial frozen state.');
lifecycle.push('- Any real provider, payment, settlement or remittance action during parity testing.');
lifecycle.push('- Any unexplained difference, open acceptance TODO, skipped supported case, surviving mutation or Banking Pay Bible contradiction.');
lifecycle.push('');
lifecycle.push('## Current proof status');
lifecycle.push('');
lifecycle.push('- **Policy mapping:** complete in the adjacent canonical JSON and its 88 finite classes.');
lifecycle.push('- **This lifecycle relationship map:** generated from the same source-bound owner census; it adds no policy.');
lifecycle.push('- **Implementation parity:** remains a separate executable gate. Mapping an owner is not proof that the revised route has produced byte-for-byte equivalent downstream inputs.');
lifecycle.push('- **Miget installation and final acceptance:** require the approved coherent release, exact installed identities and one fresh post-install audit.');

fs.mkdirSync(outDir, { recursive: true });
const canonicalText = `${JSON.stringify(contract, null, 2)}\n`;
const schemaText = `${JSON.stringify(schema, null, 2)}\n`;
const humanText = `${human.join('\n')}\n`;
const codexText = `${codex.join('\n')}\n`;
const lifecycleText = `${lifecycle.join('\n')}\n`;
fs.writeFileSync(path.join(outDir, 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json'), canonicalText);
fs.writeFileSync(path.join(outDir, 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.schema.json'), schemaText);
fs.writeFileSync(path.join(outDir, 'BANKING_PAY_HOW_EVERY_PAYMENT_TYPE_WORKS.md'), humanText);
fs.writeFileSync(path.join(outDir, 'CODEX_ZERO_DRIFT_IMPLEMENTATION_SPEC.md'), codexText);
fs.writeFileSync(path.join(outDir, 'BANKING_PAY_CREATE_DRAFT_TO_EXECUTE_PAYMENT_LIFECYCLE_MAP_V1.md'), lifecycleText);

const checksums = {
  contract: 'BANKING_PAY_POLICY_CONTRACT_ARTIFACT_CHECKSUMS_V1',
  files: [
    ['BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json', canonicalText],
    ['BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.schema.json', schemaText],
    ['BANKING_PAY_HOW_EVERY_PAYMENT_TYPE_WORKS.md', humanText],
    ['CODEX_ZERO_DRIFT_IMPLEMENTATION_SPEC.md', codexText],
    ['BANKING_PAY_CREATE_DRAFT_TO_EXECUTE_PAYMENT_LIFECYCLE_MAP_V1.md', lifecycleText]
  ].map(([file, contents]) => ({ file, bytes: Buffer.byteLength(contents), sha256: sha256(contents) }))
};
fs.writeFileSync(path.join(outDir, 'CHECKSUMS.json'), `${JSON.stringify(checksums, null, 2)}\n`);

console.log(JSON.stringify({ outDir, payment_family_count: families.length, equivalence_class_count: equivalenceClasses.length, stage_count: stageContract.length, downstream_consumer_count: downstreamConsumers.length, checksums }, null, 2));
