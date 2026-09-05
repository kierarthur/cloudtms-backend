# Codex implementation and zero-drift parity specification

The adjacent canonical JSON is the controlling machine contract. This document tells an implementation agent how to use it. It does not authorise code, database, Worker, deployment, Draft or payment mutation.

## Required implementation discipline

1. Read the applicable `AGENTS.md`, the complete Banking Pay Bible, this document and the canonical JSON immediately before any edit.
2. Freeze exact source, installed Miget, Worker, frontend and Bible identities. Never infer installed behavior from source alone.
2a. Bind the exact WORKBENCH_SETTLED_CERTIFICATION_V1 artifact SHA-256 from the canonical JSON. Treat it as a data-free input contract until a populated, sealed and server-readable instance is separately proved; never reconstruct its selection in the Draft consumer.
3. Build a consumer graph first. Every current reader of every durable Draft artifact is a parity consumer, even if it is not on the Create Draft screen.
4. Change orchestration only. Reuse each existing financial owner and equation. A duplicated calculation is a second oracle and fails review.
5. Preserve every payment-family `family_id`, visible-to-frozen alias, channel variant, state rule and fail-closed condition.
6. Paired Timesheets are mandatory: both supported shapes, ordered member identities, atomic lifecycle, TS_DAY residual, cross-channel resolution, replay and cancellation/reversion.
7. Preserve the endpoint, DRAFT_CREATE operation type, result fields and frontend terminal semantics. New diagnostics may be additive only.
8. Never increase or bypass statement/lock budgets. Performance must come from bounded row-backed pages, fewer network round trips and restartable receipts—not one giant array or long RPC.
9. At the first divergent boundary, assign the existing owner, create a deterministic red fixture, make the smallest policy-neutral correction, then rerun affected classes and the complete suite.
10. No enablement until one fresh, complete post-correction audit has zero unexplained divergences, TODO acceptance gates, skipped acceptance cases, mutation survivors or Bible contradictions.

## Source-to-decision index

### ordinary_timesheet_components

- Current policy outcome: One or more frozen SEGMENT_DELTA items retain the exact component/economic identities; totals are a consequence, not the identity.
- Amount owner: Current pre-Draft entitlement minus settled baseline minus active source reservation, at exact component identity.
- Stable identity: physical Timesheet ID; segment/component source identity; economic key TS_DAY or TS_TOTAL; rate/revision/source fingerprint.
- Fail closed: missing or ambiguous rate/payment method; negative ordinary parent without certified recovery shape; stale source revision; duplicate/missing selected identity.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1330-1449` — `canonical_timesheet_presentation_rows`: TIMESHEET_PAYMENT readiness, PAYE treatment and amount
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2270-2564` — `timesheet_allocation_component_lines`: component-level economic identity and frozen item routing

### paired_timesheet_reversal_replacement

- Current policy outcome: The pair is not collapsed into one invented payment. Frozen constituents preserve the ordered pair lineage and component identity, while the latest positive leg may act as the carrier Timesheet.
- Amount owner: The correction residual owner sums current TS_DAY truth, subtracts settled baseline and active reservations, then accounts for settled/reserved correction finance movements. It does not include expenses, mileage, additional codes, loans or unrelated overpayments.
- Stable identity: root Timesheet ID; correction operation ID; correction chain ID; ordered member Timesheet IDs; pair/chain fingerprint; per-leg policy fingerprint; TS_DAY economic key.
- Fail closed: broken pair; duplicate role; member-count mismatch; cycle/depth overflow; mixed worker or mixed client; stale chain or leg fingerprint; paid/invoiced lifecycle conflict; unresolved cross-channel target amount; reservation overrun.
- Evidence:
  - `supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql:41-236` — `timesheet_correction_chain_scope_v1`: bounded chain, exact unit shape, fingerprints and member order
  - `supabase/repeatable/21072026_1235_08_timesheet_correction_pair_transition_v1.sql:41-197` — `timesheet_correction_pair_transition_v1`: atomic lifecycle, per-leg evidence and paid/invoiced guards
  - `supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql:105-338` — `pay_correction_chain_residual_v1`: single-worker/client/channel and policy-anchor validation
  - `supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql:340-1300` — `pay_correction_chain_residual_v1`: TS_DAY residual, reservations, settlements, stable identity and draftability

### paired_timesheet_reversal_only

- Current policy outcome: Only the actual residual is frozen. A reversal row is not automatically converted into a payment or recovery merely because it is negative.
- Amount owner: The same correction residual owner; no second cancellation arithmetic is permitted.
- Stable identity: root Timesheet ID; one reversal member; correction operation/chain IDs; envelope and leg fingerprints; TS_DAY key.
- Fail closed: unexpected replacement; wrong expected count/roles; unproved signed recovery evidence; stale envelope/leg; mixed worker/client/channel.
- Evidence:
  - `supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql:101-157` — `timesheet_correction_chain_scope_v1`: REVERSAL_ONLY exact shape
  - `supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql:1-26` — `pay_correction_chain_residual_v1`: narrow residual scope and exclusions
  - `supabase/repeatable/21072026_1820_00a_import_apply_operation_claim_v2.sql:45-104` — `_import_apply_operation_claim_core_v2`: CANCELLATION maps only to REVERSAL_ONLY; CHANGED_HOURS maps only to REVERSAL_REPLACEMENT
  - `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql:182957-183312` — `timesheet_standard_delete_preview_v1`: current changed-hours pair is an exact two-row delete target and malformed/single-child shapes block
  - `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql:153787-154375` — `timesheet_standard_delete_apply_v1`: apply consumes and deletes the exact preview target set; it does not reclassify one surviving member

### timesheet_expenses_and_mileage

- Current policy outcome: MILEAGE maps to MILEAGE_DELTA; the other expense codes map to EXPENSE_DELTA. Source keys and expense code remain distinct.
- Amount owner: Current component amount ex VAT after settled baseline and active reservation.
- Stable identity: Timesheet ID; EXPENSE_CODE economic key; expense code; source-basis fingerprint.
- Fail closed: missing expense code/fingerprint; unsupported expense code; stale source basis; snoozed or blocked component.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2270-2564` — `timesheet_allocation_component_lines`: EXPENSE_CODE identity, MILEAGE routing and channel treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:3169-3227` — `candidate totals`: separate expense-code totals

### additional_and_adjustment_components

- Current policy outcome: Stable key and source identity survive; a label collision must not turn a component into a finance case.
- Amount owner: Current component entitlement less exact settled baseline and reservation.
- Stable identity: Timesheet ID; component key type; component key value; source/revision fingerprint.
- Fail closed: ambiguous key; duplicate exact identity; stale fingerprint.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2270-2564` — `timesheet_allocation_component_lines`: key-type-specific identity and item routing

### forced_advance_this_payment

- Current policy outcome: Ordinary frozen artifacts plus the existing override evidence; no loan or finance-case item is invented.
- Amount owner: The same Timesheet/component financial owner as an ordinary payment.
- Stable identity: Timesheet/component identity; override ID; reason; actor and lifecycle evidence.
- Fail closed: cancelled/stale override; wrong Timesheet; override without authority.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1330-1449` — `canonical_timesheet_presentation_rows`: is_advanced, override ID and reason without amount rewrite

### prior_paid_part_paid_and_superseded

- Current policy outcome: No duplicate full payment; the exact residual and lineage are frozen.
- Amount owner: truth minus settled baseline minus active reservation, with source-owned recovery/underpayment movements where applicable.
- Stable identity: same physical/economic component identity; settled baseline; active reservation; supersession/current-source fingerprint.
- Fail closed: reservation overrun; same identity already active in another Draft; stale supersession/current revision.
- Evidence:
  - `supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql:343-467` — `raw_outstanding`: truth minus baseline minus reservation equation

### payment_advance_payout

- Current policy outcome: Visible LOAN_PAYOUT becomes frozen LOAN_PAYOUT; allocation linkage and finance-case/component lineage remain exact.
- Amount owner: The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.
- Stable identity: finance case ID; finance component ID; case type; component key type/value; linked Timesheet when present; source basis fingerprint.
- Fail closed: unsupported or hidden visible alias; wrong finance case/component/linkage; stale case residual; channel mismatch; snoozed/blocked/unresolved case; incorrect pre-finance ordinary item link.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1771-1970` — `finance_case_lines`: case type → visible alias, direction, sign and PAYE treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2792-2974` — `canonical_preview_lines finance branch`: stable visible identity, headroom, readiness and source_ref
  - `supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql:1-end` — `pay_batch_apply_finance_adjustments`: unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner

### payment_advance_repayment

- Current policy outcome: Visible PAYMENT_ADVANCE_REPAYMENT becomes frozen LOAN_REPAYMENT; allocation linkage and finance-case/component lineage remain exact.
- Amount owner: The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.
- Stable identity: finance case ID; finance component ID; case type; component key type/value; linked Timesheet when present; source basis fingerprint.
- Fail closed: unsupported or hidden visible alias; wrong finance case/component/linkage; stale case residual; channel mismatch; snoozed/blocked/unresolved case; incorrect pre-finance ordinary item link.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1771-1970` — `finance_case_lines`: case type → visible alias, direction, sign and PAYE treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2792-2974` — `canonical_preview_lines finance branch`: stable visible identity, headroom, readiness and source_ref
  - `supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql:1-end` — `pay_batch_apply_finance_adjustments`: unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner

### overpayment_recovery

- Current policy outcome: Visible OVERPAYMENT_RECOVERY becomes frozen OVERPAYMENT_RECOVERY; allocation linkage and finance-case/component lineage remain exact.
- Amount owner: The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.
- Stable identity: finance case ID; finance component ID; case type; component key type/value; linked Timesheet when present; source basis fingerprint.
- Fail closed: unsupported or hidden visible alias; wrong finance case/component/linkage; stale case residual; channel mismatch; snoozed/blocked/unresolved case; incorrect pre-finance ordinary item link.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1771-1970` — `finance_case_lines`: case type → visible alias, direction, sign and PAYE treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2792-2974` — `canonical_preview_lines finance branch`: stable visible identity, headroom, readiness and source_ref
  - `supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql:1-end` — `pay_batch_apply_finance_adjustments`: unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner

### underpayment_payment

- Current policy outcome: Visible UNDERPAYMENT_PAYMENT becomes frozen UNDERPAYMENT_PAYMENT; allocation linkage and finance-case/component lineage remain exact.
- Amount owner: The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.
- Stable identity: finance case ID; finance component ID; case type; component key type/value; linked Timesheet when present; source basis fingerprint.
- Fail closed: unsupported or hidden visible alias; wrong finance case/component/linkage; stale case residual; channel mismatch; snoozed/blocked/unresolved case; incorrect pre-finance ordinary item link.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1771-1970` — `finance_case_lines`: case type → visible alias, direction, sign and PAYE treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2792-2974` — `canonical_preview_lines finance branch`: stable visible identity, headroom, readiness and source_ref
  - `supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql:1-end` — `pay_batch_apply_finance_adjustments`: unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner

### manual_credit_adjustment

- Current policy outcome: Visible MANUAL_CREDIT_ADJUSTMENT_PAYMENT becomes frozen MANUAL_CREDIT_PAYOUT; allocation linkage and finance-case/component lineage remain exact.
- Amount owner: The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.
- Stable identity: finance case ID; finance component ID; case type; component key type/value; linked Timesheet when present; source basis fingerprint.
- Fail closed: unsupported or hidden visible alias; wrong finance case/component/linkage; stale case residual; channel mismatch; snoozed/blocked/unresolved case; incorrect pre-finance ordinary item link.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1771-1970` — `finance_case_lines`: case type → visible alias, direction, sign and PAYE treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2792-2974` — `canonical_preview_lines finance branch`: stable visible identity, headroom, readiness and source_ref
  - `supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql:1-end` — `pay_batch_apply_finance_adjustments`: unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner

### manual_debt_adjustment

- Current policy outcome: Visible MANUAL_DEBT_RECOVERY becomes frozen MANUAL_DEBT_RECOVERY; allocation linkage and finance-case/component lineage remain exact.
- Amount owner: The existing finance-case due/residual owner and later apply-finance owner. INSERT_ITEMS and orchestration must not calculate finance economics.
- Stable identity: finance case ID; finance component ID; case type; component key type/value; linked Timesheet when present; source basis fingerprint.
- Fail closed: unsupported or hidden visible alias; wrong finance case/component/linkage; stale case residual; channel mismatch; snoozed/blocked/unresolved case; incorrect pre-finance ordinary item link.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:1771-1970` — `finance_case_lines`: case type → visible alias, direction, sign and PAYE treatment
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2792-2974` — `canonical_preview_lines finance branch`: stable visible identity, headroom, readiness and source_ref
  - `supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql:1-end` — `pay_batch_apply_finance_adjustments`: unchanged frozen item, allocation, case state, PAYE/Umbrella and replay owner

### manual_adjustment_carry_forward

- Current policy outcome: The stored source economics and complete correction lineage are frozen without reinterpretation.
- Amount owner: Stored signed ex-VAT/VAT/inclusive amounts and stored tax treatment from the correction owner.
- Stable identity: carry-forward ID; operation source key; source batch/item/transfer/correction lineage; candidate/channel/payee.
- Fail closed: missing source key; already consumed target; candidate/channel/payee mismatch; stale correction lineage.
- Evidence:
  - `supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql:2978-3070` — `manual carry-forward canonical lines`: identity, signed values, tax/VAT and source lineage

### signed_non_charge_recovery

- Current policy outcome: Exact decisive component shape/digest is carried into finalisation and reservation evidence.
- Amount owner: Existing signed-recovery classifier and recovery owner; no label or total-based inference.
- Stable identity: full economic key; amount/bucket/revision/target/conversion; sealed digest; decisive component ID.
- Fail closed: zero or multiple decisive signed matches when one is required; tampered/missing digest; ordinary same-key false positive; amount/key/revision/conversion mismatch.
- Evidence:
  - `supabase/repeatable/01092026_1459_banking_pay_signed_recovery_draft_v1.sql:1-end` — `pay_batch_signed_non_charge_recovery_evidence_v1 and finalizer`: signed classification and frozen evidence checks

## Frozen vocabulary census

- **pay_channels:** `PAYE`, `UMBRELLA`
- **paye_treatments:** `GROSS_ADD`, `GROSS_DEDUCT`, `NET_ADD`, `NET_DEDUCT`, `NONE`
- **readiness_sections:** `READY_TO_PAY`, `CASES_RESOLUTIONS`, `BLOCKED_FOR_PAY`
- **visible_payment_line_types:** `TIMESHEET_PAYMENT`, `LOAN_PAYOUT`, `PAYMENT_ADVANCE_REPAYMENT`, `OVERPAYMENT_RECOVERY`, `UNDERPAYMENT_PAYMENT`, `MANUAL_CREDIT_ADJUSTMENT_PAYMENT`, `MANUAL_DEBT_RECOVERY`, `MANUAL_ADJUSTMENT_CARRY_FORWARD`
- **deliberately_hidden_or_frozen_aliases:** `LOAN_REPAYMENT`, `MANUAL_CREDIT_PAYOUT`
- **frozen_item_types:** `SEGMENT_DELTA`, `EXPENSE_DELTA`, `MILEAGE_DELTA`, `LOAN_PAYOUT`, `LOAN_REPAYMENT`, `OVERPAYMENT_RECOVERY`, `UNDERPAYMENT_PAYMENT`, `MANUAL_CREDIT_PAYOUT`, `MANUAL_DEBT_RECOVERY`
- **finance_case_types:** `PAYMENT_ADVANCE`, `OVERPAYMENT`, `UNDERPAYMENT`, `MANUAL_CREDIT_ADJUSTMENT`, `MANUAL_DEBT_ADJUSTMENT`
- **economic_key_types:** `TS_DAY`, `TS_TOTAL`, `EXPENSE_CODE`, `ADDITIONAL_CODE`, `ADJUSTMENT_CODE`
- **expense_codes:** `EXPENSES`, `TRAVEL`, `ACCOMMODATION`, `OTHER`, `MILEAGE`
- **correction_member_kinds:** `CHANGED_HOURS_REVERSAL`, `CHANGED_HOURS_REPLACEMENT`, `CANCELLATION_REVERSAL`, `CANCELLATION_REPLACEMENT`
- **correction_shapes:** `REVERSAL_REPLACEMENT`, `REVERSAL_ONLY`
- **lifecycle_actions:** `AUTHORISE`, `UNAUTHORISE`, `PROCESS`, `UNPROCESS`

## Full-row parity protocol

For every artifact family, export full typed rows in stable business order. Create a role map only for generated IDs (for example, PAYE batch for the same fixture role). Replace only those IDs and directly dependent technical timestamps that the fixture specification explicitly permits. Compare all remaining values exactly. Money remains canonical two-decimal values; item/status/key/hash/channel/tax/VAT fields are never normalised.

The comparison must then read Current Payment Status, PAYE Worksheet, Umbrella payment evidence, Overview, Execute Payment eligibility/preview and bank projection from each Draft and compare their complete typed outputs. Finally, one disposable candidate-route Draft must use the unchanged downstream execution/cancellation/reversion path with no route-specific branch.

## Downstream owner census

### Batch Overview and execution eligibility

- Owner: `public.pay_batch_execution_summary_get`
- Decision: Produces the authoritative high-level batch, candidate, bank-out and readiness summary consumed before execution.
- Reads: pay_batches; pay_batch_candidates; pay_batch_items; transfers; freshness/rail/PAYE state.
- Source: supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql: current pay_batch_execution_summary_get definition.
- Installed Miget definition SHA-256: `74a7e6845c67592bd7aff10aa67894f9ace795a4852979df9a4a0ce0d79b8207`.

### Current Payment Status

- Owner: `public.pay_batch_payment_status_page_v1`
- Decision: Returns stable paged payment rows, statuses and exact allowed actions; the frontend must not repair or infer them.
- Reads: frozen batch/candidate/item/allocation; bank transfers/events; correction/cancel/retry evidence.
- Source: supabase/repeatable/04082026_1146_pay_batch_payment_status_page_v1.sql.
- Installed Miget definition SHA-256: `14ef0f93702c8d07a3cce7f572a0a0e9f5da677b61c473e557cb4a52859052dd`.

### PAYE Worksheet save and bank scalar

- Owner: `public.pay_set_paye_net_manual`
- Decision: Validates and saves imported PAYE net amounts; gross items are already reflected while net items adjust afterward.
- Reads: frozen PAYE items; GROSS_ADD/GROSS_DEDUCT schedule; NET_ADD/NET_DEDUCT schedule; candidate membership.
- Source: supabase/repeatable/21072026_1235_48_pay_set_paye_net_manual.sql.
- Installed Miget definition SHA-256: `67bd40c49284fad3c22177987829506b1e36c1edcd392826c000bdbabab68cec`.

### Frozen bank payment projection

- Owner: `public._pay_batch_bank_payment_projection_rows`
- Decision: Produces the final per-beneficiary bank amount and projection hash used by Execute and CSV.
- Reads: frozen batch/candidate/item amounts; saved PAYE net; Umbrella totals/payee; void/status evidence.
- Source: supabase/repeatable/20072026_1105_resolve_paye_deduction_bank_projection.sql.
- Installed Miget definition SHA-256: `4ed334ff8314f9e7c6325650b431fafb8c8db0a03ed9d7ebfcc7bcadff89384d`.

### Bank CSV evidence

- Owner: `public.pay_bank_csv_export_summary_get plus frozen bank projection`
- Decision: Exports only the exact positive/explicit-zero frozen payment scope and rejects projection or row-count change.
- Reads: bank payment projection rows/hash; PAYE net state hash; batch freshness and rail snapshots.
- Source: supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql: pay_bank_csv_export_summary_get.
- Installed Miget definition SHA-256: `a3f3d9858e2de5963d8df34535cf9576f2e363daef8286cbdc9b086fe18623d0`.

### Execute Payment scope

- Owner: `public.pay_execute_bank_transfer_scope_seed`
- Decision: Seeds the exact transfer work scope; no Create Draft version is an input.
- Reads: frozen projection; candidate/channel membership; freshness/integrity; retry-blocked-funds state.
- Source: supabase/repeatable/20072026_1215_align_transfer_scope_with_bank_projection.sql.
- Installed Miget definition SHA-256: `8dfdca201c80304b9278365f205070304acf637e96503c22e255824afd91bc35`.

### Bank-transfer preparation

- Owner: `public.pay_execute_bank_transfer_chunk_prepare`
- Decision: Creates/reuses exact transfer preparation evidence without changing beneficiary or amount.
- Reads: row-backed execution scope; frozen bank projection; provider/rail snapshots; void overlays.
- Source: supabase/repeatable/12082026_1446_pay_execute_bank_transfer_chunk_prepare_voided_overlay.sql.
- Installed Miget definition SHA-256: `9b276ea60596c256380ad7d8d97ad9c223b66c05069e1b9943703a55d752b67c`.

### Provider submission claim

- Owner: `public.pay_bank_transfers_claim_provider_submit_chunk`
- Decision: Claims only safe prepared transfers for unchanged provider submission.
- Reads: prepared transfer rows; operation scope/lease; provider submission state.
- Source: supabase/repeatable/04082026_1154_pay_bank_transfers_claim_provider_submit_chunk.sql.
- Installed Miget definition SHA-256: `64419bd47ef2ad466fc18bf7f2b0c5749795c61cb25bff2edd4d871d266410ff`.

### Settlement

- Owner: `public.pay_settle_rail`
- Decision: Applies authoritative settlement results within existing 6000ms/1000ms budgets.
- Reads: submitted/settled transfer evidence; exact settlement scope; frozen batch artifacts.
- Source: supabase/repeatable/04082026_1211_pay_settle_rail.sql.
- Installed Miget definition SHA-256: `0e5c3e38a314945f67ca716c5e6897ca9ed9730d7bbfd8e39342e31bdf26a83f`.

### Remittance scope and rendering

- Owner: `public.pay_operation_remittance_scope_seed + public.pay_remittance_build + public.pay_remittance_maybe_queue_for_trigger`
- Decision: Generates or suppresses the same candidate/Umbrella remittance from frozen Draft and execution evidence.
- Reads: frozen items/breakdowns; candidate/Umbrella payee; execution/settlement state; remittance settings/suppression.
- Source: supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql: remittance owners.
- Installed Miget definition SHA-256: `140c6bada203f524cfce636c1295453ffe935ae910bf1b1745c92cd00a44ea33`, `fb6d237cee972eaa2d454439c2d8af32111da9bb448ebc2252d8d20e25ad3c1c`, `1745985bd2a722750514f77a5ce5f4ae5744b23dc2c4f0c37f3a8e53faad54db`.

### Whole-batch Draft cancellation wrapper

- Owner: `public.pay_batch_cancel`
- Decision: Cancels only the explicitly requested whole Draft batch. It is not the owner for a whole-Candidate selection and cannot be used to widen Candidate scope.
- Reads: one Draft pay batch; active batch payments; batch cancellation idempotency and audit.
- Source: supabase/repeatable/04082026_1206_pay_batch_cancel.sql.
- Installed Miget definition SHA-256: `10c3ffcaab90dddc256d29ab3df82b1d01d6eaf3a302688a6ba82f04234fb573`.

### Whole-Candidate cancellation before payment

- Owner: `pay_payment_cancelability_diagnostic → correction request/selection → Candidate-scoped Draft overlay or pre-bank cancellation page`
- Decision: Freezes the selected Candidate membership, then voids/releases only that Candidate whole-payment scope. Untouched Draft and future-dated scheduled/local-not-sent states take their established separate branches; unrelated Candidates must remain unchanged and payable.
- Reads: frozen Candidate/item/allocation/reservation scope; provider/bank/settlement evidence; DRAFT_CANCEL or PRE_BANK_CANCEL action; source and post-Draft lineage.
- Source: supabase/repeatable/09082026_1403_pay_payment_correction_selected_items_draft_scope.sql; supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql; supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql; supabase/repeatable/04082026_1208_pay_payment_correction_expand_work.sql; supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql; supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql; supabase/repeatable/09082026_0712_banking_pay_semantic_ready_helpers.sql.
- Installed Miget definition SHA-256: `c2df38403908cefa577c4e593223ca9dda1b708d4e0e1075d395ea2056d07843`, `104d0460c50d4ccdb3e37a6e74d7a8de7a134445e66f3d2c05b15fd65298efb5`, `dc89247906e064fea22d15ebdce07925118aa4fd4624c0dbb5ed9fa0dc3d247d`, `784e3ee63dd7783a969c4c03956d3a021c441aaf94f9df0f328d643cdefaba42`, `3bcf21c8b46a64ce8dee0e9752606056149bc590669ea6065e674c50bdc705a7`, `34b334b3ad89d9de684e0fb86fe1b4f09ba3c4fc7802c53730a1a45e51ecf75a`, `c1868e301765ce99d99e69359b99a8433a358e8742d5f835bf77cd2a7524e2c7`, `ede8dfe93aca76d294b59f4ab59211020fec2ac139d6f753a4c9ce1759eb4b46`, `026ace84cc320562ea9d08631b68f9517dbcdbb3a3c9165b40bdcd1c087a6d22`.

### Executed/settled correction and certified reversion

- Owner: `public.pay_payment_correction_request_start + public.pay_settled_payment_reversal_apply_work_item`
- Decision: Uses certified frozen lineage to reverse/correct or chooses the established safe fallback; never live-reconstructs the Draft.
- Reads: frozen source/effect/settlement lineage; selected payment status scope; accepted resolution and correction work.
- Source: supabase/repeatable/04082026_1207_pay_payment_correction_request_start.sql plus current monolith settled reversal.
- Installed Miget definition SHA-256: `104d0460c50d4ccdb3e37a6e74d7a8de7a134445e66f3d2c05b15fd65298efb5`, `d985e3af4f75e30654d7952b98172a826f5ec93909581d34f4eacee83656c296`.

### Frontend interpretation

- Owner: `TEST-Frontend/js/main.js current Banking Pay child/modal code`
- Decision: Renders current authoritative fields and backend-resolved actions; no route-version repair is permitted.
- Reads: operation response; Overview; PAYE Worksheet; Current Payment Status; remittance and correction outputs.
- Source: TEST-Frontend/js/main.js at frontend commit e58e567f66ed8108a40e3c3e8388dbe33e0b0361.


## Finite equivalence-class ledger

- `ordinary_one_segment_paye` → `ordinary_timesheet_components` (PAYE): one Ready TS_DAY component.
- `ordinary_one_segment_umbrella` → `ordinary_timesheet_components` (UMBRELLA): one Ready TS_DAY component.
- `ordinary_multi_segment_paye` → `ordinary_timesheet_components` (PAYE): multiple independently keyed Ready TS_DAY components.
- `ordinary_multi_segment_umbrella` → `ordinary_timesheet_components` (UMBRELLA): multiple independently keyed Ready TS_DAY components.
- `ordinary_multiple_rate_families` → `ordinary_timesheet_components` (PAYE+UMBRELLA): DAY/NIGHT/SAT/SUN/BH/additional rates remain distinct.
- `ordinary_same_key_multiple_components` → `ordinary_timesheet_components` (PAYE+UMBRELLA): multiple rate/correction components may share TS_DAY or TS_TOTAL and remain distinct physical constituents.
- `saved_rate_resolution` → `ordinary_timesheet_components` (PAYE+UMBRELLA): one current saved resolution binds exact component/source fingerprint and target amount.
- `saved_payment_method_resolution` → `ordinary_timesheet_components` (PAYE↔UMBRELLA): one current saved pay-channel resolution is consumed; the Draft never guesses a channel.
- `paired_reversal_replacement_paye` → `paired_timesheet_reversal_replacement` (PAYE): exact two-leg pair, source and target PAYE.
- `paired_reversal_replacement_umbrella` → `paired_timesheet_reversal_replacement` (UMBRELLA): exact two-leg pair, source and target Umbrella.
- `paired_cross_channel_resolution` → `paired_timesheet_reversal_replacement` (PAYE↔UMBRELLA): source method differs from target and requires one fresh saved target resolution.
- `paired_broken_or_duplicate_leg` → `paired_timesheet_reversal_replacement` (ALL): missing/duplicate role must fail closed.
- `paired_stale_fingerprint` → `paired_timesheet_reversal_replacement` (ALL): stale chain/envelope/leg fingerprint must fail closed.
- `paired_mixed_candidate_or_client` → `paired_timesheet_reversal_replacement` (ALL): pair members span worker/client and must fail closed.
- `paired_paid_or_invoiced_conflict` → `paired_timesheet_reversal_replacement` (ALL): partial lifecycle mutation must fail closed.
- `paired_transition_replay` → `paired_timesheet_reversal_replacement` (ALL): repeat of the same complete lifecycle transition is idempotent; mixed partial state fails closed.
- `paired_draft_response_loss_replay` → `paired_timesheet_reversal_replacement` (PAYE+UMBRELLA): lost Draft response reuses the same ordered chain/component identity without duplicating either leg.
- `paired_reversal_only_paye` → `paired_timesheet_reversal_only` (PAYE): one reversal/no replacement with proved residual.
- `paired_reversal_only_umbrella` → `paired_timesheet_reversal_only` (UMBRELLA): one reversal/no replacement with proved residual.
- `expense_expenses_paye` → `timesheet_expenses_and_mileage` (PAYE): EXPENSES code.
- `expense_travel_paye` → `timesheet_expenses_and_mileage` (PAYE): TRAVEL code.
- `expense_accommodation_paye` → `timesheet_expenses_and_mileage` (PAYE): ACCOMMODATION code.
- `expense_other_paye` → `timesheet_expenses_and_mileage` (PAYE): OTHER code.
- `mileage_paye` → `timesheet_expenses_and_mileage` (PAYE): MILEAGE maps to MILEAGE_DELTA.
- `expense_vat_umbrella` → `timesheet_expenses_and_mileage` (UMBRELLA): VAT-bearing Umbrella expense preserves ex-VAT/VAT/inclusive values.
- `expense_non_vat_umbrella` → `timesheet_expenses_and_mileage` (UMBRELLA): zero/non-VAT Umbrella expense preserves source VAT evidence.
- `mileage_umbrella` → `timesheet_expenses_and_mileage` (UMBRELLA): MILEAGE maps to MILEAGE_DELTA with Umbrella authority.
- `additional_code_component` → `additional_and_adjustment_components` (PAYE+UMBRELLA): ADDITIONAL_CODE exact key.
- `adjustment_code_component` → `additional_and_adjustment_components` (PAYE+UMBRELLA): ADJUSTMENT_CODE exact key.
- `forced_advance_paye` → `forced_advance_this_payment` (PAYE): timing override retains ordinary economics.
- `forced_advance_umbrella` → `forced_advance_this_payment` (UMBRELLA): timing override retains ordinary economics.
- `part_paid_residual` → `prior_paid_part_paid_and_superseded` (PAYE+UMBRELLA): only exact unpaid residual remains.
- `fully_settled_absent` → `prior_paid_part_paid_and_superseded` (PAYE+UMBRELLA): fully settled component absent from Ready.
- `active_reservation_residual` → `prior_paid_part_paid_and_superseded` (PAYE+UMBRELLA): active reservation subtracted exactly.
- `superseded_absent` → `prior_paid_part_paid_and_superseded` (PAYE+UMBRELLA): superseded physical identity absent unless current residual authority says otherwise.
- `cancelled_untouched_reappears_once` → `prior_paid_part_paid_and_superseded` (PAYE+UMBRELLA): released untouched Draft source returns once.
- `advance_payout_paye` → `payment_advance_payout` (PAYE NET_ADD): positive initial payout.
- `advance_payout_umbrella` → `payment_advance_payout` (UMBRELLA): positive initial payout.
- `advance_repayment_paye` → `payment_advance_repayment` (PAYE NET_DEDUCT): visible PAYMENT_ADVANCE_REPAYMENT freezes LOAN_REPAYMENT.
- `advance_repayment_umbrella` → `payment_advance_repayment` (UMBRELLA): visible PAYMENT_ADVANCE_REPAYMENT freezes LOAN_REPAYMENT.
- `advance_part_repaid_residual` → `payment_advance_repayment` (PAYE+UMBRELLA): only remaining scheduled residual.
- `advance_paid_off_absent` → `payment_advance_repayment` (PAYE+UMBRELLA): no remaining due; absent/blocked.
- `advance_cancelled_absent` → `payment_advance_repayment` (PAYE+UMBRELLA): cancelled authority not selectable.
- `advance_voided_reappears_once` → `payment_advance_repayment` (PAYE+UMBRELLA): certified release permits one correct reappearance.
- `overpayment_taxable_paye` → `overpayment_recovery` (PAYE GROSS_DEDUCT): taxable recovery.
- `overpayment_nontaxable_paye` → `overpayment_recovery` (PAYE NET_DEDUCT): non-taxable recovery.
- `overpayment_umbrella` → `overpayment_recovery` (UMBRELLA): Umbrella recovery.
- `overpayment_zero_headroom` → `overpayment_recovery` (PAYE+UMBRELLA): no ordinary positive headroom; zero draftable recovery.
- `overpayment_exact_headroom` → `overpayment_recovery` (PAYE+UMBRELLA): recovery exactly consumes allowed headroom.
- `overpayment_partial_headroom` → `overpayment_recovery` (PAYE+UMBRELLA): partial recovery only; residual remains.
- `underpayment_taxable_paye` → `underpayment_payment` (PAYE GROSS_ADD): taxable credit.
- `underpayment_nontaxable_paye` → `underpayment_payment` (PAYE NET_ADD): non-taxable credit.
- `underpayment_umbrella` → `underpayment_payment` (UMBRELLA): Umbrella credit.
- `manual_credit_taxable_paye` → `manual_credit_adjustment` (PAYE GROSS_ADD): taxable credit.
- `manual_credit_nontaxable_paye` → `manual_credit_adjustment` (PAYE NET_ADD): non-taxable credit.
- `manual_credit_umbrella` → `manual_credit_adjustment` (UMBRELLA): Umbrella credit.
- `manual_debt_taxable_paye` → `manual_debt_adjustment` (PAYE GROSS_DEDUCT): taxable deduction.
- `manual_debt_nontaxable_paye` → `manual_debt_adjustment` (PAYE NET_DEDUCT): non-taxable deduction.
- `manual_debt_umbrella` → `manual_debt_adjustment` (UMBRELLA): Umbrella deduction.
- `manual_debt_zero_headroom` → `manual_debt_adjustment` (PAYE+UMBRELLA): zero permitted recovery.
- `manual_debt_partial_headroom` → `manual_debt_adjustment` (PAYE+UMBRELLA): partial permitted recovery.
- `mixed_recoveries_deterministic_order` → `overpayment_recovery` (PAYE+UMBRELLA): overpayment, manual debt and advance repayment share headroom in source-defined deterministic order.
- `carry_forward_credit` → `manual_adjustment_carry_forward` (PAYE+UMBRELLA): positive stored carry-forward.
- `carry_forward_debit` → `manual_adjustment_carry_forward` (PAYE+UMBRELLA): negative stored carry-forward.
- `signed_positive_return` → `signed_non_charge_recovery` (PAYE+UMBRELLA): exact positive signed evidence.
- `signed_negative_recovery` → `signed_non_charge_recovery` (PAYE+UMBRELLA): exact negative signed evidence.
- `signed_mixed_ordinary_same_key` → `signed_non_charge_recovery` (PAYE+UMBRELLA): ordinary same-key components do not enter signed cardinality.
- `signed_two_decisive_matches` → `signed_non_charge_recovery` (PAYE+UMBRELLA): two genuine signed matches reject.
- `signed_tampered_or_incomplete` → `signed_non_charge_recovery` (PAYE+UMBRELLA): tampered/missing digest or shape rejects.
- `timesheet_snoozed_excluded` → `ordinary_timesheet_components` (PAYE+UMBRELLA): active Timesheet snooze excludes exact scope.
- `segment_snooze_isolation` → `ordinary_timesheet_components` (PAYE+UMBRELLA): only exact segment scope excluded; unrelated Ready components preserved.
- `finance_case_snoozed_excluded` → `overpayment_recovery` (PAYE+UMBRELLA): active finance-case snooze excludes exact case.
- `snooze_expiry_reappears_once` → `ordinary_timesheet_components` (PAYE+UMBRELLA): expired/released source reappears once subject to ordinary checks.
- `action_required_excluded` → `ordinary_timesheet_components` (PAYE+UMBRELLA): unresolved rate/payment/case evidence excluded.
- `blocked_excluded` → `ordinary_timesheet_components` (PAYE+UMBRELLA): blocked source excluded.
- `active_draft_excluded` → `prior_paid_part_paid_and_superseded` (PAYE+UMBRELLA): active frozen reservation prevents duplicate Draft.
- `mixed_candidates_independent` → `ordinary_timesheet_components` (PAYE+UMBRELLA): one worker failure cannot change another worker decision; operation atomicity still applies.
- `mixed_channel_partitions` → `ordinary_timesheet_components` (PAYE+UMBRELLA): PAYE and Umbrella split into exact separate batch partitions.
- `over_100_distinct_timesheets` → `ordinary_timesheet_components` (PAYE+UMBRELLA): complete selection beyond the historical 100-row defect threshold.
- `multi_segment_over_100` → `ordinary_timesheet_components` (PAYE+UMBRELLA): more than 100 components across fewer Timesheets.
- `pagination_1001` → `ordinary_timesheet_components` (PAYE+UMBRELLA): bounded pages reproduce complete 1,001 set.
- `boundary_50000` → `ordinary_timesheet_components` (PAYE+UMBRELLA): bounded complete settled maximum without giant arrays.
- `boundary_50001_reject` → `ordinary_timesheet_components` (PAYE+UMBRELLA): deterministic fail closed before partial state.
- `duplicate_delivery_replay` → `ordinary_timesheet_components` (PAYE+UMBRELLA): same operation/receipt does not duplicate artifacts.
- `response_loss_replay` → `ordinary_timesheet_components` (PAYE+UMBRELLA): lost response resumes from durable receipt without duplicate economics.
- `concurrent_same_selection` → `ordinary_timesheet_components` (PAYE+UMBRELLA): one winner or exact idempotent replay; no double reservation.
- `stale_selection_revision` → `ordinary_timesheet_components` (PAYE+UMBRELLA): context/source change rejects before Draft effects.
- `atomic_multibatch_failure` → `ordinary_timesheet_components` (PAYE+UMBRELLA): failure in one channel/candidate partition leaves no partial authoritative Draft.
