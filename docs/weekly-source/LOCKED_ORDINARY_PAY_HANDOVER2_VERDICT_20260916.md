# HANDOVER 2 re-reviewed compatibility verdict: locked ordinary pay

Review date: 16 September 2026  
Disposition: **SELECT B — `LOCKED_FINAL_SOURCE` complete-entitlement head through the approved `LIVE_ENTITLEMENT_INPUT` seam**  
Alternative A: **REJECT — do not add a positive-only Timesheet/TSFIN correction carrier**  
Scope: design compatibility only; no implementation, release, TEST mutation or payment action

## Correction of the first verdict

The first verdict incorrectly assumed Candidate pay needed to create the later `+7` invoice event. The Plan6 contract proves that invoice and Candidate pay are separate:

- `weekly_source_billing_movements` already owns the immutable source invoice movements;
- cancellation and later reappearance remain the invoice movements `-6` and `+7`, both linked to the original `invoice_timesheet_id`;
- a source billing movement is not a Timesheet-financial or Candidate-pay instruction; and
- the Candidate-pay side needs the complete current entitlement, not another copy of the invoice movement.

The first verdict's reason for selecting A is therefore invalid. This file supersedes that disposition.

## Bounded verdict

Select B for an immutable ordinary root. Publish one current, certified complete-entitlement head with authority kind `LOCKED_FINAL_SOURCE` through the already approved `LIVE_ENTITLEMENT_INPUT` seam.

For source history `+6`, invoice cancellation `-6`, later source reappearance `+7`:

- the invoice ledger remains `+6,-6,+7` and is not read as Candidate pay;
- the locked final-source head publishes complete current Candidate entitlement `7`;
- the existing Workbench compares `7` with frozen settled, recovered and reserved history; and
- the existing Workbench produces the ordinary residual, existing overpayment recovery or no row.

The head is source authority for one complete current entitlement, not a residual, payment instruction, invoice event, correction-chain member or second settlement ledger.

## Why A is rejected

A new positive-only correction carrier would require a new correction kind, shape and role across the correction classifier, policy, chain scope, lifecycle, TSFIN, Workbench residual and invoice-correction consumers. That is unnecessary under the corrected separation and creates a material risk that the same source reappearance is represented once in `weekly_source_billing_movements` and again as a correction event visible to invoice or pay consumers.

Do not extend `_ctms_import_correction_classify_v1`, `_ctms_correction_policy_leg_read_v1`, `timesheet_correction_chain_scope_v1`, `timesheet_correction_pair_transition_v1`, `pay_correction_chain_residual_v1` or `invoice_correction_pair_scope_v1` for this Weekly Source reappearance. Do not create an appearance Timesheet, a correction TSFIN row or an invoice line from Candidate pay.

The existing ordinary correction model remains unchanged for its existing non-Weekly-Source routes. It is not the Plan6 locked-final-source representation.

## Exact Banking and Workbench changes required

1. Add one private, fixed-size current head per immutable ordinary root. It must bind the root Timesheet, Candidate, Contract, Weekly scope, current Weekly Source final revision/generation, complete component inventory/digest, complete entitlement digest, certified-zero/absent meaning, authority kind `LOCKED_FINAL_SOURCE`, monotonic revision and committed publication receipt.
2. Publish the head only from the existing Weekly Source finalisation/currentness owner after the complete source revision and all required provenance are certified. The same source lock/currentness boundary must publish the new head or certified zero atomically with its immutable receipt.
3. Do not treat `weekly_source_billing_movements` as pay input. The head may bind their source revision/provenance for completeness, but it must not sum invoice movements to derive Candidate entitlement.
4. Apply the already reviewed narrow selection change in `private.pay_workbench_unit_economic_occurrence_page_v1(...)`: for an exact current `LOCKED_FINAL_SOURCE` root, build `LIVE_ENTITLEMENT_INPUT` occurrences from the certified head rather than requiring a new physical correction Timesheet/TSFIN carrier.
5. Preserve `private.pay_workbench_timesheet_dependency_closure_v2(uuid,jsonb,integer)` and its caller unchanged. The ordinary root remains the dependency identity; the head revision/digest becomes part of that root's source/currentness evidence, not another dependency family.
6. Use the existing `pay_workbench_enqueue_candidate_refresh` and established Candidate serial/currentness owners to invalidate and rebuild pre-Draft Workbench state after a committed head revision. Do not add a frontend refresh authority.
7. Preserve existing `FROZEN_SETTLED_COMPONENT`, reservation, recovery, headroom, Case/Resolution and pay-channel owners. Workbench alone compares complete current entitlement with those histories.
8. Preserve Policy X: pre-Draft may consume the current certified head; an existing Draft, scheduled payment, executing payment or settled artifact continues to use its frozen evidence and is never rewritten by a later head.
9. The direct ordinary TSFIN projection in `weekly_source_ordinary_pay_projection_v1` must not be used to manufacture an A-style correction for the locked-root transition. Any still-valid unlocked/root-creation use remains separately governed and must be proved disjoint from `LOCKED_FINAL_SOURCE` selection.
10. No public Banking action or row is added. The only existing financial-decision change remains the narrow, reviewed `LIVE_ENTITLEMENT_INPUT` predicate described by the accepted C1 compatibility specification.

## Invoice isolation

`weekly_source_billing_movements` and the existing invoice placement/rendering owners remain the only Weekly Source invoice authority. The `-6` and `+7` movements retain their immutable identities and original `invoice_timesheet_id` relationship.

The locked final-source head and Workbench must not insert, update, delete, move, relabel, aggregate or replay those movements. Candidate-pay publication, Workbench refresh, residual calculation, recovery and Draft state cannot create or suppress an invoice line. Conversely, invoice placement or issuing cannot alter the complete Candidate entitlement head.

## Transition meanings

| Source transition | Invoice side | Candidate-pay side |
|---|---|---|
| Present to changed | Existing immutable source reversal/replacement movements | Replace current complete-entitlement head with the new complete entitlement |
| Present to absent | Existing immutable source cancellation/reversal movement | Publish certified complete zero/absence under the same root |
| Absent to present | Existing immutable positive source movement | Publish the new complete positive entitlement under the same root |
| Repeated cycles | Append immutable source movements; never rewrite history | Replace one fixed-size current head; never scan lifetime movements to calculate the target |

## Unchanged owners

- `weekly_source_billing_movements`, invoice placement, invoice issue and invoice rendering.
- Existing ordinary correction-chain and correction-TSFIN owners outside this Plan6 path.
- Existing Workbench dependency closure and its caller.
- Existing Workbench residual, settled-history, reservation, recovery, headroom, Case/Resolution and pay-channel owners.
- R01 publication and R03 projection ownership.
- Payment, provider, settlement, remittance, cancellation and recovery owners.
- Protected C1: its invoice movements remain independent; it publishes complete entitlement through the same approved seam; Workbench compares that target with settlement history.
- No source transition may infer that an unknown provider outcome is unpaid.

## Replay, currentness and boundedness

- The head has one current fixed-size row/state per ordinary root plus immutable publication receipts/audit evidence under the existing bounded retention contract.
- Identical publication replay returns the committed receipt and does not change the head revision, create a pay row, duplicate an invoice movement or enqueue duplicate financial effects.
- The same request identity with a different digest refuses before mutation.
- A lost response uses status/receipt recovery; it does not blindly repeat publication.
- A newer source revision atomically supersedes the prior current head under the established root/source lock. It never deletes invoice, settlement or audit history.
- Pre-Draft Workbench state carrying an older head revision/digest is stale and must rebuild through the existing refresh owner.
- Post-Draft frozen artifacts remain unchanged.
- Repeated source cycles must remain constant-size for active head lookup; no active Workbench read may traverse the lifetime invoice-movement or correction history.

## Release-blocking conditions

Implementation and release remain blocked until all of the following are frozen and independently verified:

1. Exact private head relation/interface, column types, authority-kind constraint, certified-zero/absence representation, component schema, revision and digest preimages.
2. Exact single writer, lock order, source-currentness checks, publication receipt, identical/conflicting replay and unknown-outcome recovery.
3. Proof that one root has exactly one current head and that no Timesheet/TSFIN correction carrier is created for `LOCKED_FINAL_SOURCE`.
4. Exact narrow predicate and occurrence mapping in `private.pay_workbench_unit_economic_occurrence_page_v1(...)`, including null/zero/type preservation and no fallback that double-counts current TSFIN plus the head.
5. Proof that `private.pay_workbench_timesheet_dependency_closure_v2(...)` and its caller remain unchanged and that every seed resolves to the same ordinary-root unit.
6. Exact Candidate refresh/currentness binding, including stale build, stale preview, stale resolution, concurrent publication and lost-response behavior.
7. Constant-size active lookup and bounded page/byte/work limits for complete component projection; no lifetime movement scan and no full-population variable object before admission.
8. Invoice differential proof that movement IDs, amounts, VAT, grouping and rendered output are identical whether Candidate pay is protected, waiting, published, reconciled or zero, and that Workbench creates no invoice movement.
9. Joined PostgreSQL 17 and 18 acceptance for HealthRoster and NHSP covering present-to-changed, present-to-absent, absent-to-present, repeated cycles, certified zero, settled/part-settled/reserved history, overpayment recovery, unknown provider outcome, concurrent publication and exact replay.
10. Joined Banking acceptance for the accepted C1 outcomes: complete `120` against settled `100` produces residual `20`; settled `120` against current `100` produces the existing recovery `20`; equal target/history produces no row; reappearance produces no duplicate pay.
11. Policy X tests proving active Draft, scheduled, executing, settled, cancelled and recreated states retain frozen evidence and are not rewritten by a later head.
12. Security, ownership, ACL, contract-diff, clean PostgreSQL 17 rebuild, protected TEST runtime and PostgREST schema-cache proof required by the repository release process.

## Final disposition

**B is the compatible and minimal representation under the corrected invoice/pay separation. A is not approved for Plan6 locked ordinary pay.**

This verdict authorises design continuation only. It does not authorise implementation, database installation, deployment, TEST or LIVE mutation, payment/provider action or release.
