# NHSP/HealthRoster Weekly Authoritative Amendments

## Independent-verification handover

Date: 31/07/2026  
Environment: TEST only  
Implementation status: committed, pushed, installed and deployed

## Required verifier response

Independently verify this implementation against the locked **NHSP/HealthRoster Weekly Authoritative Amendments Plan**. This is a tightly scoped import-authoritative correction change, not an invitation to redesign Import Review, invoicing, TSFIN, Banking Pay, payment, settlement or remittance.

Return:

1. an overall verdict of `FULLY IMPLEMENTED` or `FURTHER WORK REQUIRED`;
2. evidence for every finding;
3. a function-by-function implementation plan containing **only** functions or files that still require work to fulfil this brief;
4. exact tests for each remaining item.

Do not propose a broad rewrite. Do not add Banking Pay work. Do not change payment calculations, Policy X, draft creation, CSV settlement, provider execution, remittances, invoice totals or HealthRoster Daily behaviour. If nothing remains, say so explicitly and do not manufacture follow-up work.

## Outcome implemented

Authoritative changed-hours imports now reconcile the exact source shift from signed, issued invoice evidence rather than assuming that a historical reversal/replacement pair is complete or unchanged.

For each NHSP Weekly or import-authoritative HealthRoster Weekly source shift:

```text
B = complete signed effective issued invoice position
M = current active, non-archived, wholly uninvoiced mutable position
A = newly imported authoritative position
```

The route is now selected from current evidence:

- `AMEND_SOURCE`: an ordinary active, uninvoiced source timesheet is amended in place.
- `AMEND_PAID_UNINVOICED_SOURCE`: the existing paid-but-uninvoiced rollover route is used; payment is not treated as invoice evidence.
- `AMEND_EXISTING_REPLACEMENT`: the current wholly uninvoiced correction generation is repaired or amended in place under the same correction identity.
- `CREATE_REVERSAL_REPLACEMENT`: when effective issued evidence exists, a new generation is created where the new reversal is exactly `-B` and the corrected-hours positive is exactly `A`.

This permits repeated corrections. Each fully invoiced generation becomes part of `B`, so the next generation reverses the actual complete signed position, not blindly the original timesheet or an assumed latest positive.

Missing, deleted, edited and archived historical pair members are evidence, not assumptions. The calculation uses the complete signed invoice ledger. Archived rows contribute neither to `B` nor `M` and are never restored or reused. A partial current invoice generation, an active draft/invoice transition, an unprovable source allocation, a non-standard balance or stale evidence blocks safely with a specific Import Review reason.

HealthRoster Weekly validation-only, HealthRoster Daily and ordinary non-authoritative imports remain outside this correction-generation route.

## Implemented function-by-function plan

### 1. `public._import_review_effective_invoice_balance_core_v1`

New internal read-only reconciliation authority. It resolves exact source identity, collects bounded signed issued invoice/credit evidence, calculates `B`, resolves mutable `M`, excludes archived rows, reports deleted/missing historical identifiers, detects partial/pending/draft conflicts, calculates standard representability, and returns deterministic source/invoice/unit fingerprints. It processes source arrays up to 100 per call; this is an internal evidence-window bound only.

### 2. `public._import_review_action_catalog_core_v1`

Preserves existing mapping, Daily, validation, email, query, cancellation and reference decisions. It calls the new helper in deterministic groups of at most 100 source identities and processes every group, so there is no 100-shift import limit. It places safe blockers in the review, exposes frozen reconciliation evidence, and chooses one of the four authoritative routes above.

### 3. `public._import_review_apply_envelope_core_v1`

Freezes the approved `B`, `M`, `A`, source scope, invoice evidence, member IDs, schedules and fingerprints into the immutable apply envelope.

### 4. `public.import_review_apply_guard_v1`

Locks and re-attests the exact reviewed source unit. It rejects stale source, invoice, correction or policy evidence before a source mutation can proceed and places the approved units in transaction-local capability state.

### 5. `public.import_review_correction_generation_transition_v1`

New service-role lifecycle coordinator with `PREPARE`, `VALIDATE` and `AUTHORISE` actions. It consumes only the operation-bound unit, repairs or prepares the exact current members, delegates ordinary lifecycle mutations to the existing bulk functions through a transaction-local capability, validates exact balance/member postconditions, and authorises only after the post-TSFIN position still matches the frozen unit.

### 6. `public.timesheet_unauthorise_bulk_atomic`

Adds only the narrowly scoped transaction-capability branch needed for an approved import reconciliation unit. Existing eligibility, trigger, audit, mutation and cache behaviour remains intact. Arbitrary import-correction bypass remains impossible.

### 7. `public.timesheet_authorise_bulk_atomic`

Adds the matching transaction-capability branch for exact post-TSFIN reauthorisation. It accepts only the expected operation-bound timesheet set and retains all normal authorisation protections.

### 8. `public.hr_weekly_apply_transactional`

Routes only import-authoritative HealthRoster Weekly changed-hours actions through the reviewed reconciliation unit, invokes the exact lifecycle phase, and proves the final current pair/member state. Validation-only Weekly remains on its existing route.

### 9. `public.nhsp_weekly_apply_transactional`

Implements the same source-neutral authoritative routing and postconditions for NHSP Weekly.

### 10. `public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

Creates or repairs the standard HealthRoster reversal/corrected-hours records from frozen `B` and `A`; missing active roles can be recreated without reviving archived history. It preserves source-specific evidence and parent/correction identity. The four-argument overload is unchanged.

### 11. `public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

Implements the equivalent NHSP generation/repair behaviour from the same frozen reconciliation rules.

### 12. `public.import_review_actions_page_v1`

Returns the new route, evidence and friendly blocker detail through the existing paged review contract without changing page-size policy.

### 13. `public._import_review_apply_complete_core_v1`

Validates that every selected reconciliation action has reached its required committed/follow-up state before finalising the operation.

### 14. `public.bulk_timesheet_row_patch_v1`

Classifies only exact current import-correction members with source-specific labels. An exact NHSP reversal/replacement is shown as `NHSP Reversal` / `NHSP Corrected Hours`; the HealthRoster equivalents use `HealthRoster` labels. Ordinary amended source timesheets retain their normal NHSP or HealthRoster label.

### 15. `public.bulk_authorise_dataset_v1`

Places exact NHSP correction members in the NHSP section and exact HealthRoster correction members in the HealthRoster section, not the main Timesheet section. Conflicting source evidence fails closed.

### 16. `public.invoice_correction_pair_scope_v1`

Adds the single approved compatibility exception. It accepts only an exact, current, non-archived two-member pair belonging to a completed authoritative Import Review operation whose current pair still reconciles to the committed `B/A` contract. It does not change invoice generation, issue, credit, totals, VAT or document functions.

## Worker and frontend

- `broker/src/import-review-follow-up.js` now performs operation-bound `VALIDATE → AUTHORISE → VALIDATE` for authoritative correction units after TSFIN, with retryable safe failure and no source reapply.
- `js/import-review-v1.js` presents precise route outcomes and friendly blocker explanations through the normal CloudTMS confirmation modal.
- `js/main.js` consumes the server-owned correction source/kind/label fields for Bulk Authorise classification. It does not infer a correction label from a generic adjustment.

## Schema and source files

Backend implementation commit:

```text
97759051348777dd192e2c5678ee19ac2b55b4aa
branch: test
remote: github.com/kierarthur/cloudtms-backend
```

Frontend implementation commit:

```text
187c2305af4c6ce26d5e10eae83d36f59e85c766
branch: main
remote: github.com/kierarthur/TEST-Frontend
```

Database source:

- one new migration: `supabase/migrations/31072026_1534_import_correction_audit_source_index.sql`;
- existing canonical repeatables were replaced in place;
- no second/latest duplicate implementation file was added;
- rollback SQL is outside deployable repeatables at `codex_outputs/nhsp-healthroster-weekly-authoritative-amendments/rollback/31072026_1712_restore_pre_reconciliation_functions.sql`.

The rollback restores the fourteen pre-change function bodies from backend Git HEAD `7f951085980b7cdd42c767b43cdf788120b4e991`, reasserts their owners/grants, drops the two new functions and drops the new index. It deliberately does not reverse already committed import/timesheet data.

## TEST database installation

Target project: `test-cloudtms` (`yakevhtttcsljosbdpov`). Production was not accessed.

Applied TEST migrations/installations:

```text
nhsp_hr_weekly_authoritative_amendments_20260731
nhsp_hr_weekly_authoritative_catalog_json_runtime_fix_20260731
nhsp_hr_weekly_authoritative_catalog_null_mapping_fix_20260731
nhsp_hr_weekly_authoritative_catalog_guarded_policy_fix_20260731
nhsp_hr_weekly_authoritative_catalog_policy_case_fix_20260731
nhsp_hr_weekly_authoritative_audit_object_scope_fix_20260731
restore_current_import_review_contract_after_reconciliation_20260731
```

The small follow-up installations corrected runtime issues found by live catalog execution and are already folded into the single canonical local repeatable bodies. They fixed:

1. PostgreSQL's argument limit in one large `jsonb_build_object` by composing two JSON objects;
2. null client/contract mapping being sent to the existing auto-authorise resolver;
3. guarded policy resolution order/case handling;
4. non-timesheet audit object IDs being interpreted as missing timesheet IDs.

The final contract restoration did not introduce new source or Banking Pay behaviour. Redeploying the current Worker exposed that TEST still held an older Import Review contract wrapper. The existing current wrapper from `25072026_1615_banking_pay_canonical_correction_carrier.sql` was restored so the Import Review route could prove prerequisites that were already installed. No `pay_*` function was changed.

Final database proof:

```text
expected exact implementation signatures: 16
installed exact implementation signatures: 16
all exact signatures present: true
audit lookup index present: true
canonical correction-carrier contract ready: true
targeted family-materialisation contract ready: true
owners: postgres
new internal helper executable by anon/authenticated/service_role: false
new transition executable by service_role only: true
```

## Live TEST evidence

Read-only/live catalog proof was run against both source routes:

- HealthRoster Weekly review `735e56…`, preview generation 13: catalog completed with 10 advisory and 1 no-action result.
- NHSP Weekly review `69225…`, preview generation 6: catalog completed with 21 advisory and 5 no-action results.
- A read-only helper check for the previously discussed James source returned `B=0`, `M=-2`, no historical missing IDs, and the exact safe blocker `ZERO_EFFECTIVE_POSITION_HAS_ACTIVE_CORRECTION_GENERATION`. That existing James issue was not reopened or mutated.

No invoice, timesheet, payment, draft, CSV, provider, settlement, remittance or email operation was executed as part of this implementation verification.

## Deployments

Normal TEST Worker:

```text
worker: test-cloudtms-backend
version: 92dbca8d-b60e-43c2-aac2-f892e1f94cf8
source commit: 97759051348777dd192e2c5678ee19ac2b55b4aa
deployment: successful
```

A fresh Wrangler Tail session was opened after that deployment. A harmless authenticated/preflight Import Review request returned HTTP 204 and Tail captured the `/api/import-reviews` request with `outcome: ok`. The stream also observed an unrelated scheduled Banking Pay cron already configured on the shared Worker; it was not invoked by this work and no Banking Pay operation was performed.

TEST frontend:

```text
host: https://testmode.arthur-rai.co.uk
deployment source: GitHub Pages main branch
Pages build commit: 187c2305af4c6ce26d5e10eae83d36f59e85c766
Pages status: built
```

## Verification results

Passing automated checks:

```text
backend broker/import suite:             234 / 234
focused reconciliation/follow-up suite:   61 / 61
frontend static/unit suite:               347 / 347
patched-asset Playwright workflow:           2 / 2
deployed TEST Playwright smoke:               2 / 2
```

Browser verification proved:

- authenticated TEST login;
- normal TEST origin and backend;
- exact local patched assets during pre-deploy verification;
- exact served `import-review-v1.js` SHA-256 after deployment;
- live Worker/DB contract success;
- live HealthRoster eligible-client request with `no-store`;
- Import Review home rendered;
- refresh rendered;
- desktop and 412 px narrow modal layout remained within the viewport.

One pre-deploy browser run initially failed because the existing test fixture omitted an already-required canonical contract marker. The fixture was corrected and the same workflow then passed. The first deployed smoke run correctly found the stale TEST contract wrapper described above; after restoring the repository's existing current wrapper, it passed.

The broad legacy backend suite was also inspected. Three Banking Pay-only static assertions fail on the working tree and reproduce on untouched backend Git HEAD, so they pre-date this implementation. They were not amended because doing so would breach the strict import-only boundary. Relevant backend/import tests are all green.

## Scope and safety confirmation

```text
TEST Supabase used: yes
Normal TEST Worker deployed: yes, expressly authorised
TEST frontend deployed: yes, expressly authorised
Production accessed or deployed: no
Payment/provider execution: no
Banking Pay functional operation: no
Banking Pay function/source change: no
Invoice function changed: only invoice_correction_pair_scope_v1, as expressly approved
HealthRoster Daily function changed: no
HealthRoster validation-only financial behaviour changed: no
Policy X drift: none
Secrets printed or committed: no
Raw Tail/database/browser artifacts committed: no
Unrelated dirty user file preserved: yes
```

The preserved unrelated backend change is:

```text
codex_outputs/banking-pay-pending-owner-recovery/closure-correction-and-independent-verification-handover.md
```

It was neither staged nor committed by this implementation.

## Verification boundary

The implementation has compile/static, live catalog, installed-schema, Worker Tail and browser deployment proof. Verification did not create a fresh destructive end-to-end chain of issued invoices and repeated timesheet mutations on TEST. The independent reviewer should inspect the actual function bodies and may recommend a narrowly scoped transactional fixture test if it finds a specific unproven branch. It must not use that point to expand the runtime implementation beyond the functions named in this handover.
