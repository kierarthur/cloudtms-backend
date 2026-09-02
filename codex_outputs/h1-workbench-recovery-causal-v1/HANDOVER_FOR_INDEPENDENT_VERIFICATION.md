# HANDOVER 1 — Workbench self-recovery independent verification

> **UPDATED LOCAL SOURCE / NOT COMMITTED / NOT INSTALLED / NOT DEPLOYED / NOT A FINAL RUNTIME CERTIFICATE**
>
> The user-defined endpoint is the updated functions/code plus an independent-verification handover. Installation and deployment are explicitly not the endpoint. Do not push, install, deploy, refresh Banking Pay, create a Draft, or invoke any payment/provider/settlement action while verifying this handover.

## 1. Decision the independent verifier must make

Verify whether the H1 source package correctly closes the finite, source-derived Banking Pay Workbench self-recovery defects without changing payment policy. Return two separate verdicts:

1. **H1 source/local verdict** — whether the updated H1 code and local PostgreSQL 17/18 evidence satisfy the bounded H1 recovery objective.
2. **Whole-Workbench runtime verdict** — whether a fault-free installed Workbench has been proved.

The first verdict is ready for independent review. The second must currently remain **NOT PROVED** because the H1 code is deliberately uninstalled, no post-H1 real browser acceptance exists, H2-owned Draft/certificate work remains open, F8 is only H2 `FIXED_LOCAL_PASS` rather than installed/final, and there is no sealed certificate instance. Do not convert absence of an observed failure into a fault-free runtime verdict.

H1's own evidence-based position is:

- the frozen H1 recovery catalogue is 18/18 classes covered;
- the complete audit registry contains eight rows F1–F8; exactly seven are H1-owned corrections F1–F7, all have source/local PASS evidence, F1 was already sealed/installed at audit start, F2–F7 remain local only, and F8 is an H1-discovered but H2-owned external blocker now at H2 `FIXED_LOCAL_PASS`, not installed/final;
- 18/18 bounded mutation operators are killed, with zero survivors;
- the exact before/after payment-policy projection passes on PostgreSQL 17.11 and 18.6;
- no H1-owned local defect, TODO, skipped H1 acceptance case or mutation survivor is open;
- the full operational “fault-free Workbench” assertion is not yet available and must not be inferred from this source handover.

### Exact F1–F8 count and ownership model

| ID | Finding | Discovery owner | Correction owner | Current status | Counting bucket |
| --- | --- | --- | --- | --- | --- |
| F1 | Signed-recovery classifier ordering | Banking Pay parent sealed incident; accepted and re-proved by H1 | H1 | `INSTALLED_PASS`; final combined re-audit open | H1-owned correction; pre-existing at audit start |
| F2 | Deterministic error classification | H1 audit | H1 | Local PASS; not installed; final combined re-audit open | H1-owned correction; new this iteration |
| F3 | Valid Candidate terminal-state vocabulary | H1 audit | H1 | Local PASS; not installed; final combined re-audit open | H1-owned correction; new this iteration |
| F4 | Earliest first-cause retention | H1 audit | H1 | Local PASS; not installed; final combined re-audit open | H1-owned correction; new this iteration |
| F5 | Unchanged deterministic successor fence | H1 audit | H1 | Local PASS; not installed; final combined re-audit open | H1-owned correction; new this iteration |
| F6 | PostgreSQL 17/18 release-exporter portability | H1 audit | H1 | Local dual-engine PASS; not installed; final combined re-audit open | H1-owned correction; new this iteration |
| F7 | Final repeatable closure ordering | H1 audit | H1 | Local dual-engine PASS; not installed; final combined re-audit open | H1-owned correction; new this iteration |
| F8 | Release-verifier database-name portability | H1 audit | H2 | H2 `FIXED_LOCAL_PASS`; PG17/PG18 canonical NEW, alternate-name NEW-attested and managed-identity UPGRADE 3/3 each; 22/22 fail-closed contexts and 2/2 collision negatives; not installed/final | External H2-owned blocker discovered by H1 |

The authoritative counts are therefore: eight H1 audit issue rows; one pre-existing sealed row registered at audit start (F1); seven findings newly discovered by H1 in the current iteration (F2–F8); seven H1-owned corrections (F1–F7), of which one is installed and six are local-only PASS; and one external H2-owned blocker (F8). Seven issue rows remain open for final closure (F2–F8), and zero have a final combined re-audit PASS.

The earlier dashboard value `H1 total 7` counted H1-owned corrections F1–F7. The later value `F1–F8 total 8` correctly counted the complete audit registry, but its phrase `new H1 findings this iteration 8` was wrong: F1 was pre-existing, while F8 was new but H2-owned for correction. No future dashboard may interchange audit-row count, discovery count or correction-owner count.

## 2. Mandatory operating boundaries

- Read the applicable workspace and repository `AGENTS.md` files and the complete `TEST-Frontend/BANKING_PAY_BIBLE.md` before reviewing or editing.
- Current runtime truth is Miget `agency_test` / database `cloudtms_test_clone`. Never use either former Supabase project as current truth and never access `agency_live`.
- Any database inspection must be bounded and read-only. Do not print credentials, sensitive rows or secret-file contents.
- Do not run DDL/DML on shared Miget TEST, drain queues, create a Draft, call a provider, submit a payment, settle, remit, send communications or mutate application data.
- Preserve Policy X and every existing economic, selection, Draft-finance, approval, provider and settlement owner.
- Treat H2-owned findings as external dependencies. Do not “fix” them inside H1 or mark them H1 PASS.
- The local PostgreSQL containers named below are task-owned disposable proof infrastructure. Do not touch another task's Docker resources.

## 3. Exact source locations and identities

### H1 backend worktree

- Path: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\h1-workbench-recovery-backend`
- Branch: `codex/h1-workbench-recovery-causal-v1`
- HEAD/base commit: `f5f09b140f4ac9188762e36baf9d2d14bd4c17a7`
- Base tree: `a495cc57d1a53ecf3c6778c1971198aa26ea83a0`
- State: intentionally dirty and uncommitted
- Backend tracked binary-diff SHA-256: `018dc25084c058d6318465f5499da9b0cf9e89d164a2b53fb4da9db59e56986f`

### H1 frontend/Bible worktree

- Path: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\h1-workbench-recovery-frontend`
- Branch: `codex/h1-workbench-recovery-bible-v1`
- HEAD/base commit: `e58e567f66ed8108a40e3c3e8388dbe33e0b0361`
- State: intentionally dirty and uncommitted; only the append-only Bible entry changes
- Frontend/Bible tracked binary-diff SHA-256: `9487f3cbce6c252bffa38360750420f45abeafae31329802e35e50877b4cb193`

### Current shared and installed identities

- Current shared backend/test source: `14dd89afb1944187508a562b40631957512428eb`
- Current shared tree: `47d2f96cf41897d0344184d3b13d0eb78a5e1dc5`
- Direct parent: `7811288c04fd0fc1427ab9d98e4f844e1c66079e`
- The ancestry from H1 base f5 through database-installed `12ecbb56e25a890f2ac7f4af0d65406109622bea`, 87411c, 781 and exact successor 14dd is proved.
- The direct 781-to-14dd delta is exactly `broker/src/candidate-app-backend.js` and `tests/candidate-app-backend.test.js`. The cumulative 12ec-to-14dd delta remains exactly those two paths plus `tests/candidate-app-finalisation.test.js`. Both comparisons have zero SQL, contract, Banking, finance, Draft or H1-held-path overlap.
- Current installed database authority remains 12ec through Miget UPGRADE `33550417397`; the later 781 and 14dd successors are JS/tests-only and must not be described as installed database releases.
- Current repository/installed contract SHA-256 at the 12ec database authority: `5c94b3f8a644e10095f5446f7782f429337aed9b92fe521b7872c2ffc77a0cd6`.

The Candidate publication hold is released. Parent-sealed evidence for 14dd records full backend 1,112/1,112, focused Candidate 162/162, CI green and ordered Workers healthy. H1 independently proves the Git ancestry, exact two-file direct delta, cumulative three-file Candidate-only delta and H1 byte non-overlap; it does not relabel the parent-sealed runtime results as H1 reruns.

Review `SHARED_BASELINE_RECONCILIATION_14dd89af.json`, `ROLLBACK_SOURCE_MANIFEST_14dd89af.json` and `MIGET_INSTALLED_IDENTITY_SNAPSHOT_12ecbb56.json` rather than trusting this summary.

## 4. Proved causal defects and exact corrections

### F1 — signed-recovery classifier ordering

This defect was corrected and installed before H1 began. Current source and installed Miget filter the complete signed pre-signature before strict cardinality. Ordinary same-economic-key components do not count as signed evidence. H1 preserves this fix and its fixtures; it is not pending code.

### F2/F3 — deterministic failure classification and valid Candidate state

File: `supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql`

- Adds only the established signed-evidence integrity code to the deterministic source-stage class.
- Terminalises that unchanged deterministic cause without backoff.
- Uses the existing valid Candidate terminal state `FAILED`; it does not introduce or retain the invalid `ERROR` literal.
- Preserves the existing top-level public `code` while adding the causal envelope described below.
- Does not change `max_attempts`, selection, amounts, Draft state ownership or payment policy.

### F4 — earliest cause survives lease expiry/exhaustion

Files:

- `supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql`
- `supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql`

The existing attempt ledger remains the authority. The additive envelope is `WORKBENCH_FIRST_DIVERGENT_CAUSE_V1` with:

- `first_divergent_cause`
- `first_divergent_attempt_number`
- `latest_observed_failure`
- `latest_attempt_number`

A later lease/cancellation-grace/exhaustion label remains visible as the latest operational observation but cannot replace the earliest deterministic cause.

### F5 — finite unchanged-cause successor fence

File: `supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql`

The accepted narrow Option A is implemented:

- successful current build reconciliation remains first;
- valid active-successor rebinding remains second;
- the deterministic failed-close branch requires the exact locked current session/Candidate/version/job/run/build/stage-attempt pointer and an approved source-stage code;
- current live source authority is re-proved under the existing locks;
- unchanged deterministic authority creates no successor and replay is a no-op;
- a genuine source-authority change retains the established exactly-one canonical-successor path;
- terminal job and attempt history remains immutable;
- existing scope/progress owners set `SOURCE_BUILD_ERROR`, clear the pending pointer, set Candidate `FAILED`, retain safe first-cause presentation and disable Draft readiness atomically.

### F6 — release-exporter PostgreSQL 17/18 portability

File: `supabase/release/export_contract.sql`

- Excludes only ordinary local relation `pg_constraint.contype = 'n'` duplicates that PostgreSQL 18 exposes for real, non-dropped `attnotnull` columns.
- Keeps `columns[].not_null` as the portable NOT NULL authority.
- Keeps inherited/unsupported shapes visible and fail-closed.
- Preserves CHECK, PK, UNIQUE, FK, exclusion and domain-related authority.
- Contains no server-version branch and changes no database object or business policy.

### F7 — final repeatable closure ordering

File: `supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql`

- Adds immutable `08082026_0902_reassert_authorities_after_legacy_monolith.sql` as the first dependency of the existing final closure.
- Does not edit the historical 0902 file or any of the five final functional owners.
- Ensures a transitive change that makes the historical umbrella pending also makes the established final closure pending.
- Exact restored final hashes remain bulk authorise `930d55e...`, manual upsert `89543b...`, replay `363aea...`, selected rows `7d6221...` and QR enqueue `090fcbd...`.

## 5. Exact production/release files changed

The H1 source package changes these production/release authorities only:

- `supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql`
- `supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql`
- `supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql`
- `supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql`
- `supabase/release/export_contract.sql`
- `supabase/release/current-contract.json`
- `supabase/release/current-release.json`
- `supabase/verification/banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json`
- `supabase/verification/31082026_0014_banking_pay_dirty_apply_family_authority_repair_verification.sql`
- new `supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql`

There is no Worker runtime or frontend runtime change. The only frontend-repository edit is the append-only Bible/compatibility entry.

The H1-only generated contract changes exactly three existing routine `definition_sha256` values:

- fail-job: `baef72fe071ed7bb0ee3a48cc82acf33053c2a5f0bba0bb8816bccbbb34abb49`
- orphan repair: `d8c224d170144b23f0bd2f04a492a04a4e4e16fafd4664a579d18794df531e26`
- claim-start: `f49899e3f5ae7920a71019707199e4c26c6d4f21f2332b28961ce60c7f76522e`

Every signature, default, logical owner, ACL, `search_path`, volatility, parallel-safety and security-definer field is unchanged. The local f5-based contract is not a combined 14dd/H2 contract. Do not overwrite shared generated JSON wholesale; reconcile entries and regenerate from exact shared base `14dd89afb1944187508a562b40631957512428eb` if a later task creates a combined source candidate.

## 6. Payment-policy parity — mandatory verifier conclusion

H1 changes execution/recovery evidence and release mechanics only. It does **not** change:

- who or what is eligible or selected;
- candidate or pay-channel grouping/routing;
- ex-VAT, VAT, ERNI, PAYE or Umbrella economics;
- signs, economic keys, prior-paid, supersession, recovery/headroom or source-reservation treatment;
- approval, hold, resolution, snooze, provider, settlement, remittance or cancellation policy;
- Draft item/allocation/reservation economics or ultimate payment outcomes.

The rollback-contained 1927 verifier seeds PAYE, Umbrella and negative-recovery pre-Draft sentinels, then compares the exact policy projection before and after real fail/repair/enqueue/lease paths. It includes pay methods, selected constituent identities, selection/readiness, ex-VAT/VAT/inc-VAT/sign/economic keys, prior-paid/supersession/recovery/headroom/source-reservation facts, published Candidate policy fragments and Draft/provider/settlement/remittance boundary counts. PostgreSQL 17.11 and 18.6 both pass with exact equality and rollback.

The genuine-source-change branch correctly advances operational `source_change_seq`; that execution-authority field is deliberately not mislabeled as payment policy. All financial and selection facts remain in the equality oracle. Five bounded oracle-weakening mutations are killed.

## 7. External H2 Draft boundary — not H1 Workbench scope

This section records a downstream compatibility blocker only. H1 does not edit, correct, own or certify Draft materialisation. The H1 source deliverable remains Workbench recovery; H2 remains sole owner of Draft derivation, correction design and parity verdict.

- F-013b is **not a defect**. Visible Workbench repayment is `PAYMENT_ADVANCE_REPAYMENT`; hidden recovery-template/frozen Draft-item vocabulary is `LOAN_REPAYMENT`. The proposed Stage 16C equality-to-`IN` change is withdrawn and prohibited.
- Visible `MANUAL_CREDIT_ADJUSTMENT_PAYMENT` to frozen `MANUAL_CREDIT_PAYOUT` is also deliberate cross-layer translation.
- PAYE taxable components remain `GROSS_ADD/GROSS_DEDUCT` inside payroll net. Fixed/non-taxable PAYE components remain `NET_ADD/NET_DEDUCT` after imported PAYE net.
- Umbrella retains separate ex-VAT/VAT/channel/payee authority, with fixed/non-taxable components VAT-zero.
- The former `01092026_2245` all-eight INSERT_ITEMS proposal remains unsupported and excluded. The later downstream-only interface-divergence evidence does not revive that cross-layer design.
- `01092026_2250` and `01092026_2251` are prohibited and must not appear in source, contract, manifest or release lists.
- `tests/fixtures/banking-pay-h1-h2-f013-release-order-v1.json` is compatibility-only rejection/non-overwrite evidence. It is not a finance correction.
- H2's 2313 fixture proves `OVERPAYMENT_RECOVERY` correct across four supported PAYE/Umbrella tax variants only through the downstream phases it actually exercises. The fixture manually inserts Candidate scope and allocation rows and begins at `INSERT_ITEMS`; it does not run canonical preview, `VALIDATE_SESSION`/prepare, scope seed or allocation seed.
- For `MANUAL_DEBT_RECOVERY` (4 variants), `PAYMENT_ADVANCE_REPAYMENT` (2), `LOAN_PAYOUT` (2), `UNDERPAYMENT_PAYMENT` (4), and `MANUAL_CREDIT_ADJUSTMENT_PAYMENT` (4), the fixture records `PROVISIONAL_PROVED_INTERFACE_DIVERGENCE`: its manually seeded rows stop at `INSERT_ITEMS` with `MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE`, produce no Draft item or reservation, and leave case state unchanged. This is not a final defect verdict.
- Final proved defect rows are **zero**. A final verdict requires a real canonical-preview → session/prepare → scope-seed → allocation-seed → `INSERT_ITEMS` → finance → finalizer fixture. No H2 runtime correction is authorised. Existing amounts, tax, VAT, pay channel, payee, headroom, prior-paid/settled treatment and deliberate visible-to-frozen vocabulary remain authoritative.
- H2's current machine status remains 35/54 executable-bound and 25/100. The category-focused suite is 15/15, mutation operators 12/12 killed, the H2 audit subset is 74 PASS / 0 fail / 5 TODO / 1 structural skip, repository JavaScript is 1,106/1,106, `db:check` is 208 migrations / 509 repeatables, and unchanged PostgreSQL 17.11 and 18.6 owners each pass all 20 rollback-only variants with phase-correct response-loss replay. H2 explicitly supplied no stable combined-manifest signal and made no runtime function, release, Worker, Bible-authority, contract, Miget, Draft or provider change.
- The seven checksum-bound H2 evidence-only artifacts include the historically named 2313 “end-to-end” SQL/JSON/test files, but their proved scope is downstream-only because the SQL manually seeds Candidate scope/allocation rows and begins at `INSERT_ITEMS`; the remaining artifacts are the category crosswalk fixture/test, parity issue ledger and finance operating model. Their full SHA-256 identities are recorded in H1's compatibility fixture and were independently matched against H2's isolated worktree.

## 8. Machine evidence and latest results

- Focused H1 source/release/certificate/shared-baseline bundle: **92 PASS, 0 fail, 0 TODO, 0 skip**.
- Complete JavaScript suite: **1,143 PASS, 0 fail, 0 TODO, 0 skip**.
- Database source integrity: **208 migrations and 508 repeatables PASS**.
- PostgreSQL 17/18 exporter runtime test: **22 PASS, 0 fail, 0 TODO, 0 skip**, including identical portable relation contract.
- Exact 1927 first-use causal/payment-policy verifier: **PASS and ROLLBACK on PostgreSQL 17.11 and PostgreSQL 18.6**.
- Frozen recovery catalogue: **18/18 finite production-reachable classes**, 38 fixture bindings, 10/10 required fault-order schedules, 11/11 historical shapes.
- Mutation matrix: **18/18 killed**, zero survivors.
- Latest broad Banking source matrix from the earlier local phase: **1,166 pass, 3 fail, 162 database-environment skips**. The three failures were not hidden: two external owner/test blockers and one frontend-worktree binding issue whose exact rerun passed 73/73. This broad result is not a final whole-Workbench audit.
- `git diff --check`: PASS for backend and frontend/Bible worktrees.

## 9. Exact independent rerun sequence

Run from the H1 backend worktree. Do not change shared TEST.

### Source and focused suite

```powershell
git status --short
git diff --check
node --test tests/banking-pay-dirty-apply-family-authority-repair.test.cjs tests/banking-pay-pending-owner-recovery.test.cjs tests/banking-pay-stale-successor-convergence.test.js tests/banking-pay-workbench-fail-job-authority.test.js tests/banking-pay-workbench-source-build-authority.test.js tests/database-release-system.test.mjs tests/banking-pay-workbench-causal-recovery.test.js tests/banking-pay-workbench-settled-certification-contract.test.js tests/banking-pay-workbench-shared-baseline-reconciliation.test.js
npm run db:check
```

Expected: 92/92 focused PASS and database integrity 208/508 PASS.

### Complete JavaScript suite

The H1 worktree intentionally has no private dependency installation. The last run used a temporary junction to the unchanged main repository `cloudtms-backend\node_modules`, ran `npm test`, verified 1,143/1,143, then removed only that exact junction while preserving the target. A verifier may repeat that safe task-local prerequisite after first proving the target and junction identities; do not install globally or alter shared dependencies.

### PostgreSQL 17/18 exporter runtime proof

Task-owned containers currently available:

- `h1-workbench-recovery-pg17` — official `postgres:17`, PostgreSQL 17.11, local port 55417
- `h1-workbench-recovery-pg18` — official `postgres:18`, PostgreSQL 18.6, local port 55418

Both use local trust authentication and contain `banking_modal_v2_test`. Set only process-local variables:

```powershell
$env:CLOUDTMS_RELEASE_PORTABILITY_PG17_URL='postgresql://postgres@127.0.0.1:55417/banking_modal_v2_test'
$env:CLOUDTMS_RELEASE_PORTABILITY_PG18_URL='postgresql://postgres@127.0.0.1:55418/banking_modal_v2_test'
node --test tests/database-release-system.test.mjs
Remove-Item Env:CLOUDTMS_RELEASE_PORTABILITY_PG17_URL
Remove-Item Env:CLOUDTMS_RELEASE_PORTABILITY_PG18_URL
```

Expected: 22/22 PASS, including the real PostgreSQL 17/18 portable-contract comparison.

### Exact causal and policy-parity first use

```powershell
$sql = Get-Content -Raw -LiteralPath 'supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql'
$sql | docker exec -i h1-workbench-recovery-pg17 psql -X -U postgres -d banking_modal_v2_test -v ON_ERROR_STOP=1
$sql | docker exec -i h1-workbench-recovery-pg18 psql -X -U postgres -d banking_modal_v2_test -v ON_ERROR_STOP=1
```

Each engine must emit the exact PASS notice and `ROLLBACK`. Any failure, missing rollback, unexpected successor, policy-projection inequality or financial-boundary count change fails the review.

For the full clean NEW and UPGRADE/replacement replay, do not merely reuse the standing databases. Reproduce the repository release plan in task-owned PostgreSQL 17 and 18 databases and independently re-prove H2's F-012 release-engine attestation: canonical NEW, differently named NEW-attested and managed-identity UPGRADE must pass only in their exact approved contexts, while the recorded fail-closed and collision-negative contexts must remain rejected. Compare the resulting contract/security/function identities with the pack; H2's local pass is not installed/final evidence.

## 10. Documents and machine contracts to inspect

- `INDEPENDENT_AUDIT_PACK.md` — complete H1 evidence, issue ledger, identities and open gates
- `ROLLBACK_PACK.md` — non-destructive reversal design and policy-parity requirements
- `LOCAL_CANDIDATE_CHECKSUMS.pending.json` — SHA-256/byte inventory; verify every entry from disk
- `WORKBENCH_SETTLED_CERTIFICATION_V1.contract.json` — data-free H1-to-H2 certificate contract, not a populated certificate
- `tests/fixtures/banking-pay-workbench-recovery-coverage-v1.json` — frozen machine coverage/issue/external-dependency ledger
- `tests/fixtures/banking-pay-h1-h2-f013-release-order-v1.json` — compatibility-only finance-policy rejection/non-overwrite record
- `SHARED_BASELINE_RECONCILIATION_14dd89af.json` — exact current shared-source ancestry/non-overlap evidence
- `MIGET_INSTALLED_IDENTITY_SNAPSHOT_12ecbb56.json` — fresh read-only installed database baseline
- `ROLLBACK_SOURCE_MANIFEST_14dd89af.json` — exact source reversal identities
- frontend `BANKING_PAY_BIBLE.md` — append-only compatibility entry

## 11. Certificate boundary for H2

`WORKBENCH_SETTLED_CERTIFICATION_V1` is data-free and unsealed. It requires one exact session/revision/progress/publication identity; the complete ordered selected constituent universe; Candidate/pay-channel partitions; readiness/exclusion disjointness; complete paging/unloaded-selection proof; current-job/Draft-gate proof; source/install/Worker/frontend/Bible identities; exact per-constituent economic/recovery/evidence facts; exact before/after payment-policy projection equality; and one overall canonical digest.

Per constituent, H1 supplies expected **pre-Draft** facts only. It does not contain Draft-produced allocation rows, Draft item identities/amounts, Draft reservation rows, materialisation facts or a post-Draft parity verdict. H2 remains sole owner of those actual Draft derivations and comparisons. H2 consumes `certification_id` and `overall_digest_sha256` and must not reconstruct Workbench selection.

No certificate instance can be populated from this local handover because no post-H1 installed/current Workbench generation exists.

## 12. External blockers that prevent a whole-Workbench fault-free verdict

- H2 F-010 Draft selection/cardinality/compaction and downstream finalizer finance-reservation boundary
- H2 F-012 release-verifier database-name portability — H2 `FIXED_LOCAL_PASS`, still uninstalled and awaiting final combined rerun; issue-ledger SHA-256 `81f05b74091aa1873ddd6ef834644c790e1cd492cca8426c827f96650102dd2b`
- H2 sealed-rate verification-artifact reconciliation
- external `bulk_authorise_dataset_v1` source-owner split
- external Candidate named-security source-test expectation drift
- H2 F-013 complete canonical end-to-end proof for six visible finance categories; `OVERPAYMENT_RECOVERY` is proved correct only through exercised downstream phases, sixteen variants/five category rows are provisional interface divergences, zero final defect rows are proved, and no runtime correction is authorised
- H2 stable artifact manifest, final boundary suite and certificate integration
- no combined H1/H2 source tree
- no H1 installed/deployed identity
- no post-H1 real TEST/browser acceptance
- no populated settled certificate
- no fresh full combined post-correction audit

These do not reopen the locally passed H1 causal catalogue, but they do block any assertion that the entire Workbench is fault-free.

## 13. Required verifier response

Return a concise evidence table plus one of:

- `H1 SOURCE OBJECTIVE VERIFIED; WHOLE-WORKBENCH RUNTIME NOT PROVED`
- `H1 SOURCE OBJECTIVE NOT VERIFIED`

For every failure, name the exact file/routine, reproducible command or database fixture, expected versus actual evidence, policy impact, owner and whether it reopens F1–F7 or is an external H2/shared blocker. Do not propose or apply a fix until ownership and policy impact are established.

Do not return `FAULT-FREE WORKBENCH CONFIRMED` unless the reviewer has separate authority and evidence for the currently installed complete H1/H2 source, real TEST Workbench/browser acceptance and the sealed certificate. This handover intentionally does not supply those facts.

