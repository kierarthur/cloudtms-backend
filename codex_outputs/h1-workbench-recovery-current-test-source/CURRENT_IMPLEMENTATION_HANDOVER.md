# Banking Pay Workbench self-recovery — current TEST-source implementation handover

Status: **SOURCE IMPLEMENTATION COMPLETE / REBASED TEST RELEASE CANDIDATE / NOT PUSHED / NOT INSTALLED / NOT DEPLOYED / NOT A FAULT-FREE RUNTIME VERDICT**

Date: 2 September 2026

## 1. Plain-English outcome

The Workbench recovery corrections are now present in an isolated local copy of the current TEST backend source.

They fix how the Workbench responds when a source-build job fails, retries, loses a lease, reaches exhaustion, or is repaired:

- a known permanent source error stops retrying immediately;
- the first real cause is retained even if a later lease-expiry or exhaustion event occurs;
- an unchanged permanent failure cannot create an endless chain of replacement jobs;
- a genuine later source change can still create exactly one valid replacement job;
- the Candidate state uses the schema-valid value `FAILED`;
- historical failed and `DEAD` rows remain immutable audit history and do not become current authority;
- the final release closure reasserts the established current owners after an historical umbrella replay;
- PostgreSQL 17 and 18 export the same ordinary NOT NULL authority without weakening any other constraint.

The implementation does **not** change who or what can be paid, any amount, tax, VAT, pay channel, pay method, grouping, approval, hold, resolution, provider, settlement, remittance, selection rule, readiness rule, or Draft financial owner.

It also does **not** alter the Create Draft request, response, selected-row contract, scope-seed owner, allocation owner, item owner, reservation owner, certificate interface, Worker route, or frontend runtime. It is independently reviewable and releasable as Workbench recovery source, subject to the proof gates below.

## 2. Runtime and repository terminology

Miget is the current TEST database authority:

`TEST frontend -> TEST backend -> Miget gateway/PostgREST -> cloudtms_test_clone`

Repository directories still named `supabase/` are historical source-layout names. They do not mean the implementation routes TEST to Supabase.

No former Supabase project was queried or mutated for this work. No LIVE resource was accessed.

## 3. Exact current source and installed baseline

### Local candidate

- Worktree: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\h1-workbench-recovery-d669-20260902`
- Base commit: `21548a8781549c3ef35e33e3ee526e95d2ac5b78`
- Base tree: `cef75d6e2192733285e370111dd6de785992f45e`
- Base parent: `3e4de7ac5be84f1a63d765916d0763b0cbfca6d2`
- Working state: rebased local TEST release candidate; final evidence amendment pending before push
- Candidate contract file SHA-256: `92d97b6256bb7dd6c69609240b5ea474731d755e14122f5bd1fc9bf7e41d5a87`
- Candidate contract semantic SHA-256: `ddc1b7e7b143d5cf8bda8b64d4f0376cfe1526ed8c859ee0e8d1b5175d9b53a9`
- Base contract semantic SHA-256: `0cb447aa439ffb6792e1f805aecdcab47ad386821ed3dc9da88408eb2b1ee2c8`

The generated contract semantic delta is exactly three existing routine `definition_sha256` values. Routine count, signature, defaults, owner, ACL, `search_path`, volatility, security-definer state, schemas, tables, triggers, policies, indexes and all other contract objects are unchanged.

### Fresh read-only Miget proof

- Database: `cloudtms_test_clone`
- Database user identity: `dee50tht`
- PostgreSQL: `17.11 (Debian 17.11-1.pgdg11+2)`
- Latest VERIFIED release: `20260822-test-authority-upgrade-a849a25e5391`
- Installed git identity: `a849a25e5391b893f91aa4e5ada2c9794ba9244b`
- Repository and installed contract: `41391fa0a4443b7ef18de98ce68cc1a0e6ac466fbadbc13bc03649520fbe64cc`
- H1 installed: **false**

The H1 candidate was rebased onto exact current TEST source `21548a87…`. Its repaired generated contract is complete JSON and differs from that exact base in only the three declared H1 routine definition hashes. All later Candidate source and contract history is preserved.

## 4. Exact production/release source changes

### A. `public.pay_workbench_fail_job`

Source:

`supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql`

Candidate definition SHA-256:

`baef72fe071ed7bb0ee3a48cc82acf33053c2a5f0bba0bb8816bccbbb34abb49`

Changes:

1. Classifies `PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID` as a deterministic source-stage error before retry eligibility is evaluated.
2. Persists the earliest concrete attempt error as `WORKBENCH_FIRST_DIVERGENT_CAUSE_V1`.
3. Keeps later observed failure and attempt number separately.
4. Uses the schema-valid Candidate terminal state `FAILED`.
5. Passes the evidence-preserving error envelope through build, job, scope, Candidate and audit projections.
6. Leaves transient failures on the existing bounded same-job retry route.

### B. `public.pay_workbench_repair_orphaned_pending_source_build`

Source:

`supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql`

Candidate definition SHA-256:

`d8c224d170144b23f0bd2f04a492a04a4e4e16fafd4664a579d18794df531e26`

Changes:

1. Retains the existing precedence: reconcile an already-successful current build first; otherwise rebind an already-valid active successor.
2. Requires the exact current session, Candidate, session version, source sequence, job, run, build and stage-attempt authority under the established lock order.
3. Applies deterministic fail-close only when the terminal deterministic source failure still belongs to the current source generation.
4. If source authority changed, preserves the established canonical successor path.
5. If the source authority and deterministic cause are unchanged, creates no successor, does not alter `max_attempts`, and records `SOURCE_BUILD_ERROR` with no pending owner.
6. Repeated repair or drain calls are no-ops for the unchanged cause.
7. A later genuine source change can still create one canonical current successor.
8. Retains all terminal job, build, attempt and `DEAD` history.

### C. `public.pay_workbench_source_build_attempt_claim_start_v1`

Source:

`supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql`

Candidate definition SHA-256:

`f49899e3f5ae7920a71019707199e4c26c6d4f21f2332b28961ce60c7f76522e`

Changes:

1. Keeps the existing cancellation-grace and lease-expiry classifications.
2. Preserves the earliest deterministic attempt cause when a later lease expiry or terminal exhaustion is recorded.
3. Sends the combined causal envelope to the existing repair owner.
4. Requires the terminal repair result to be one of the proved success, active-successor, deterministic-fail-close or max-attempt-fail-close outcomes.
5. Does not change claim signature, Worker call shape, lane ownership, retry count, lease budget or Candidate parallelism.

### D. Release exporter portability

Source:

`supabase/release/export_contract.sql`

The exporter excludes only PostgreSQL 18's exact ordinary relation-level `pg_constraint.contype='n'` duplicate when it is backed by one real non-dropped `attnotnull=true` column and every ordinary-shape guard matches.

`columns[].not_null` remains the enforceable authority. CHECK, primary-key, unique, foreign-key, exclusion, inherited and unsupported/malformed NOT NULL shapes remain visible. There is no PostgreSQL-version branch.

### E. Final release-closure ordering

Source:

`supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql`

The immutable historical `08082026_0902_reassert_authorities_after_legacy_monolith.sql` is added as the first dependency of the already-established final Banking Pay closure. The final closure then reasserts the same five existing current definitions. No historical owner and no five-function definition is changed.

This is release installation ordering, not payment behavior.

### F. Companion baseline reconciliations

These two corrections are included only to reconcile the two pre-existing full-CJS failures discovered during H1 closure. They do not change Workbench recovery economics, Create Draft, or final installed behavior.

1. `supabase/repeatable/14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql` now carries the exact duplicate-expense review exclusions already present in the later `29082026_0326` final closure and current installed contract. The repository generator still declares `14082026_1310` as the canonical source and reports the existing `29082026_0326` output is current byte-for-byte. This prevents a future regeneration from removing the protection; it does not add a new final function behavior.
2. `tests/banking-pay-modal-v2-contract.test.cjs` now binds the unchanged current Candidate security verifier's 124 / `390cec48151731c4346e701cf48940ae` inventory and explicitly rejects the obsolete 122 / `e82084b8b739995d086e72f1983acfb1` pair. No Candidate function, verifier, ACL or browser boundary changed.

`tests/banking-pay-legacy-monolith-authority-reassert.test.cjs` additionally guards the three duplicate-review facts while retaining byte-identity between the canonical owner and generated final closure.

### G. Verification and generated artifacts

- `supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql`
- `supabase/verification/31082026_0014_banking_pay_dirty_apply_family_authority_repair_verification.sql`
- `supabase/release/current-contract.json`
- `supabase/release/current-release.json`
- `supabase/verification/banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json`

The 1927 verifier is rollback-contained. It exercises deterministic failure, repeated repair/drain, genuine source change, retry/lease/exhaustion, first-cause preservation and a before/after payment-policy projection. It must finish with its PASS notice and `ROLLBACK`.

## 5. Policy and Create Draft parity

Every H1 issue row carries an explicit before/after policy-parity requirement. The implementation:

- changes no selected constituent;
- changes no readiness or exclusion class;
- changes no ex-VAT, VAT or inc-VAT amount;
- changes no sign or economic key;
- changes no prior-paid, supersession, recovery, headroom or reservation calculation;
- changes no PAYE or Umbrella treatment;
- changes no pay channel, pay method or payee;
- changes no Draft input, Draft output or Draft-produced fact;
- creates no Draft, batch, item, allocation or reservation in its verifier;
- changes no provider, payment, settlement or remittance action;
- preserves Policy X.

The only operational authority intentionally changed is how the pre-Draft Workbench records and recovers from a failed source-build execution.

## 6. Current test evidence

### Passed

- Pre-edit source integrity: 208 migrations / 508 repeatables.
- Post-edit source integrity: 210 migrations / 510 repeatables.
- Focused H1 suite: **92/92 PASS**, zero fail, skip or TODO.
- Current-package integrity validator: **8/8 PASS**, zero fail, skip or TODO.
- Complete JavaScript suite after rebasing and refreshing the package identities: **1,169/1,169 PASS**, zero fail, skip or TODO.
- Candidate binding registry check: PASS.
- `git diff --check`: PASS.
- Frozen recovery catalogue: 18 finite production-reachable logical classes.
- Deterministic fixture bindings: 38.
- Required fault-order schedules: 10.
- Historical shapes: 11.
- H1 mutation operators: 18/18 killed in the focused causal package.
- Fresh Miget authority/installed-definition read: PASS and read-only.

### Broader CJS matrix

Result: **854 tests; 757 pass; 0 fail; 97 conditional environment/runtime skips; 0 TODO.**

The two earlier failures now pass through the companion source/test reconciliations described above. No H1 Workbench recovery test fails, and the complete source-only CJS matrix has no failure or TODO. The 97 conditional database/runtime cases remain explicit and are not counted as executed acceptance proof.

`CURRENT_CANDIDATE_CHECKSUMS.json` binds the current handover, source inventory, test evidence, read-only Miget snapshot and rollback manifest byte-for-byte. The package validator recomputes every listed byte count and SHA-256 from disk.

### Fresh PostgreSQL 17/18 execution status

The reviewed H1 source package previously passed PostgreSQL 17.11 and 18.6 clean NEW, UPGRADE/replacement, first use, idempotent reapply, security/ACL, exact contract and rollback-contained policy parity. The H1-owned definitions and verifier in this current candidate are byte-semantically the same.

This turn could not execute a fresh dual-engine run because Docker Desktop's named pipe denied the Codex sandbox identity even while Docker was running, and the installed PostgreSQL 18 `initdb` cannot re-execute under the restricted sandbox token. This is recorded as an environment execution blocker, not converted into a PASS.

The exact `21548a87…`-based candidate must additionally pass the protected Miget TEST UPGRADE contract/security verifiers before it is treated as installed, and a fresh TEST Workbench session must then prove runtime recovery behavior.

## 7. Required independent verification

From the exact worktree:

```powershell
npm run db:check
git diff --check
node --test tests/banking-pay-dirty-apply-family-authority-repair.test.cjs tests/banking-pay-pending-owner-recovery.test.cjs tests/banking-pay-stale-successor-convergence.test.js tests/banking-pay-workbench-fail-job-authority.test.js tests/banking-pay-workbench-source-build-authority.test.js tests/database-release-system.test.mjs tests/banking-pay-workbench-causal-recovery.test.js tests/banking-pay-workbench-settled-certification-contract.test.js tests/banking-pay-workbench-shared-baseline-reconciliation.test.js tests/banking-pay-workbench-current-implementation-handover.test.js
npm test
node --test tests/*.test.cjs
```

Then, in clean task-owned PostgreSQL 17.11 and 18.6 databases:

1. run the repository NEW plan and apply;
2. export and compare the complete contract;
3. prove the candidate contract differs from the base only in the three H1 routine hashes;
4. run the 1927 verifier and require PASS plus ROLLBACK;
5. start from the exact prior definitions and run UPGRADE/replacement;
6. reapply every changed repeatable a second time;
7. verify exact signatures, defaults, logical owner mapping, ACL, `search_path`, volatility, security-definer and browser isolation;
8. reproduce unchanged deterministic failure and prove zero successors across repeated repair/drain;
9. advance source authority and prove exactly one canonical successor;
10. reproduce transient retry, obsolete generation, valid active successor, valid completed build, max-attempt exhaustion, lease expiry and response loss;
11. compare the complete policy projection before and after;
12. prove zero durable writes remain after each rollback-contained fixture.

Do not increase or bypass the established statement or lock budgets. Do not access LIVE, create a real Draft, call a provider, execute a payment or mutate shared TEST merely to complete review.

## 8. Release and rollback boundary

No commit, push, Miget release, Worker deployment or frontend deployment has occurred.

If the source is rejected before publication, restore the exact base blobs named in `CURRENT_ROLLBACK_MANIFEST.json` and omit the H1-new files. Do not use a broad reset that could discard unrelated work.

If a later installed release is rejected, publish a separately reviewed superseding repeatable containing the exact prior routine definitions and regenerated prior contract. Do not delete jobs, attempts, `DEAD` rows, release records or any business data. Historical audit evidence must remain.

There is no Worker/frontend runtime rollback because this implementation changes neither.

## 9. Independent verdict requested

The verifier should return one of:

- **APPROVE SOURCE IMPLEMENTATION FOR A CONTROLLED TEST RELEASE**
- **APPROVE WITH ENUMERATED AMENDMENTS**
- **REJECT**

Approval requires:

- exact inventory/checksum match;
- zero H1 focused failures;
- clean PG17 and PG18 NEW and UPGRADE/replacement proof;
- exact intended-only contract/security diff;
- full policy parity;
- no Create Draft dependency;
- no unexplained H1 mutation survivor or runtime branch;
- explicit treatment of the two unrelated CJS baseline failures.

Even after source approval, a “fault-free Workbench” runtime verdict requires a separately authorised installation, fresh Miget installed-identity proof, real TEST read/browser acceptance, and one complete post-correction audit. This handover deliberately does not claim those unperformed outcomes.
