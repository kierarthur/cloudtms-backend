# H1 Workbench self-recovery independent-audit pack

> **LOCAL CANDIDATE / NOT INSTALLED / NOT DEPLOYED / NOT FINAL**
>
> This is an unsealed H1-only review document. It is not a WORKBENCH RECOVERY PASS, is not the combined H1/H2 candidate, and grants no authority to commit, push, install, deploy, refresh Banking Pay, create a Draft, or invoke a payment/provider action.

## Status

- Ledger key: `H1-WORKBENCH-RECOVERY-CAUSAL-V1`
- Audit iteration: `1`
- Evidence score: `60/100`
- Passed gates: H1-1, H1-2, H1-3 and H1-4
- Open gates: H1-5 installed/deployed identity, H1-6 authorised real TEST/browser acceptance, H1-7 H2 boundary and sealed certificate, H1-8 fresh complete post-correction audit
- Manual release gate: active; explicit user consent for the exact sealed candidate is required before any shared action
- Combined-candidate status: `OPEN — H2 F-010 remains active; F-012 is FIXED_LOCAL_PASS but uninstalled/non-final; F-013 has one downstream-phase proved-correct control, sixteen provisional interface-divergence variants and zero final proved defect rows pending a complete canonical end-to-end fixture; F-013b is PASS_NO_CORRECTION_REQUIRED; H2 reports no stable combined artifact manifest`
- Shared publication hold: `RELEASED — exact Candidate successor 14dd89afb1944187508a562b40631957512428eb is sealed and independently source-reconciled as a two-file JS/test-only direct child of 781.`
- Installed/deployed status: `OPEN for H1 — shared TEST has advanced to a proved non-overlapping Candidate successor, but no H1 definition is installed`
- Final re-audit status: `OPEN — zero complete clean post-correction audits since the last finding`
- User-defined delivery endpoint: `updated local functions/code plus checksum-bound independent-verification handover`; installation and deployment are explicitly outside this deliverable. The original H1-5 through H1-8 operational gates remain visible so a reviewer cannot mistake local source proof for a fault-free installed Workbench.

## Exact starting identities

| Authority | Identity |
| --- | --- |
| Backend source head | `f5f09b140f4ac9188762e36baf9d2d14bd4c17a7` |
| Backend base tree | `a495cc57d1a53ecf3c6778c1971198aa26ea83a0` |
| Frontend/Bible source head | `e58e567f66ed8108a40e3c3e8388dbe33e0b0361` |
| H1 backend branch | `codex/h1-workbench-recovery-causal-v1` |
| H1 frontend/Bible branch | `codex/h1-workbench-recovery-bible-v1` |
| Current shared backend TEST source | `14dd89afb1944187508a562b40631957512428eb` |
| Current shared backend TEST tree | `47d2f96cf41897d0344184d3b13d0eb78a5e1dc5` |
| Current shared backend parent | exact single parent `7811288c04fd0fc1427ab9d98e4f844e1c66079e`; 12ec, f5, 874 and 781 ancestry proved |
| Current database-installed source | `12ecbb56e25a890f2ac7f4af0d65406109622bea` via `33550417397 SUCCESS`; 14dd is JS/tests-only after 781 and is not relabelled as a database install |
| Current shared source contract file SHA-256 | `72028e063484fc665b4febf8a2b7e2b7315d5578084ff8e5c0720522a132f1d6` |
| Current shared source contract semantic SHA-256 | `5c94b3f8a644e10095f5446f7782f429337aed9b92fe521b7872c2ffc77a0cd6` |
| Fresh current shared installed proof | Miget `cloudtms_test_clone`, PostgreSQL `17.11`, release `20260822-test-authority-upgrade-12ecbb56e25a`, VERIFIED; repository/installed contract SHA-256 both `5c94b3f8a644e10095f5446f7782f429337aed9b92fe521b7872c2ffc77a0cd6` at `2026-09-01T20:09:09.885196Z` |
| Established classifier UPGRADE | `33530075121 SUCCESS` |
| Established classifier closure SHA-256 | `81107b3bfea54b3b198c9e03fcb33e2dd0dceee2d1af83bbb3e3b8c4ef144542` |
| Established installed classifier definition SHA-256 | `06fd7386091293cb24797f5ddffc0f236d85c953058e1098e9b945c0e57b7b72` |

The local worktrees are intentionally dirty and uncommitted. A final candidate commit/tree identity and clean status are therefore `OPEN`, not inferred. The current backend tracked binary-diff SHA-256 is `018dc25084c058d6318465f5499da9b0cf9e89d164a2b53fb4da9db59e56986f`; the frontend/Bible tracked binary-diff SHA-256 is `9487f3cbce6c252bffa38360750420f45abeafae31329802e35e50877b4cb193`. These exclude untracked H1 files, which are individually bound by the pending checksum manifest.

Read-only Git proof establishes the complete ancestry from H1 base f5 through shared database authority 12ec and later Candidate successors to exact head `14dd89afb1944187508a562b40631957512428eb`. The 12ec Candidate session-family migration/repeatable/contract/release additions remain preserved. The direct 781-to-14dd delta is exactly two Candidate JS/test paths; the cumulative 12ec-to-14dd delta remains exactly three Candidate JS/test paths. Both have zero H1, SQL, contract, Banking, finance or Draft intersection, and every H1-held path is byte-identical. Any future combined candidate must start from 14dd and preserve all intervening Candidate ancestry. This is source non-overlap evidence, not permission to combine or publish.

Fresh bounded Miget evidence is stored in `MIGET_INSTALLED_IDENTITY_SNAPSHOT_12ecbb56.json`. It proves the current shared database release and complete contract identity above, `pg_show_plans=off`, and the three exact H1-owned installed canonical hashes remain the pre-H1 authorities: fail-job `2750d5...`, orphan repair `b71da7...`, and claim-start `d00ad3...`. Their physical Miget owner, security-definer state, volatility, parallel safety and controlled search paths were read directly; the provider-normalized logical owner/ACL is bound by whole-contract equality. This is current-baseline evidence only. It does not award H1-5 because the H1 definitions remain deliberately uninstalled.

## Proven incident and first cause

The real TEST incident session was `42751f42-a7e7-458f-9464-724d9deda455`. Attempts 1–7 failed deterministically in `RECONCILE_EXECUTE` with `PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID`. Attempt 8 later passed through lease/cancellation-grace handling and the job became `DELIVERED_ATTEMPT_EXHAUSTED`. The later lease label is not the root cause.

The signed-recovery classifier defect is already corrected and installed at the starting head. Its current rule filters the complete signed pre-signature (`component_fallback = WORKED_TIME_AMOUNT`, authoritative truth `0`, authoritative baseline `< 0`) before strict cardinality. Ordinary same-economic-key history is therefore not signed evidence. This established correction is F1 and is not pending H1 work.

The H1 census then proved four causal defects in the existing recovery owners:

1. The signed-evidence integrity code was absent from the deterministic failure class and was retried.
2. Terminal convergence used Candidate state `ERROR`, which the installed table constraint does not allow; the valid vocabulary is `PENDING`, `READY`, `FAILED`.
3. Lease expiry/exhaustion could replace the earliest deterministic cause in the visible envelope.
4. The orphan repair had no lineage-wide finite fence for an unchanged deterministic current-authority source-stage cause.

The accepted correction extends existing owners only. It creates no causal table, new job/retry/replacement owner, feature flag, economic key, selection owner, Draft owner or payment authority.

## H1 source inventory

### Production/release authority files

- `supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql`
- `supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql`
- `supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql`
- `supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql`
- `supabase/release/export_contract.sql`
- `supabase/release/current-contract.json`
- `supabase/release/current-release.json`
- `supabase/verification/banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json`
- `supabase/verification/31082026_0014_banking_pay_dirty_apply_family_authority_repair_verification.sql`
- `supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql`

### H1 test/evidence files

- `tests/banking-pay-workbench-fail-job-authority.test.js`
- `tests/banking-pay-workbench-source-build-authority.test.js`
- `tests/banking-pay-pending-owner-recovery.test.cjs`
- `tests/banking-pay-stale-successor-convergence.test.js`
- `tests/banking-pay-workbench-causal-recovery.test.js`
- `tests/banking-pay-workbench-settled-certification-contract.test.js`
- `tests/banking-pay-workbench-shared-baseline-reconciliation.test.js`
- `tests/fixtures/banking-pay-workbench-recovery-coverage-v1.json`
- `codex_outputs/h1-workbench-recovery-causal-v1/SHARED_BASELINE_RECONCILIATION_14dd89af.json`
- `codex_outputs/h1-workbench-recovery-causal-v1/MIGET_INSTALLED_IDENTITY_SNAPSHOT_12ecbb56.json`
- `codex_outputs/h1-workbench-recovery-causal-v1/ROLLBACK_SOURCE_MANIFEST_14dd89af.json`
- `codex_outputs/h1-workbench-recovery-causal-v1/HANDOVER_FOR_INDEPENDENT_VERIFICATION.md`
- `tests/database-release-system.test.mjs`
- `tests/banking-pay-dirty-apply-family-authority-repair.test.cjs`
- `tests/fixtures/banking-pay-h1-h2-f013-release-order-v1.json` (compatibility-only rejection/order evidence; no finance runtime owner)

### Intended routine-definition delta

| Routine | Base definition SHA-256 | Local candidate definition SHA-256 | Other contract metadata |
| --- | --- | --- | --- |
| `public.pay_workbench_fail_job(uuid,jsonb,integer)` | `2750d5b1e12c23bd08ab0b6691b02bdae59c655d51413f3b60ae3f2792a785ad` | `baef72fe071ed7bb0ee3a48cc82acf33053c2a5f0bba0bb8816bccbbb34abb49` | unchanged |
| `public.pay_workbench_repair_orphaned_pending_source_build(uuid,uuid,integer,timestamptz,text)` | `b71da7c4003008f60625580a167efb0b405c13c4bb1bcb8aa4dbcc2bb17007b4` | `d8c224d170144b23f0bd2f04a492a04a4e4e16fafd4664a579d18794df531e26` | unchanged |
| `public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid)` | `d00ad36f7eef644259e77e1d6a8b35ec8f14461aed3b9cce98d736b57058048e` | `f49899e3f5ae7920a71019707199e4c26c6d4f21f2332b28961ce60c7f76522e` | unchanged |

The H1-only generated contract semantic SHA-256 is `52dcaa52f21bb8db2d3ee313fb71da383ee4d4be24ed6e115b0e5341a439c490`; the current H1-only local contract file SHA-256 is `8334c85a761c57ac921a6e6c10353512464d409b54db1d0204143597130c9c9b`. Both are based on f5 and are not the future combined identity. The three routine entries change only `definition_sha256`; signature/defaults, owner, ACL, volatility, parallel safety, security-definer state and `search_path` remain exactly controlled by their prior contract entries. Fresh reconciliation must start from exact `14dd89afb1944187508a562b40631957512428eb`, retain all Candidate ancestry, apply the three H1 hashes, retain only H2 entries that survive its policy/owner review, export again and rerun both engines.

## Narrow correction semantics

- `fail_job` classifies only the approved signed-evidence integrity code as deterministic, terminalises it without backoff, preserves the top-level `code`, writes Candidate `FAILED`, and records the existing-attempt-ledger-derived `WORKBENCH_FIRST_DIVERGENT_CAUSE_V1` envelope.
- `claim_start_v1` records lease expiry/exhaustion as the latest observed failure while retaining the earlier deterministic first cause.
- `repair_orphaned_pending_source_build` keeps success reconciliation and valid-active-successor rebinding first. Under the existing Candidate/job locks it fails closed only for the exact current pointer, session/version/run/build/stage attempt, current source authority and approved deterministic source-stage code. An advanced source authority uses the existing canonical-successor path. Replay is a no-op and creates no successor for the unchanged cause.
- Top-level error compatibility remains; the additive causal fields are `causal_contract_version`, `first_divergent_cause`, `first_divergent_attempt_number`, `latest_observed_failure` and `latest_attempt_number`.

## Release-system findings owned by H1

### F6 — exporter portability

PostgreSQL 18 exposes ordinary column NOT NULL specifications as relation `pg_constraint` rows with `contype = 'n'`, duplicating the already-sealed `columns[].not_null` authority. The correction filters only catalog-proved ordinary local duplicates. Unsupported/inherited shapes remain visible; domain constraints and every CHECK/PK/UNIQUE/FK/exclusion constraint remain untouched. No database object changes.

### F7 — closure ordering

The immutable historical umbrella `08082026_0902_reassert_authorities_after_legacy_monolith.sql` could become pending through a transitive H1 change without making the established final closure `30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql` pending. The correction adds the immutable umbrella as the first dependency of that existing final closure, before every current-owner include and before `BEGIN`. It edits neither the umbrella nor any of the five functional owners.

Expected final hashes after replay:

- bulk authorise: `930d55e60b1599fcdba40ab7b5308ba5991a666f7a92b23f39d8c33a481af5e3`
- manual upsert: `89543b82378468b1ae43534f5a4b1a200ffc60ffbef76196398b7f7d6521792f`
- replay: `363aeab20aed70b8396793808f9a2263766e984d66914317bdf0a767e6e0f360`
- selected rows: `7d622194f7bca877bf8420cb6f10f9ad46a69bad118c5f8fb9ed16810492d98c`
- QR enqueue: `090fcbd7a66ade81f107635c360a038a514a5c26358c0b4aa716bdea91245347`

## Finite coverage and issue ledger

The executable ledger is `tests/fixtures/banking-pay-workbench-recovery-coverage-v1.json` (`WORKBENCH_RECOVERY_COVERAGE_V1`). It freezes 18/18 source-derived production-reachable classes, 38 unique fixture bindings, 10/10 required fault-order schedules, 11/11 historical shapes, 13/13 killed causal mutations and 5/5 killed payment-policy-parity oracle mutations: 18/18 total, zero surviving. Every H1 issue carries explicit before-policy, runtime divergence, after-runtime, evidence and empty forbidden-delta fields. It contains no future-hardening rows and records zero clean post-correction audits so far.

| Finding | Discovery owner | Correction owner | Proof | Current state | Count bucket |
| --- | --- | --- | --- | --- | --- |
| F1 classifier cardinality ordering | Banking Pay parent sealed incident; H1 accepted/re-proved | H1 | source + installed TEST | `INSTALLED_PASS`, final combined re-audit open | H1-owned; pre-existing at audit start |
| F2 deterministic classification | H1 audit | H1 | source + fixtures | local PASS, not installed, final combined re-audit open | H1-owned; new this iteration |
| F3 Candidate state vocabulary | H1 audit | H1 | source + real constraint + fixtures | local PASS, not installed, final combined re-audit open | H1-owned; new this iteration |
| F4 first-cause retention | H1 audit | H1 | incident + source + fixtures | local PASS, not installed, final combined re-audit open | H1-owned; new this iteration |
| F5 deterministic successor fence | H1 audit | H1 | source + first-use fixtures | local PASS, not installed, final combined re-audit open | H1-owned; new this iteration |
| F6 exporter portability | H1 audit | H1 | PostgreSQL 17/18 | local PASS, not installed, final combined re-audit open | H1-owned; new this iteration |
| F7 closure ordering | H1 audit | H1 | PostgreSQL 17/18 NEW/UPGRADE | local PASS, not installed, final combined re-audit open | H1-owned; new this iteration |
| F8 release-verifier database-name portability | H1 audit | H2 | source + alternate-name failures + PG17/PG18 attested local proof | H2 `FIXED_LOCAL_PASS`; not installed/final; combined rerun open | external H2-owned; discovered by H1 |

Count reconciliation: the complete H1 audit registry has eight rows F1–F8. Seven rows F1–F7 are H1-owned corrections and have local-or-installed PASS; F1 is the sole installed correction, while F2–F7 are six local-only corrections. F1 was a pre-existing sealed finding registered at audit start. Seven findings were newly discovered by H1 in this iteration: F2–F8. F8 is included in the audit registry and discovery count but excluded from the H1-owned correction count because H2 owns its correction. Open issue rows are F2–F8 (seven); final combined re-audit PASS rows are zero.

This resolves the earlier `7` versus `8` discrepancy: `7` meant H1-owned corrections; `8` meant all audit rows. The later wording `new H1 findings this iteration 8` was inaccurate because it treated pre-existing F1 as new and did not distinguish F8's external correction ownership. The machine ledger and verifier now fail if these dimensions are conflated.

## Local proof results

| Evidence | Result |
| --- | --- |
| H1 executable coverage suite | 11 PASS / 0 fail / 0 TODO / 0 skip |
| Data-free settled-certification contract guard | 7 PASS / 0 fail / 0 TODO / 0 skip |
| 14dd ancestry/path/semantic/installed-baseline/rollback-object guard | 11 PASS / 0 fail / 0 TODO / 0 skip |
| Current H1 local evidence bundle, including the handover-verdict and compatibility-only F-013 non-overwrite/rejection guards | 92 PASS / 0 fail / 0 TODO / 0 skip |
| Expanded focused/history/Draft-gate set | 94 PASS / 0 fail / 0 TODO / 0 skip |
| Complete JavaScript suite | 1,143 PASS / 0 fail / 0 TODO / 0 skip |
| Database source check | 208 migrations / 508 repeatables PASS |
| Complete Banking Pay source matrix | 1,166 pass / 3 fail / 162 database-environment skips |
| Exact frontend-bound rerun for the environment-only failure | 73 PASS / 0 fail / 0 skip |
| H1 mutation matrix | 18 killed / 0 surviving: 13 causal + 5 payment-policy-parity oracle |
| PostgreSQL 17.11 H1 gates | 8/8 PASS |
| PostgreSQL 18.6 H1 gates | 8/8 PASS |
| Exact before/after payment-policy projection | PostgreSQL 17.11 PASS / PostgreSQL 18.6 PASS; rollback contained |

The first complete JavaScript attempt exposed only missing local `xlsx`/`pdf-lib` modules. A temporary junction to the unchanged main-repository dependencies enabled the latest clean 1,143/1,143 run and was removed afterward; the shared dependency target was preserved. This prerequisite is not counted as a product finding. Local PostgreSQL sessions explicitly disabled JIT because the existing `WORKBENCH_CHUNK` budget sets `statement_timeout = 15s` and official-image JIT compilation consumed that budget; production timeout/economics were not changed.

The three broad-matrix failures are not suppressed. Two are external source/test-owner blockers; the third was the missing frontend worktree binding and has its separate 73/73 green rerun. The 162 database-environment skips are not final acceptance and must be replaced by the fresh combined PostgreSQL 17/18 run.

## External H2/shared closure rows — OPEN

- H2 F-010 Draft selection cardinality/compaction, downstream finalizer finance-reservation `LIMIT 100`, and certificate consumption
- H2 F-012 release-verifier database-name portability is H2 `FIXED_LOCAL_PASS`, not installed/final. H2 reports PG17/PG18 canonical NEW, differently named NEW-attested and managed-identity UPGRADE 3/3 each; 22/22 fail-closed contexts; 2/2 collision negatives; all eight synthetic identity families absent after rollback; focused 25/25; full JavaScript 1,106/1,106; and `db:check` 208/509. The H2 issue-ledger SHA-256 is `81f05b74091aa1873ddd6ef834644c790e1cd492cca8426c827f96650102dd2b`. These are external local results, not installed/final H1 evidence.
- H2 sealed-rate verification manifest/generator reconciliation
- external `bulk_authorise_dataset_v1` source-owner split
- external Banking source test still expecting Candidate named-security 122/`e82084...` while shared 12ec verifier seals 124/`390cec...`
- H2 F-013 category-specific execution boundary. Its earlier all-eight-alias premise remains invalidated. Source/history prove that visible `PAYMENT_ADVANCE_REPAYMENT` to frozen `LOAN_REPAYMENT` and visible `MANUAL_CREDIT_ADJUSTMENT_PAYMENT` to frozen `MANUAL_CREDIT_PAYOUT` are deliberate cross-layer translations. PAYE taxable components remain `GROSS_ADD/GROSS_DEDUCT` inside payroll net; fixed/non-taxable PAYE components remain `NET_ADD/NET_DEDUCT` after imported net; Umbrella retains separate ex-VAT/VAT/channel/payee authority with fixed/non-taxable components VAT-zero. H2's 2313 rollback fixture manually inserts Candidate scope/allocation rows and starts at `INSERT_ITEMS`; it does not exercise canonical preview, `VALIDATE_SESSION`/prepare, scope seed or allocation seed. It proves `OVERPAYMENT_RECOVERY` correct in four variants only through those exercised downstream phases. The other sixteen variants across `MANUAL_DEBT_RECOVERY`, `PAYMENT_ADVANCE_REPAYMENT`, `LOAN_PAYOUT`, `UNDERPAYMENT_PAYMENT`, and `MANUAL_CREDIT_ADJUSTMENT_PAYMENT` are `PROVISIONAL_PROVED_INTERFACE_DIVERGENCE`, not final defects: the manually seeded rows return `MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE`, create no Draft item/reservation and leave case state unchanged. Final proved defect rows are zero. Final classification requires a real canonical-preview through finalizer fixture; no runtime correction is authorised. The former `01092026_2245` proposal remains quarantined/excluded and H2 retains sole ownership.
- H1's compatibility-only machine guard `tests/fixtures/banking-pay-h1-h2-f013-release-order-v1.json` proves the F7 final closure and all 58 transitive source units do not define or replace `pay_batch_insert_items_from_preview`, requires the obsolete `01092026_2245` proposal to remain absent, and records the two deliberate vocabulary translations. It does not certify a replacement. Any future H2 correction needs a new exact path, category-specific policy proof, renewed authority, stable manifest and a new combined-candidate order proof.
- F-013b is closed as **not a defect**. Current source, installed Miget, history and Bible prove visible Workbench repayment is `PAYMENT_ADVANCE_REPAYMENT`; `LOAN_REPAYMENT` is deliberately the hidden recovery-template and frozen Draft-item vocabulary with established layer translations. The proposed Stage 16C equality-to-`IN` change and `01092026_2250`/`01092026_2251` artifacts are withdrawn and prohibited. H1 removed its uncommitted experimental F-013b guard from the final-candidate inventory and preserves the rejection in the append-only issue/Bible ledger only. The historical apply-finance owner remains byte-identical at SHA-256 `9171d175ea23a783f34c45cdbd42559062496bfa8d9daeac48dc9cb20abe4bd4`.
- H2's current evidence-only catalogue is 54 production-reachable classes, 35 executable-bound and 19 open. Its category-focused suite is 15/15, mutation operators are 12/12 killed, the H2 audit subset is 74 PASS, 0 fail, 5 deliberate TODO and one structural skip, its dependency-correct full JavaScript rerun is 1,106/1,106, and unchanged PG17.11/PG18.6 owners each pass 20/20 rollback variants. H2 score remains 25/100 and it has supplied no stable combined-manifest signal. Its seven evidence-only artifact SHA-256 identities are recorded in the H1 compatibility fixture. These results do not expand or invalidate H1's frozen 18-class recovery catalogue and do not make any H2 row H1-PASS.
- H2 combined-candidate artifact manifest and no-edit-in-flight declaration
- H2 exporter-dependent PostgreSQL 17/18 rerun against the combined tree
- H2 constituent parity and certificate-boundary suite

These are dependencies, not H1 authority. H1 must not duplicate, restate as its correction, or mark them PASS.

## Certificate boundary

The data-free draft contract is `WORKBENCH_SETTLED_CERTIFICATION_V1.contract.json`. It binds the exact current session/version/progress and per-Candidate certified publication identities to the complete ordered selected constituent universe, its Candidate/pay-channel partitions, exclusion/disjointness/readiness proof, source/install/deployment identities, exact before/after existing-payment-policy projection equality and one overall digest. H2 consumes `certification_id` and `overall_digest_sha256` and must not reconstruct Workbench selection or treat the parity field as a replacement economic owner.

No certificate instance exists yet. It may be populated and sealed only after protected installation, fresh database/read/browser acceptance and a stable combined H1/H2 source manifest. A queued response, a stale UI observation or a later success in isolation is insufficient.

## Required release order and stop gates

1. Receive H2's stable exact artifact manifest, confirmation that F-010 edits are no longer in flight, and the exact F-012 local-pass artifact inventory bound to issue-ledger SHA-256 `81f05b74091aa1873ddd6ef834644c790e1cd492cca8426c827f96650102dd2b`.
2. Exchange exact heads, paths, routines, contracts and semantic JSON deltas; use shared backend `14dd89afb1944187508a562b40631957512428eb` as the backend starting head and stop on overlap, ambiguous owner or Bible contradiction.
3. Construct one fresh isolated combined candidate from exact 14dd plus the unchanged frontend base and H2's stable manifest. Do not merge whole generated files by assumption; preserve all 12ec-to-14dd Candidate ancestry and exclude the prohibited F-013b artifacts.
4. Re-read both Bibles and update the append-only compatibility ledger for the exact candidate.
5. Rerun all focused/full/property/mutation/concurrency/response-loss suites and fresh PostgreSQL 17/18 NEW, UPGRADE/replacement, first-use, idempotency, security and exact-contract proof. Prior H1/H2 results are prerequisites, not substitutes.
6. Seal the independent-audit pack, rollback pack, exact source diff, complete checksum manifest, candidate commit/tree identity and clean status.
7. Report `READY FOR INDEPENDENT REVIEW` and stop for explicit user consent.
8. Only after consent: publish the single ordered exact-head TEST release, verify installed/database/Worker/frontend identities, then perform authorised real TEST read/browser acceptance without creating a Draft.
9. Run one fresh complete post-correction audit. Any new/reopened finding returns to diagnosis, fixture, narrow fix, full retest and Bible update in a separate sealed successor candidate.

## Contradiction and preservation statement

No H1 local change alters selection membership, candidate/pay-channel partitioning, economic keys, ex-VAT amounts, VAT, ERNI, PAYE/Umbrella treatment, prior-paid/supersession authority, recovery/headroom, reservations, Draft materialisation, Policy X, provider execution, payment, settlement, remittance or cancellation. This is now an executable before/after runtime assertion on PostgreSQL 17 and 18, not only a source-shape claim. Historical `DEAD`/`FAILED` rows remain immutable audit history and cannot become current authority. The installed signed-classifier fix and every earlier Banking Pay regression fixture remain preserved.

## Known limitations

- No local candidate commit/tree or clean status exists yet.
- The manifest does not yet include H2's stable files or H2's one sealed-rate entry.
- No H1 change is installed or deployed.
- No authorised post-H1 real TEST/browser acceptance exists.
- No sealed certificate instance exists.
- No fresh complete post-correction audit exists.
- Final checksum and rollback manifests remain unsealed until the combined candidate is stable.

