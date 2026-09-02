# H1 Workbench self-recovery rollback pack

> **LOCAL CANDIDATE / NOT INSTALLED / NOT DEPLOYED / NOT FINAL**
>
> This is a rehearsed rollback design, not authority to execute it. No shared TEST or LIVE mutation, commit, push, deployment, Draft, payment or provider action is authorised.

## Starting authority

- Backend source: `f5f09b140f4ac9188762e36baf9d2d14bd4c17a7`
- Backend tree: `a495cc57d1a53ecf3c6778c1971198aa26ea83a0`
- Frontend/Bible source: `e58e567f66ed8108a40e3c3e8388dbe33e0b0361`
- H1 initial installed contract SHA-256: `d21916d6b2e07965edf50c42d3aa9a752ba2a2ff1ca9a39ab91df970fd7e70af`
- H1 isolated work began at that authority. Shared backend/test subsequently advanced through database-installed Candidate successor `12ecbb56e25a890f2ac7f4af0d65406109622bea` (tree `d673f9ff27e0dd127fbe4ff713d4c06ece86f303`) and JS/tests-only successors to exact source head `14dd89afb1944187508a562b40631957512428eb` (tree `47d2f96cf41897d0344184d3b13d0eb78a5e1dc5`, parent `7811288c04fd0fc1427ab9d98e4f844e1c66079e`).
- Fresh current shared installed database authority remains release `20260822-test-authority-upgrade-12ecbb56e25a`, VERIFIED on Miget `cloudtms_test_clone`; repository and installed contract SHA-256 both `5c94b3f8a644e10095f5446f7782f429337aed9b92fe521b7872c2ffc77a0cd6`. Exact source head 14dd is the minimum source-preservation base; 12ec remains the exact database-install identity and is not relabelled as 14dd.
- Shared TEST contains no H1 change, so there is currently no H1 remote state to roll back. Any future H1 rollback must preserve every f5-to-14dd Candidate source/test change and every 12ec Candidate session-family migration, contract, release and deployment identity.

## Finding ownership boundary

Rollback scope follows correction ownership, not the total audit-row count:

| ID | Discovery owner | Correction owner | Current status | H1 rollback unit? |
| --- | --- | --- | --- | --- |
| F1 classifier ordering | Banking Pay parent sealed incident; H1 accepted/re-proved | H1 | Installed PASS; final combined re-audit open | Existing installed authority is preserved; it is not part of the uninstalled local reversal |
| F2 deterministic classification | H1 audit | H1 | Local PASS, not installed | R1 |
| F3 Candidate state vocabulary | H1 audit | H1 | Local PASS, not installed | R1 |
| F4 first-cause retention | H1 audit | H1 | Local PASS, not installed | R1 |
| F5 deterministic successor fence | H1 audit | H1 | Local PASS, not installed | R1 |
| F6 exporter portability | H1 audit | H1 | Local dual-engine PASS, not installed | R2 |
| F7 closure ordering | H1 audit | H1 | Local dual-engine PASS, not installed | R3 |
| F8 release-verifier portability | H1 audit | H2 | H2 `FIXED_LOCAL_PASS`, not installed or final; combined rerun open | No; H2 owns correction and rollback |

The package contains eight audit rows, seven H1-owned corrections and one external H2-owned blocker. F1 was pre-existing at audit start; F2–F8 are seven findings newly discovered by H1; F8 is counted in discovery and the registry but never in an H1 rollback unit. The prior `7`/`8` discrepancy came from alternating between the H1 correction count and the complete audit-row count. Rollback decisions must never use those numbers interchangeably.

## Rollback invariants

- Never delete or rewrite job, attempt, session, release-ledger or business history.
- Never drop a relation, function signature, policy, trigger or data row.
- Use complete prior `CREATE OR REPLACE` definitions from the exact starting commit; never hand-reconstruct a function from a narrative diff.
- Preserve owners, ACLs, security-definer/invoker state, volatility, parallel safety and controlled `search_path`.
- Preserve Policy X and every economic, selection, recovery/headroom, Draft and payment owner.
- A rollback does not make a historical terminal row current authority.
- After any installed rollback, prove current Workbench ownership/progress and browser Draft gating before acceptance.

## Component rollback units

### R1 — causal Workbench functions

Restore from the exact then-current shared base. The following three source definitions are byte-identical at backend `f5f09b140f4ac9188762e36baf9d2d14bd4c17a7`, database-installed successor `12ecbb56e25a890f2ac7f4af0d65406109622bea` and current shared source `14dd89afb1944187508a562b40631957512428eb`:

- `supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql`
- `supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql`
- `supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql`

Restore their generated contract and targeted-manifest definitions to:

- fail-job `2750d5b1e12c23bd08ab0b6691b02bdae59c655d51413f3b60ae3f2792a785ad`
- repair `b71da7c4003008f60625580a167efb0b405c13c4bb1bcb8aa4dbcc2bb17007b4`
- claim-start `d00ad36f7eef644259e77e1d6a8b35ec8f14461aed3b9cce98d736b57058048e`

The new verification file and its two `current-release.json` references are removed from a withdrawn source candidate only. Preserve 12ec's `01092026_2032_candidate_federated_session_family_projection_verification.sql` entries exactly. If the H1 release had already run, its historical ledger evidence remains; a later superseding release records the prior definitions without erasing the earlier release.

### R2 — F6 exporter portability

Restore `supabase/release/export_contract.sql` from the exact base only if the exporter correction itself is proved defective. This is source/release tooling and changes no installed business function.

Important limitation: the prior exporter is known not to produce a portable PostgreSQL 17/18 relation contract because PostgreSQL 18 emits ordinary NOT NULL `contype = 'n'` rows. Therefore:

- on the current PostgreSQL 17 TEST target, exact source reversal can be verified against the prior approved contract;
- on PostgreSQL 18, exact reversal intentionally reproduces F6 and cannot be called a green portability result;
- if PostgreSQL 18 release support must remain available, retain the proven F6 filter while rolling back only the causal runtime definitions, or prepare a separately reviewed superseding exporter correction.

No rollback may hide unsupported/inherited NOT NULL shapes or weaken CHECK/PK/UNIQUE/FK/exclusion coverage.

### R3 — F7 closure ordering

Restore `supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql` and its direct verifier/test evidence from the base only if F7 itself is proved defective. Do not edit immutable `08082026_0902_reassert_authorities_after_legacy_monolith.sql` or any of the five functional owner files.

F7 changes dependency ordering only. If its include is withdrawn, the previous UPGRADE resurrection risk returns and must be recorded as an open release blocker; a successful one-off function state does not prove future upgrade ordering.

F-013b is not a rollback unit because source/history/Bible proof established deliberate staged vocabulary, not a defect. The proposed `01092026_2250`/`01092026_2251` owner/verifier pair is withdrawn and prohibited and must not exist in the final candidate, contract or manifest. F-013a's former all-eight proposal at `01092026_2245` is also excluded. H2's 2313 fixture manually seeds Candidate scope/allocation rows and starts at `INSERT_ITEMS`; it proves `OVERPAYMENT_RECOVERY` correct only through the exercised downstream phases and records sixteen variants/five category rows as `PROVISIONAL_PROVED_INTERFACE_DIVERGENCE`. Final proved defect rows are zero because canonical preview, session/prepare, scope seed and allocation seed were not exercised. No runtime correction is authorised. The compatibility guard records the provisional interface observations and deliberate visible-to-frozen vocabulary. Any future H2 correction requires complete canonical-preview through finalizer proof, parent authority, a new exact owner manifest and H2's independently reviewed reversal; H1 must preserve it during any H1-only rollback.

### R4 — documentation and evidence

The Banking Pay Bible is append-only. Do not delete the H1 entry. Append a withdrawal/rollback result with exact reason, owner, source/release identity, tests and preserved-fix statement. Evidence packs remain immutable audit artifacts and a new checksum manifest supersedes, rather than overwrites, the prior seal.

## Rehearsal evidence

The H1 local database gate has passed on task-owned PostgreSQL 17.11 and PostgreSQL 18.6 for complete NEW, applicable UPGRADE/replacement, first use, idempotent reapply, security/ACL, exact-contract and rollback-contained causal fixtures. The combined rollback rehearsal is still `OPEN` because H2 has not supplied its stable combined artifact manifest.

`ROLLBACK_SOURCE_MANIFEST_14dd89af.json` binds each prior H1/F6/F7 source unit and shared generated artifact to its exact 14dd Git blob SHA-1, blob-content SHA-256 and byte count, while separately binding the 12ec installed database identity and the append-only frontend Bible origin. Independent reversal can therefore retrieve exact repository bytes without trusting this narrative. Whole generated files must not be copied over a later shared tree; their entries must be regenerated and reconciled while preserving all f5-to-14dd non-H1 authority.

The operational rollback rehearsal for the final candidate must prove:

1. exact prior definitions are installed by complete replacements;
2. signature/default/owner/ACL/search-path/volatility/security metadata is unchanged;
3. the three prior definition hashes and prior contract return exactly where the target/version permits;
4. final closure restores its five certified current owners without partial publication;
5. the prohibited F-013b `2250/2251` artifacts and unsupported F-013a `2245` proposal remain absent; any different later H2 owner that has subsequently received category-specific policy proof and explicit authority remains the sole target authority after F7 and its canonical hash remains unchanged by an H1-only rollback;
6. security and H2 cross-boundary verifiers remain green;
7. Workbench current session ownership, progress, first-cause presentation and Draft gate are read-only rechecked;
8. no attempt/job/session/business/audit row is deleted or rewritten.
9. the exact before/after payment-policy projection remains equal: selected constituent identities, pay methods/channels, amounts, VAT/tax/sign/economic keys, prior-paid/supersession/recovery/headroom/reservation facts and Draft/provider/settlement/remittance boundary counts are unchanged by the reversal.

## Ordered rollback after a consented TEST release

1. Freeze further H1/H2 publication and record the exact failure evidence; do not hot-patch.
2. Decide the smallest rollback unit from R1/R2/R3. Do not revert an independent proven release-system correction merely because a causal function failed.
3. Build a complete superseding repeatable candidate from the exact prior H1 definitions on top of the then-current shared source (no older than 14dd), preserving every intervening non-H1 source/test/migration/repeatable/contract/release entry; reread both Bibles and exchange exact paths/hashes with H2.
4. Rehearse the exact rollback on fresh PostgreSQL 17/18 targets, subject to the explicit F6 PostgreSQL 18 limitation above.
5. Obtain explicit user authority for the exact rollback release if shared TEST has already changed.
6. Publish through the protected release route only; never edit ledgers or apply piecemeal SQL.
7. Verify installed definition/closure/contract/security identities and read-only Workbench/browser state.
8. Append the rollback evidence to the Bible and compatibility ledger. Retain all historical release/job/attempt records.

## Worker/frontend reversal

The present H1 source changes no Worker or frontend runtime asset. Their rollback steps are therefore `NO RUNTIME DELTA` at this local boundary. If H2's later combined candidate adds Worker/frontend changes, H2 must supply exact prior/current deployment identities and ordered reversal steps before the combined rollback pack can be sealed. H1 must not infer them.

## Open rollback gates

- stable H2 artifact manifest
- fresh combined-candidate source and checksum manifest
- exact combined contract/targeted-manifest semantic diff
- combined PostgreSQL 17/18 NEW/UPGRADE/rollback rerun
- H2 boundary suite after rollback
- final candidate commit/tree and clean status
- explicit user consent before any shared action

