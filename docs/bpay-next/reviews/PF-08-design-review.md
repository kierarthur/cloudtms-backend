# PF-08 independent design review

11 October 2026. Read-only design/source review by the requested Sol6.1 Extra High reviewer. No product/checklist changes, SQL execution, database calls, tests, fixtures, browser work or commits. **Design remains OPEN: the E4/E5 boundary below must be resolved before implementation.** This is not an implementation or joined acceptance verdict.

Read the full PF08 card (`06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1830–1851`), all checklist rows (`IMPLEMENTATION_CHECKLIST.md:3309–3324`), master sequence112, BD124, the current investigation, and the native owners below. The current human-owner E4/E5 directions are the root task's explicit relay; their authority is not replaced by older card wording.

## Findings requiring a design amendment

1. **E5 cannot be avoided by making the button transaction metadata-only.** BD124 says staff Try again changes no preparation revision, but its resumed PIN/RESERVE/ALLOCATE/FINANCE_ALLOCATE STEP can subsequently change holds. The owner requires the affected execution preparation revision to advance before those changes. Keep the same native job and fixed `captured_preparation_revision`, but atomically advance only the execution revision once when accepting staff retry. Retain expected/target execution revisions in the existing immutable Retry receipt so exact replay cannot advance twice. `private.bpay_next_prepare_job_active_v1(uuid)` currently accepts original revision1 or a genuine DR05 request only (`0559:16–29`); add a finite exact association to this same-job retry receipt/current revision. Do not forge a DR05 request or obtain a new financial job/cut.

2. **E4 requires a native completion/count rule, not a promise about a later screen.** Current final Review refuses every original PREPARE job whose status is not DONE (`0559:5349–5355`); a PREPARING worker plus attention metadata leaves the Draft preparing even after the other Candidates finish. The narrow recommended financial-phase handoff is an exact worker REVIEW transition with preparing→review counters once, retained original financial phase/cursors, and a narrowly qualified final-review exception for that stopped attention job. Confirm must remain blocked and `all_ready` false. Manual same-job retry then restores PREPARING/counts, invalidates the root review revision, advances execution revision, and resumes the unchanged retained checkpoint. This is a real exhausted preparation failure, not fabricated successful capture. If the owner instead explicitly approves a separate operational bucket, BD124 must state its native counter/completion contract; the existing unconditional unfinished-job gate cannot remain unnoticed.

3. **The ceiling needs an explicit phase census.** PREPARE includes postfinancial WORKBENCH_WORK/WORKBENCH_CASE, whose worker can already be READY/REVIEW, and release/projection cleanup. `0559:697–703` proves those different states. Do not debit preparing or overwrite an existing financial outcome when a projection fails. Preserve its actual prior bucket and qualify projection attention separately. Exclude E2/manual RELEASE, expiry and discard cleanup from this ordinary ceiling so attention cannot strand mandatory release. Reusing `begin_prepare_projection_v1` wholesale is unsafe: it changes the phase to WORKBENCH_WORK after counter transitions (`0559:2373–2408`), destroying the resume checkpoint unless specifically preserved.

4. **Unknown active leases cannot become actionable Review prematurely.** A timeout does not prove rollback. Compare the exact original module/run/Candidate/job, nonce/epoch, phase and all phase-specific cursors under native owner locks. Preserve an active lease and its exact replay; only after its outcome is safely established may exhausted attention change native worker/root state. A committed advance/DONE wins. RESERVE/ALLOCATE have the native Work/component cursor pair; `cursor_key` alone is insufficient. Cleanup/retirement and successful owner completion must bypass any new blanket due/attention admission guard.

## Closed findings and sound scope

The initial all-financial-kinds ceiling is now correctly restricted to Candidate Draft PREPARE. Other financial kinds retain their owners, and WB19's definite POSITION_APPLY recovery stays unchanged. This closes the inaccessible-other-kind attention issue. The five-attempt operational ceiling and 30/60/120/240-second durable backoff are acceptable design choices; claim `attempt_count` is not consecutive failed STEP count. Same-epoch failure bookkeeping must be idempotent, validated progress resets the failure streak, and predecessor waits/lease busy/continuation-send failure do not count as financial failures.

The existing workbench Retry receipt/route, nullable Work only for exact Case-only PREPARE, bounded attention reader, actual name/captured channel, native command journal and ID-only wake are sufficient. Progress/result rows are SDP34 sorting-exempt. No new command kind, table, producer, money formula, selection proof, parked F25 resolver or automatic browser command retry is required. Any additional receipt/job execution-association fields must remain exact points, not a newest-history or JSON census lookup.

## Exact native sources and review hashes

Paths below are relative to BUILD-BE. SHA-256 records reviewed saved bytes; concurrent amendments need a fresh review.

| Source | SHA-256 |
|---|---|
| `docs/bpay-next/design/PF-08-build-design.md` | `ce74afc6a6ddae0a6fe099f481dd94d3aa22063b46ed10cd9980d059b04f0c85` |
| `docs/bpay-next/design/PF-08-current-investigation.md` | `4fd57a2fb06f045833e07037bebabb8bbb7fa62b20c7bb4d531342f567723bee8` |
| `supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql` | `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6` |
| `supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql` | `29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c` |
| `supabase/repeatable/10102026_2207_banking_pay_next_workbench_retry_enrollment_guard_v1.sql` | `13701d48b6f64780be2546597a21a11d56e5221a2a82f36ca3059e763272487f` |

Native successor parents: public financial job/intake2041; private retry2207; PREPARE claim/active/review0559; final projection0814 (plus current alias-only successor0847 where registered); public preparation page2129; due wake0560. Existing Position failure2150 and Position claim1430 retain their distinct definite-error contracts. Current registry is controlling, not a stale literal card filename.

Future five-failure/manual-retry, other-Candidate completion, duplicate/unknown delivery, held-prefix conservation, expiry/discard cleanup and actual joined Office behavior remain NOT_RUN. No source or design verdict substitutes for those acceptance clauses.

## Final amended design verdict

Fresh BD124 reviewed at SHA-256 `44c4e792a13407e3147a9febbbfc317c85f7fc087ad43982564909a61fd950237`. **DESIGN PASS.** This final verdict supersedes the initial OPEN verdict and its four findings for these amended bytes. No product implementation, runtime or joined acceptance PASS is asserted.

The amendment explicitly gives the five economic phases a real REVIEW/preparing→review handoff without claiming job DONE or discarding its retained checkpoint. Staff retry uses that same job, advances execution revision once through an exact immutable receipt association, and keeps the captured economic revision/cut/prefix fixed. The association remains sticky after successful STEP. Genuine unfinished DR05 request qualification retains its accepted target/actor/PK; a later newly accepted RELEASE receives the current execution revision normally.

The two ordinary postfinancial projection phases retain their actual worker status/count bucket/revision. Native RELEASE/PROJECT binding, expiry and discard cleanup are excluded before phase classification. Unknown active leases cannot become actionable attention: the fifth uncertain failure waits for expiry and reconciliation under native run→Candidate→job locks; no sixth attempt precedes that check, and committed progress/DONE wins. These provisions close findings1–4 above without adding a money owner, new job kind or producer.

Implementation checks remain concrete requirements of this design: economic Retry restores both worker and root PREPARING, exact counters and root review-revision invalidation atomically; its receipt points at the exact expected/target execution revision, active job and retained full phase cursor pair. Final Review's attention exception and Confirm's refusal must be exact native tuples, not a broad unfinished-job exemption. Successful progress may clear operational retry count/due/attention but must retain the execution association. The opening general statement about execution+1 should be read with the explicit economic-phase qualification below it; ordinary projection retry does not advance that revision.

The earlier investigation's OPEN amendment is superseded by this fresh design; any finite consumer census produced for implementation must use current registry parents and this exact association rather than broad economic-key rewrites. Future joined checks remain NOT_RUN.
