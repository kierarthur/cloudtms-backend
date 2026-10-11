# PF-08 SQL source review

11 October 2026. Independent read-only source review against amended BD124, the full card/checklist, current registry parents and `.codex-local/PF08-parents.json`. No product/checklist edits, SQL execution, database calls, tests, fixtures, browser work or commits. **Initial source review OPEN; root's Worker/screen implementation is still in progress.** No installed/runtime/joined verdict is asserted.

## Early actionable finding

**P08-S01 — PROGRESS adds an unbounded attention count** (`2259:1319–1321`). Each poll performs `COUNT(*)` across all native attention jobs for the Run. DR06 deliberately reads maintained counters. The original immutable baseline can select more Candidates than the 1000 changed-descriptor overrides; current Create inserts its actual Candidate scope and publishes that native count (`1358 guarded_create:551–571`). There is no fixed row budget on this new aggregate. PF08 requires visible attention and bounded recovery rows, not an attention total. The smallest correction is an indexed EXISTS/boolean presence plus the bounded attention page. If an exact total is required, maintain it under the existing handoff/retry/retirement owners and read the stored counter; do not recount every poll.

## Additional early findings

**P08-S02 — Retry eligibility lacks native epoch/execution qualification** (`2259:1221–1223`). The attention reader can advertise Retry for a retained old-epoch Run because it checks deadline, active job and absence of expiry/discard, but neither matches the current module epoch nor proves `prepare_job_active_v1`/the exact attention state. The public page's generic active-NEXT gate is insufficient for this particular job. Match the current owner epoch and native execution/attention tuple, or refuse the stale Run. Mutation admission already rejects this stale attempt; the reader must describe that actual eligibility truthfully.

**P08-S03 — Attention page budgets rows rather than the final reply** (`2259:1229–1234`). The 120000-byte check applies to the accumulated array; adding the `rows`/`complete` object can exceed the closed transport bound. Reserve the fixed wrapper bytes or measure the final envelope before issuing it. Preserve bounded continuation and the original last retained Candidate cursor.

**P08-S04 — Nullable Work CHECK accepts UNKNOWN** (migration `bpay_next_workbench_retry_work_ck`). `CHECK(work_id IS NOT NULL OR result->>'retry_kind'='PREPARE')` accepts NULL when Work and retry kind are both absent/null. Use an explicit true/null-safe PREPARE comparison. This is the required conditional-null contract, not a new receipt mechanism.

## Positive source evidence

The complete2207 Position retry body is retained apart from conditional nullable Work validation and dispatch to the exact PREPARE helper; ordinary nonnull Work/publication checks and atomic checkpoint repair remain. The new receipt is inserted before the execution revision update and records exact expected/target+1, captured revision, input job and worker. Successful STEP clears retry count/due/attention without clearing the sticky execution/receipt association. Whole-row receipt immutability remains inherited. Current DR05 request targets are qualified through an effective execution helper, not rewritten.

Failure bookkeeping compares nonce/owner epoch/phase plus the native Work/component cursor pair, uses native module→Run→Candidate→job locks, and deduplicates an invocation by owner epoch. Known rollback clears only the exact own lease. Unknown retains its active lease, and fifth-attempt reconciliation occurs at CLAIM before attempt6. The native handoff changes financial PREPARING→REVIEW counters while retaining the economic checkpoint; postfinancial projection keeps its original bucket. Current final Review has a narrow attention exception, and manual economic retry restores root/worker PREPARING and counters before future hold work. The remaining exact current guard/retirement census is under review.

A static function-body comparison against the declared parent files found narrow intentional deltas in the eighteen copied owners; unrelated financial calculations were not changed by those deltas. There are25 definitions:18 current-parent successors and7 new private helper/reader definitions. The shared public financial service, preparation page and due reader preserve their original security and dispatch boundaries. Registry advancement/ACL/catalogue installation and Worker/UI closure remain root work, not proof from this review.

## Saved-byte checkpoint

| File | SHA-256 |
|---|---|
| `supabase/migrations/11102026_2259_banking_pay_next_prepare_retry.sql` | `383dd587f7783662fa980871b85b597562544c12ab4a1e924325a025aca0d8f3` |
| `supabase/repeatable/11102026_2259_banking_pay_next_prepare_retry_v1.sql` | `d86406e41ab83055ead14a65a358dae20a01e79b4d24b833a26b8977ed9aea05` |
| `.codex-local/PF08-parents.json` | `aae0d352d80b2260bf45cb1f087754a334fa598d96f2d9afb3b6006c13a6adfd` |
| `docs/bpay-next/design/PF-08-build-design.md` | `44c4e792a13407e3147a9febbbfc317c85f7fc087ad43982564909a61fd950237` |

All repeated-failure, exact unknown/replay, same-job manual retry, held-prefix conservation, other-Candidate progress and expiry/discard/Remove joined outcomes remain NOT_RUN.

## Final current SQL source verdict

**SQL SOURCE PASS** for current2259 repeatable plus the2311 null-guard successor below. This final verdict supersedes the initial OPEN and closes S01–S04. It does not approve unfinished Worker/UI, A4, installation or business/joined acceptance. Root reports2259 locally installed/immutable; this review did not execute SQL or independently prove installation.

S01 is closed by a native indexed EXISTS boolean, without counting all attention jobs. S02 is closed by matching the Run's original command to current NEXT/module epoch and using `prepare_job_active_v1` for Retry eligibility. S03 now budgets the assembled rows/complete envelope. S04 is corrected in a separate migration2311 with `(result->>'retry_kind') IS NOT DISTINCT FROM 'PREPARE'`; immutable2259 is retained and no row is rewritten.

The narrowed twelve-signature execution census at `PF-08-execution-revision-seams.md` matches the actual current point qualification. The helper binds exact immutable receipt/job/Run/Candidate/module/worker/input/captured revision, preserves original DR05 accepted target, and has no latest-receipt lookup. Economic retry creates that receipt before its guarded execution+1, restores root and worker PREPARING/counters/review revision atomically, and retains all economic rows, holds and checkpoints. Ordinary projection retry keeps its status/count bucket/revision and existing sticky association. Confirm's original REVIEW/worker blockers remain unchanged.

Unknown fifth failures retain active lease authority; admission never disables the exact live STEP retry or claims attempt6 before due/expiry reconciliation. Validated owner progress clears the operational streak while retaining economic-retry identity. One failure is counted per original owner epoch; stale/advanced/DONE authority wins. Backoff/due discovery is durable and has no sleeps or native nonce adoption.

The early deferred admission helper does not block reviewed native cleanup: a newly accepted RELEASE atomically clears count/due/attention and supersedes the old retry point, while its new typed target is accepted at current+1. Expiry/discard retire affected old jobs to DONE and clear attention, so admission's first DONE bypass remains authoritative even when historical count/due is retained. Their own job kinds never enter PREPARE admission. A stale read immediately before a concurrent retirement may produce one deferred delivery; it cannot alter the committed release/retirement, and the new action owns its original wake. No additional RELEASE money/collection/Net/Case formula rewrite is required.

Final reviewed hashes:

| File | SHA-256 |
|---|---|
|2259 repeatable|`9134aad494858e038710e087a417cb232ebae3ad06f37d888e9350ec8b4a8c87`|
|2259 migration (retained)|`383dd587f7783662fa980871b85b597562544c12ab4a1e924325a025aca0d8f3`|
|2311 receipt null-guard migration|`5db2d3bb2522a1352fb45e5f25dd6d0f85c651d45c0df6179bf581dd1ce48f61`|
|current PF08 parent manifest|`cd0d252e374041f51c7c1019be320af5362b35183af61387bde44aa9c884a52e`|
|twelve-seam current-owner census|`dd3f0f983e4ca1b65d7b97380e8133362a40c885ab3bd84f7f93622473975248`|
|current final-definer registry|`bba34606fe384dc8f28d649a575cb5145905fc1521a6ca97d045f4692879084d`|

All future repeated-retry, uncertainty, native cleanup races, original DR05 completion and real actor-attribution acceptance remain NOT_RUN.

## Fresh qualification/projection amendment

**SQL SOURCE PASS retained** for2259 SHA-256 `1a57552c42d63d0dc885a4e7294472c03319537da9e913a96cbbadc10c2ab879`, superseding the preceding repeatable hash. Fresh source inspection confirms the new row reads use their exact table aliases; their selected columns, row identities, predicates and lock modes are unchanged. Existing copied baseline statements are not replaced by a broad star-read waiver.

The ordinary WORKBENCH_WORK/WORKBENCH_CASE attention handoff now also advances root `review_revision` once (`2259:56–59`) while preserving the worker's actual status/count bucket and economic/execution revision. It remains in the same locked handoff transaction; the `needs_attention` guard prevents duplicate handoff increments. This invalidates stale displayed progress without claiming financial completion or changing holds. Economic handoff keeps its original counter/revision transition. No new financial formula or release owner is added.

All25 provenance `sourceBodySha256` values were statically compared with the actual current complete definitions and matched. The current parent manifest's complete bodies also match saved2259. Existing source-gate additions enumerate the seven exact private helper/reader signatures and finite named call edges; reviewed entries retain their original callees and introduce no2259 blanket suppression or unchanged-file waiver. This review did not run the gates.

Fresh documentation/gate checkpoint:

|File|SHA-256|
|---|---|
|2259 migration (immutable, unchanged)|`383dd587f7783662fa980871b85b597562544c12ab4a1e924325a025aca0d8f3`|
|2311 strict null-guard migration (unchanged)|`5db2d3bb2522a1352fb45e5f25dd6d0f85c651d45c0df6179bf581dd1ce48f61`|
|PF08 successor provenance|`198c3935802bdb41dacd3a8f248293a649b268dbc7bf0db0ec391e087ce01c42`|
|PF08 parent manifest|`00df9207c5232a80a7194c104e6154d4daff5774ea7ac56f29a291d24063cf64`|
|source gates|`b214199008a07464d01ec176d04ee7dfb5fca4d1e4ded7f67835612f73a95462`|
|final-definer registry|`bba34606fe384dc8f28d649a575cb5145905fc1521a6ca97d045f4692879084d`|
|BD124|`44c4e792a13407e3147a9febbbfc317c85f7fc087ad43982564909a61fd950237`|

S01–S04 remain closed. SQL execution, installation verification, Worker/UI/A4 and all future joined acceptance remain outside this read-only source receipt.
