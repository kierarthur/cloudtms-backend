# DY-10 D02 Worker source review

Independent read-only source review on11 October2026 against BE HEAD `2dac72ad5862127d99676d1c927c187d4cbf9a4a`, the full approved DY10 card/checklist and `design/DY-10-worker-design-review.md`. Root alone edits product. No SQL/database calls, tests, business fixtures, rendering, hosted calls, checklist edits or commits were performed. D03 UI and D05 joined acceptance are not this receipt.

## Current verdict and essential findings

**SOURCE CHANGES REQUIRED at the reviewed hash below.** The actual Worker receipt/duplicate changes satisfy the approved narrow design. Two truthful-state edges in the native detail/drain support remain:

1. **P2 — nullable native due timestamp rejected.** Current `broker/src/index.js:74642–74645` requires next_attempt_at to be a timestamp string. Native column `supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql:4524` is nullable; dequeue explicitly treats NULL as due (`05012026_tsfin_batch_rpcs_part1.sql:76,113`). A genuine queued Daily with NULL next_attempt_at becomes UNAVAILABLE. Accept NULL or a valid timestamp; retain malformed-value refusal.

2. **P2 — unknown pending counts reported as zero.** `index.js:172577–172578` initialise pendingTotal/pendingReady to0. If the first Worker/write receipt or pending-summary read fails, the catch at172718–172722 sets done=false but returns pending_total:0/pending_ready:0 with no confirmed pending summary. Initialise unknown counts to NULL and reset counts/next-attempt facts to NULL in that catch. Keep genuine empty-input zeros and confirmed native zeros.

An earlier concrete DTO error is already **corrected in these fresh bytes**: the reader no longer rejects attempt_count>0 with no error. Native dequeue increments attempt_count on lease before calculation (`05012026_tsfin_batch_rpcs_part1.sql:82–85,119–122`), so this must remain valid PENDING. Current74648 refuses only a nonempty error with zero attempts.

## Source that passes

The complete saved `rpcTsfinWriteSnapshotsAndComplete` was read. No-op chunk completion now requires the native integer count to equal that exact chunk length (`index.js:165604–165622`). Partial/unconfirmed clearing follows the existing normal-write fallback and resets the helper's no-op bookkeeping; it does not fabricate per-ID success.

Raw write receipt validation precedes adding no-op counts (`:165643–165665`). Singleton cardinality, finite nonnegative integer counts, count conservation against actual written rows, exact error tuple keys, distinct known primary membership and matching Timesheet IDs are checked. String scalar/JSON representations are parsed narrowly; absent/null/boolean/empty/malformed fields cannot default to successful zero. The final combined count conserves the original input. No new SQL result/protocol is introduced.

The complete changed Worker regions were read alongside the original full function and normal diff. Daily context, no-op and calculation catches preserve existing primary failure identity (`:167169–167174,167789–167808`). `markTimesheetComplete` adds the current page's original primary through effToPrimary (`:167032–167043`), and both weekly/Daily duplicate loops require positive completion in addition to the original failure exclusions (`:167833–167852`). A failure before the write batch can no longer clear its duplicates. Successful weekly native heal/no-op, native locked cleanup and genuine missing-context revocation keep their existing completion points. No calculation, Source query owner or money formula changed.

A valid mixed write receipt now checkpoints each successful input rather than withholding all successful checkpoints merely because another row failed (`:167822–167827`). A malformed/unknown helper result throws before new batch checkpoints or duplicate cleanup. The existing native outbox lease/backoff remains the retry owner; native per-row failures stay isolated.

Targeted drain retains normal defaults3 inline/60 loops/50 items and finite integer bounds (`:172518–172526`). Native pending counts reject missing/malformed values and ready>total (`:172615–172625`); the open finding concerns the separate catch-return state, not this count parser.

The current-Daily detail addition remains inside the existing admin Timesheet-details reader (`:73448–73449`) and verifies the original resolved current physical ID/version before querying the native outbox. It selects only reason,attempt_count,next_attempt_at,last_error, reads at most8 rows, validates the exact seven enum reasons and uniqueness, and exports only the three-field identity/version/status DTO (`:74629–74654,74848–74851`). The bound is supported by the original enum at baseline1500:118 and UNIQUE(timesheet_id,reason) at baseline1501:4611. No raw error or money enters this DTO. FAILED/PENDING/NONE/UNAVAILABLE remain factual processing evidence, never approved zero. D03 rendering is still outside this review.

## Scope and byte evidence

No SQL change is present in this batch. Current1001 publisher and1129 genuine Daily evidence remain unchanged; original certified-zero, WITHDRAWN, realised-excess recovery, Rule B captured inputs, E2 existing release and native locks retain their owners. This review does not certify application/runtime behavior.

Reviewed index SHA-256: `8e9ebf9edd4b3b705d9d40b9845b71c91ad99159dcd0f0159dbb1185e0b74ef4`. Normal diff contains77 additions/42 removals in15 hunks. Direct static comparison of every unchanged raw region against the HEAD blob passed, preserving exact original bytes/line terminators outside those hunks. Both HEAD/current retain32 CRLFs; the changed TSFIN range uses original LF terminators. The comparison only reads Git/disk bytes; it executes no product or test.

## Final source closure — both remaining findings corrected

Fresh saved index and its full normal diff were reread. **SOURCE PASS for the scoped D02 Worker/native-detail support; all essential findings above are superseded. No essential source defect remains in this reviewed batch.**

Current74644 accepts native next_attempt_at=NULL and otherwise requires a valid timestamp string. It still refuses malformed evidence and retains the already-corrected active-lease PENDING behavior. Current172577–172579 initialise unknown pending facts toNULL; the drain exception branch at172718–172725 resets pending total/ready/next-attempt toNULL and sets done=false. Genuine empty input and confirmed native zero remain their original truthful zero paths. No new response field, command, SQL owner or fallback zero was added.

Final index SHA-256: `1ae4b5df3c72a5659fc4454a49e7b617b541fc71d0c312ed7a78b272f5f5f2ff`. Normal diff is82 additions/44 removals. The unchanged-region raw-byte comparison against HEAD was repeated after these fixes and passed; original mixed newline bytes remain preserved outside the exact diff hunks.

This is static source closure only. D03 UI, visual acceptance and all D05 joined journeys remain separate and NOT_RUN by this reviewer.

