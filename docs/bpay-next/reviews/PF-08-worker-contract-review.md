# PF-08 Worker and closed API source review

11 October 2026. Independent read-only review of BD124, current 2259 SQL contracts, broker queue/job adapters, Workbench/preparation decoders and Office recovery. Product remains root-owned. No business calls, tests, fixtures, database operations, captures or commits were performed by this reviewer; joined/runtime/A4 acceptance is NOT_RUN.

**Current verdict: SOURCE PASS.** The final amendment below closes all six findings against the saved source bytes. Earlier OPEN checkpoints are retained as finding history, not the current verdict. This is a source receipt, not whole-item or application acceptance.

## Initial checkpoint — OPEN

The current queue still accepts only the native `{id,kind}` envelope and takes a fresh CLAIM followed by one bounded STEP. The new PREPARE failure evidence is assembled from that actual STEP invocation before the retryable/unknown early return. FAILURE uses the same fixed financial RPC and a separate bounded response reader. A definite durable deferred receipt is acknowledged; bookkeeping uncertainty retains native queue recovery. Review continues on exact parsed rollback classification, the new nullable-Work receipt branch, attention paging and the retained Office retry journal. This checkpoint is not an item or runtime PASS.

## Actionable findings sent to root

1. **Repeated same-job attention kept a stale controller** — Office `1657–1664` originally updated its displayed row while the Workbench controller retained the original epoch/phase/cursor closure (`js/bpay-next-workbench.js:781–783`). Root corrected this by recreating only a nonpending, nonbusy controller when its exact operands change. The current correction preserves uncertain original bytes and is source-sound.
2. **PREPARE failure fell through into the Position owner** — broker `index.js:41010–41011` originally called the Position failure service when PREPARE bookkeeping returned false. That service's native kind guard refuses it, but the extra wrong-owner call should be removed by an explicit PREPARE branch returning RETRY when no durable record exists.
3. **Attention decoder parity** — Office `718` originally checked fields but omitted broker preparation `707–708`'s exact five-failure count and finite seven-phase predicate. Mirror those attention-only predicates; ordinary WORKERS remains unchanged.
4. **Existing Review Try again uses the wrong action for PF08 attention** — Office `1580` sends WORKER_REPREPARE for every REVIEW worker, including `PF08_NEEDS_ATTENTION`. Its unfinished job intentionally fails DR05's completed-job admission. Route that exact issue through the existing qualified progress/same-job recovery or give truthful navigation instead of the incompatible action. No new preparation command is needed.
5. **A scrolling attention page can be labelled with a later revision** — Office `1648–1653` originally used mutable `facts.review_revision` after awaiting WORKERS. Concurrent PROGRESS during pagination can make the attempted reset return because the attention read is busy; the old page then receives the new revision and suppresses subsequent refresh. Capture the requested revision, reject/restart when it changes, and do not combine rows across versions.
6. **Retired attention PREPARE receipts need their exact new issue code** — SQL2259 expiry/discard bindings retain `original_review_issue_code` (`1917,2028`); their typed old-job receipts use `coalesce` after cleanup clears the current issue (`2072,2126`). This can return `PF08_NEEDS_ATTENTION`. The original broker job-step `prepareIssue` finite list excludes it, so late original STEP replay is rejected as RESPONSE_INVALID. Permit that exact code/null Work issue in the typed EXPIRED/DISCARDED receipt variants, preserving all original retirement and ordinary economic result checks.

Findings remain OPEN until the current saved corrections are independently reread. Line anchors above identify the reviewed checkpoint; later amendments record final anchors and hashes.

## Verified transport and contract evidence

- Queue `bpay-next-financial-queue.js:67–91` derives failure evidence only from an actual PREPARE STEP in the seven admitted phases. Known parsed SQL rollback is distinct from attempted transport uncertainty (`bpay-next-job-step.js:435–474`); the four transaction retry codes retain their original mapping, arbitrary SQL55000 retains the existing finite refusal mapping, and unparsed/lost outcomes remain unknown. Neither CLAIM waits nor failed continuation publication becomes PREPARE failure evidence.
- The fixed RPC admits FAILURE only for PREPARE (`bpay-next-job-rpc.js:10`), with an eight-second deadline, streamed 16384-byte ceiling and closed recorded/job/attention/due/replay response (`25–47`). The callback is awaited before ACK. Durable deferral is accepted only through the closed five-field PREPARE receipt (`job-step.js:454–459`); the queue ACKs that native handoff (`financial-queue.js:168–177`). Lost bookkeeping replies retain queue recovery. The unchanged background owner still publishes only `{id,kind}`, awaits continuation sends and independently visits READY/LEASE_EXPIRED lanes (`background.js:89–93,117–123`); no lease adoption or payment payload was added.
- Workbench server and frontend use the same eight Retry operands and the same closed PREPARE receipt extension (`broker/src/bpay-next-workbench.js:792–813`; `js/bpay-next-workbench.js:718–739`). Null Work selects the exact PREPARE branch and requires native Run/worker/execution/captured revision fields, READY and unrepaired; nonnull Work retains the original Position schema and NEW/REMOVED/DONE semantics. SQL2259 `236–330` retains original nonnull Work/publication guards and immutable receipt replay. The nullable-Work constraint's separate2311 null-safe fix is covered by the independent SQL review.
- The frontend journal remains qualified by actor/agency/module epoch/job (`js/bpay-next-workbench.js:743–755`). It persists the exact mutation body before sending, reads back that retained request first after uncertainty, and only explicitly replays the original body when no receipt exists (`777–790`). It does not substitute current row operands into a pending request or automatically run the financial STEP.
- PROGRESS adds one boolean while preserving stored counter conservation, Run identity and original expiry/discard validation (`broker/src/bpay-next-preparation.js:564–576`; Office `635–647`). Attention WORKERS retains the existing bounded Candidate seek/cursor, has exactly fourteen typed row fields and no money estimates (`preparation.js:705–739`). The finite PREPARE phases exclude mandatory RELEASE; SQL remains responsible for excluding RELEASE-associated projection work.
- Source-gate additions are exact routine signatures/callee edges (`scripts/bpay-next-source-gates.mjs:49–55,207–226`) and one exact PROGRESS constructor. They do not add a blanket SQL/dynamic/history allowance. Static JSON comparison found exactly25 current2259 registry owners matching all25 provenance signatures: eighteen complete current successors and seven new private helpers/readers. Retained predecessor lists remain explicit. This is source inspection, not a claimed execution of the gates.

## Correction checkpoint

Current saved source closes findings1,2,3 and5: Office recreates a controller only after retained bytes/busy state are clear and the operands change; index now returns RETRY directly for an unrecorded PREPARE failure; Office rejects nonfive counts/nonfinite phases; the attention read captures its requested review revision and restarts a stale read at the original bounded first page. Finding4's ordinary qualified progress path is corrected; direct Case launchers still need an existing-context fallback. Actual main PaymentAdvance `12999–13002` and CasePause `20600–20602` pass no progress namespace, and Office `2103` opens their unconfirmed Draft through Review. The smallest correction can read the already-existing validated Workbench HEADER in that exact PF08 branch, recheck current actor/NEXT, and pass its native context. Do not guess identity from PROGRESS fields or add a new reader.

The SQL reviewer independently closed its SQL findings in `PF-08-sql-review.md`. This transport checkpoint remains OPEN only for the direct-launcher context completion and final byte readback. No runtime, joined, timing, queue delivery or A4 result is asserted here.

## Latest correction readback — OPEN for finding6

All five original findings are now closed in saved source. Office `1581–1586` uses its existing qualified namespace or the same validated Workbench HEADER context for a direct Case launch, rechecks current actor/NEXT after the read and opens the existing progress window. The original DR05 Try again is retained for all other review issues. No guessed namespace or new reader was added. Broker and FE Workbench `retryField` now explicitly recognise only `v===null` for the required null Work receipt field, closing root's observed successful-receipt defect without weakening nonnull Position checks. The remaining essential source finding is only the retired attention receipt's issue-code schema6 above.

## Final saved-source amendment — SOURCE PASS

Finding6 is closed. Broker job-step `95–98` uses `retiredPrepareIssue?` only in the typed EXPIRED/DISCARDED PREPARE schemas. Its classifier `147` admits the original finite issue list plus the exact `PF08_NEEDS_ATTENTION` literal; `280` requires a null `issue_work_id` for that added literal. The ordinary PREPARE classifier at `146` and the existing retired done/replay/zero-visited/worker/cursor validation at `281–298` remain intact. This permits the native retained issue receipt without expanding ordinary economic results.

The final saved Office bytes retain the actor/NEXT-qualified HEADER fallback (`1581–1586`), original pending Retry body protection and exact nonpending controller replacement (`1665–1679`), and revision-aware bounded attention paging (`1655–1663`). Sending/result copy, native Failed badge and scoped phone styling do not change command identity. Explicit Refresh rereads facts; the visible two-second timer reads PROGRESS and never invokes financial STEP or automatically sends the per-Candidate Retry (`1772–1779`). Broker/FE null-field decoders at `798/724` require strict null for PREPARE and preserve the nonnull Position branch.

All six review findings and root's subsequently observed strict-null receipt defect are closed. No additional essential source defect was found in this bounded review. The current transport preserves the DR04 identity-only queue, awaited durable failure handoff, original unknown-outcome recovery, native same-job Retry identity, mandatory RELEASE exclusion and existing economic owner boundaries. SQL ownership remains covered by the separate PF-08 SQL source review. No new route, command kind, money formula, source producer, blanket source-gate waiver or browser financial runner was introduced by these reviewed changes.

Raw SHA256 below records the final saved bytes after root preserved the existing mixed line endings. These are local source hashes, not a claim of a Git commit, deployment or runtime result.

| Repository / source | SHA256 |
| --- | --- |
| BE `broker/src/bpay-next-job-step.js` | `8E17E2497E804F2BFB4322D313E21F259F3999F1F0414B73C96237E5B6EFDBB2` |
| BE `broker/src/bpay-next-financial-queue.js` | `CF0CA928C7906829E3340C9AB3BD1B4C73A2102879BCF04AF7D4D86057856A51` |
| BE `broker/src/bpay-next-job-rpc.js` | `D68BEA0826711CD6340B2625D0765EA687C13AD435AE0D036F93863DB891FA70` |
| BE `broker/src/bpay-next-background.js` | `0524712D1BF52209EEA3A7F8846491BAC4FC452B2EA593978C85162476049514` |
| BE `broker/src/index.js` | `D9E36994E2531F9849B16AA11231E2DE10513F0B94239D91982D9D0738EC6A33` |
| BE `broker/src/bpay-next-workbench.js` | `DE79195B4C5CE489825383AC79BE4BF711A9B0D2D2041CD11D99C81C1F445F79` |
| BE `broker/src/bpay-next-preparation.js` | `A60F226A920EBBAD650CF67F4ABB281C71B7FFE1759B846EAE084EFE5C1B96CB` |
| BE `scripts/bpay-next-source-gates.mjs` | `90B713E9942507D42614FA0BE27A36A0A361D2EB028221067D1AE5C672BA2145` |
| BE `docs/bpay-next/final-definer-closures.json` | `BBA34606FE384DC8F28D649A575CB5145905FC1521A6CA97D045F4692879084D` |
| BE `docs/bpay-next/design/PF-08-build-design.md` (BD124) | `66C0E1B8AC38FB7547AF362D57A9008C1E555FF9B21F52CD0109A6DA4EB34318` |
| BE repeatable `11102026_2259_banking_pay_next_prepare_retry_v1.sql` | `1A57552C42D63D0DC885A4E7294472C03319537DA9E913A96CBBADC10C2AB879` |
| BE migration `11102026_2311_banking_pay_next_prepare_retry_receipt_null_guard.sql` | `5DB2D3BB2522A1352FB45E5F25DD6D0F85C651D45C0DF6179BF581DD1CE48F61` |
| FE `js/bpay-next-office.js` | `8751E31DF867712D01F5284ED18972C2BCD05A7A55502940D4B6DCD077624652` |
| FE `js/bpay-next-workbench.js` | `499E2C2FD3468C8801C86C1A1AB409792B19BE47390C069A2F73FB62DE0061CB` |
| FE `css/bpay-next-office.css` | `1C8F3AB6DA7971C4DCDFCD126FDB01AFB9673906B10A7998323A554162223E00` |

Reviewer boundary: business SQL, business fixtures, hosted endpoints, joined application acceptance, delivered queue/retry journeys, runtime/timing checks and A4 pixel acceptance remain **NOT_RUN**. Root-reported syntax/source checks and captures are separate receipts; this reviewer did not execute them.

