# DR-04 independent queue identity source review

**Final verdict: PASS for the current queue/service/adapter/static source slice; PDR04-QG-01 CLOSED.** The earlier unfinished lease/schema/static checkpoints below are retained as history and superseded by the final current audit and hashes appended at the end. Read-only review, 11 October 2026. No product edits, SQL/database execution, tests, fixtures, hosted actions or commits. App/queue acceptance is **NOT_RUN**. Parent counter/cancellation/Office limits remain in the separate focused receipt.

Reviewed current saved `broker/src/bpay-next-background.js`, `bpay-next-financial-queue.js`, `bpay-next-legacy-retirement.js`, and the four current NEXT send-site edits in `index.js`, against DR-04 card121–153, the saved queue investigation and original job/runtime owners. All references below are backend worktree paths.

## Required integration gates, already known to root

1. **Financial own-lease yield remains necessary.** The current0280 service preserves nonterminal LEASED after its original owner validates/commits a page. The new queue republish cannot fresh-claim that active lease until native expiry. The service successor must validate the original owner result first, then clear only the exact successful STEP's own nonterminal nonce/deadline and mark it READY, preserving committed phase/cursors, epoch and financial effects. No arbitrary lease adoption or unlock RPC is acceptable. Current service anchors: `supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql:415–451`.
2. **Adapter/result and projection phase closure remains pending.** Current `bpay-next-job-step.js:213–214` still equates a null lease deadline with DONE and would reject a successfully yielded nonterminal page. Its closed phase/input/checkpoint maps must match the final named WORKBENCH_WORK/WORKBENCH_CASE owner implementation; the queue now references these phases (`financial-queue.js:98–105,130–140`). Those branches are forward wiring, not proof of an existing successful projection step. Keep original rows≤100, exact native cursor type, epoch equality, typed issue/result contracts and fixed transport bounds. Update obsolete persisted-CLAIM/exact unknown-STEP recovery comments (`job-step.js:11–20`) to state actual native expiration/fresh-claim recovery. No synthetic success or phase stub was found in the reviewed queue files.

These are explicit unfinished supporting work, not newly discovered failures outside the authorized chain. The final package cannot receive a source PASS before both are reviewed. The card147 static shape check is also pending in the existing source-check path; runtime validation alone is not that check.

## Exact body and one-page execution

`financial-queue.js:33–45` validates an ordinary closed object with precisely the own enumerable data keys **id/kind**. IDs are UUIDs for closed native financial kinds/Workbench/expiry, and the native enrollment-clock singleton is exactly string`1` for ENROLL. Unknown kind, extra field, accessor/symbol/nested/encoded token or invalid identity is rejected. The existing byte bound is retained.

Background builders emit only native identity (`background.js:16–20`), and publication validates every body before awaited sendBatch (`26–30`). All four index send sites are now the same two-field forms (`index.js:41029,198871,198891,198913`), retaining their original post-receipt waitUntil/await and missed-wake handling. The F-13 classifier recognizes the closed identity family (`legacy-retirement.js:45–47`); the only semantic edits to that module are the import and this recognition. Existing real old-obligation qualification/disposition logic is preserved. Malformed/old incompatible NEXT bodies are retained through retry, not guessed terminal ACK.

Financial handling takes one original metadata CLAIM, builds one original typed STEP from that successful fresh response and performs that one bounded owner page (`financial-queue.js:156–168`). The only nonce/epoch entering STEP are the fresh claim reply. No stored-active-token reader, outer job lock, nonce in queue or second page loop was introduced. Existing per-kind continuation construction is reused for invocation-local STEP operands. The final published continuation is the original **wake**, not the constructed token-bearing call (`173`).

Workbench identity delivery likewise performs fresh CLAIM and one exact STEP from its returned generation/step/phase/cursor (`background.js:56–69`). Busy/stale/error outcomes retry. Existing Workbench STEP already yields its own lease atomically in `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql:533–541`, so no new financial owner/table is required for this lane. Enrollment and expiry use their retained original one-call owners (`background.js:71–86`).

## Loss, ACK and original failure handoff

Only a typed terminal CLAIM or known completed STEP is ACKed as finished (`financial-queue.js:159–169`). A known nonterminal success requires valid progress and awaited publication before ACK (`170–176`). Failed/unknown publication retries the identity; job effects/cursor remain durable and existing READY/LEASE_EXPIRED scans rediscover lost sends. An unknown CLAIM/STEP is not treated as completion; no active lease is adopted. Fresh claim remains gated by native expiration/order, then resumes from the actual committed cursor. Late old-token work is fenced by native nonce/epoch checks.

The original position-failure handoff survives (`financial-queue.js:67–82`). Only a parsed definite POSITION_APPLY rollback cause supplies the current invocation's exact job/nonce/epoch/phase/cursor to the original failure RPC. Uncertain/retryable outcomes never take that path. ACK_RECORDED still requires the existing durable callback; `index.js:41008–41013` otherwise returns RETRY. No token or body is added to failure logging, no financial completion is fabricated, and CLAIM errors carry no position STEP evidence.

Background catches dependency/send faults and explicitly retries (`94–96`). Recovery remains independent bounded lanes (`105–152`): one enrollment, ≤100 READY and expired leases, ≤100 expiry, cleanup, two pages per WB-37 due lane and ≤100 Workbench wakes. No batch/connection/provider/configuration changes or drain loop appear in the semantic diff.

## Saved-source footprint and bytes

Raw Git diff includes broad LF→CRLF conversion in these four files, making index appear wholly replaced. `git diff --ignore-space-at-eol` isolates the actual narrow edits: four index send substitutions; identity/CLAIM→STEP queue logic; two classifier lines. This is review noise/byte footprint to keep explicit, not an observed runtime defect or permission to alter unrelated source.

Checkpoint SHA256:

| File | SHA256 |
| --- | --- |
| broker/src/bpay-next-background.js | `37CE9D687D926581BA67B260C681ABD5C6882CF4A640F9936AED5B1B2B2518E2` |
| broker/src/bpay-next-financial-queue.js | `52957A1406A5DF1F710AD33141CD7FCFB5EA1352B9E691405B3CFFF3333DFC80` |
| broker/src/bpay-next-legacy-retirement.js | `B4385F54C7EB86B3F6F45393F3A9928C5AF5E8EF29266A3B3781FF7E246D499C` |
| broker/src/index.js | `416EC2469FE73188AD979A1ADD839F10E648F291F6A3DB04BDD4203BE7272DF0` |

These hashes describe the reviewed saved checkpoint, not a final source/installation identity. No compile, source-gate execution, business fixture, queue/minute path or connected acceptance was performed by this reviewer.

## Current closure audit

The earlier financial-yield and adapter/phase gaps are now closed in saved source. Service0533:2259–2305 validates the original owner's result against the retained job before yield. For nonterminal LEASED it checks its exact nonce/owner_epoch, then updates only that job's matching lease to READY and clears nonce/deadline, preserving committed phase/cursors/effects. It does not adopt an active lease or unlock before validating the page. The result is generated after yield. job-step:217–218 now requires null STEP lease deadline even for a nonterminal page, plus original owner_epoch for that page. Its closed PREPARE input/claim/result/checkpoint maps include the two native projection phases, preserve READY/REVIEW/EXPIRED and issue qualification, rows≤100, cursor types and response bounds. job-rpc keeps one fixed service-only financial target and no retry/drain.

Actual index sends were reread at41029,198871,198890–198891 and198913: three singleton ENROLL wakes and one original POSITION_APPLY job identity, all id/kind only. They retain original accepted-receipt gating and awaited/waitUntil transport with minute recovery after failed send. Other historical sends around40643–40890 remain legacy transport, not new NEXT payloads; their existing retirement authority is preserved. Background identity publication checks every body before its awaited sendBatch. Financial continuation publishes only the validated original wake. No financial fields, nonce, epoch, cursor, phase or encoded inner message appear in these NEXT bodies.

Financial delivery has exactly one fresh CLAIM and at most one STEP, using invocation-local fresh returned nonce/epoch/checkpoint. Terminal CLAIM/known DONE is ACK; a known nonterminal page requires actual valid advancement and an awaited wake send before ACK. Lost fresh CLAIM cannot reveal/adopt its stored active token; retries encounter native lease-busy until expiration, then fresh claim resumes the committed cursor. Unknown STEP likewise retries identity, not fabricated completion; a committed yield can be claimed immediately, while a retained active lease waits. The adapter's direct exact-STEP replay remains a separate supported API behavior; queue JSON never retains its token. Native job order, epoch and lease guards remain owners.

Original failure retention is intact: only a parsed definite POSITION_APPLY STEP rollback supplies that same invocation's exact job/nonce/epoch/phase/cursor/error to the original durable failure RPC. Retryable/unknown outcomes never become recorded terminal failures. index41008–41013 acknowledges only ACK_RECORDED from that existing helper, otherwise returns RETRY with bounded diagnostic code/recovery. Enrollment's closed blocked receipt now retries and is epoch-fenced by final0541; its focused review is `DR-04-parent-enrollment-review.md`. Existing separate bounded minute lanes and unchanged configuration remain; no browser financial continuation, batch sweep or Candidate inventory scan was introduced.

### Static source guard review

The gate now calls assertNextQueueShapes before normal final-definer/source checks. It strips ordinary comments, checks the four actual literal NEXT sends, required closed publication/validator and original-wake continuation patterns, and rejects the former full-body builders. This is the current finite NEXT wiring guard, not permission to rewrite unrelated legacy send sites or a general JS proof/parser.

The five root-lock baseline amendments are narrow: only the five full native signatures (CLAIM, PIN, capture, hold, no-Case complete); only ROWTYPE_SELECT:V_RUN:PRIVATE . BPAY_NEXT_PAY_RUN; only original `WHERE id=v_run_id FOR SHARE` full normalized baseline statements; only SHARE→UPDATE replacement; and only the original occurrence count. The same-signature/full-statement key and consumption remain. Other variables/tables/predicates/reads/signatures are not waived; frozen-authority and ordinary numeric/write/construct checks still run. This is retention of the exact existing parent read under stronger lock, not new SELECT-star permission.

Actual projection membership keys are BIGINT in core1324:452 and Case core0300:195. The two bounded subqueries0533:75–78,91–94 select only that native key, limit before aggregate and feed max(p.selection_no). File-gated P . SELECTION_NO type metadata therefore matches the current expressions. **PDR04-QG-01:** the latest initial strengthening checks only case-sensitive literal `max(p.selection_no)`, so a third MIN or differently cased/spaced MAX use can escape that count while still receiving the file-wide BIGINT classification. To fulfil the stated no-third-aggregate boundary, count normalized MIN/MAX occurrences for that column and restrict metadata to the exact projection signature/two current aggregates. This is source-allowance breadth only; no defect in the two actual SQL aggregates was found. Reported to root before final commit.

Current checkpoint bytes are superseded only when the final corrected static guard is independently reread. No source-gate, SQL, queue or joined app execution was performed by this reviewer; root's reported checks remain separate evidence.

## Final source verdict and exact bytes

**PASS. PDR04-QG-01 CLOSED in the current saved source gate.** Independently reread source-gates:1327–1355 after root's correction. The aggregate census strips SQL comments and counts both MIN/MAX, optional pg_catalog qualification and varying case/whitespace for p.selection_no; the exact count must be two. Both literal native Work/Case selected expressions and exactly two bounded order/limit subqueries are still required. The P . SELECTION_NO BIGINT map is now passed **only** to the single SQL statement defining exact signature `private.bpay_next_prepare_projection_page_v1(uuid,uuid,bigint,text,bigint,integer)`. Every other0533 statement receives an empty type map and the existing ordinary construct checker. No file-wide type waiver remains. Actual native schema declarations and two current bounded aggregates were verified; no extra UUID/unknown aggregate permission or blanket SQL exemption is granted. All normal source/frozen/numeric/write/final-definer checks remain in the path.

The five exact strengthened root-read allowances retain original baseline signatures, row variable/table, whole statement/predicate and count; they permit only the existing SHARE→UPDATE parent lock change required before Candidate/control/job locks. The type metadata does not exempt those row reads or any other construct. No protected baseline, original installed financial source or generic scanner rule is disabled.

Final actual queue source confirms all four named NEXT index sends, closed identity publication, fresh CLAIM/one STEP, exact own-lease yield after owner-result validation, typed projection phases, original failure retention, awaited continuation-before-ACK and indexed lost-wake recovery. No remaining essential finding in this exact source slice. Counter snapshots and DR05 active debit/release/retry dependencies remain governed by `DR-04-parent-enrollment-review.md`; DR06 owns future Office blocker display. This queue PASS does not accept those future UI/cleanup joins or any hosted installation.

| Final reviewed source | SHA256 |
|---|---|
| broker/src/bpay-next-background.js | 0524712d1bf52209eea3a7f8846491bac4fc452b2ea593978c85162476049514 |
| broker/src/bpay-next-financial-queue.js | 1311b89eb9b07b961140f64ee07b710170fdae975ea83e1718b72d020c6fc948 |
| broker/src/bpay-next-legacy-retirement.js | 7850a2fc72c72c462b67fef51d29a8258904f4b605127cb2c3f1d2d6a0ad588a |
| broker/src/index.js (four NEXT sends/failure dependency scope) | 5bc63c0e68c95913535e0a8947c0bbe25bfb899005ef15b811b4d14b798bf40d |
| broker/src/bpay-next-job-step.js | 1ae85f5636a7a401f3d73dc288c39502a7160b1727a5b248b37db22b7f0d954c |
| broker/src/bpay-next-job-rpc.js | d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7 |
| broker/src/bpay-next-runtime-rpc.js (ENROLL scope) | d9a6688bd0187508eb3f3af8e84645e2cc50c96c1292e2c1be72c3f1bbb1f204 |
| supabase/repeatable/11102026_0533_banking_pay_next_prepare_rule_corrections_v1.sql | 3587b148012921abfc6f1117f0d2bb5691f7e181b049ecd46a46c9b833e71d60 |
| supabase/repeatable/11102026_0541_banking_pay_next_enrollment_epoch_fence_v1.sql | 2854fd9f5f51d9654193c59d74258620afa8bdf020f45dfa8bfd78c3f47fc4b8 |
| scripts/bpay-next-source-gates.mjs | 016afe4b251e7c35f40427209f19cb7be21a5ad12a9b5eceaa308d8220815206 |

Root reported normal/source checks before the last narrow guard correction and is running its fresh final gate separately. This reviewer did not execute them. Every producer→Worker consumer→minute tick, browser-closed preparation, lost/duplicate/unknown transport and joined app acceptance remains AUTHORED/NOT_RUN.

Last current-byte recheck: the gate normalises CRLF→LF before scanning and slicing each0533 statement, keeping the scanner's offsets aligned with the source passed to the ordinary checker. This preserves the exact signature type scope and closes no additional business behavior. Final gate hash above includes this correction; PASS remains source-only.
