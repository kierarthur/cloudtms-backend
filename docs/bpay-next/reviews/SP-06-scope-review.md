# SP-06 whole-batch scope and source review

Read-only review on 10 October 2026 against the complete SP-06 card (05_SOURCE_AND_PROTECTED_PAY.md:248–276), all D01–D05 checklist rows, B4.4 shared-owner integrity, SRQ-026/028/029/043/157/227, and BD044/SP-06-SCOPE. Current verdict: **D01–D05 source PASS after the final saved guard corrections; one literal BUILD_LOG line-per-piece documentation split remains**. The closed findings concern the actual protected controllers, not another owner or a new protocol. This reviewer executed no tests, SQL, runtime, fixtures, app actions or hosted operations and changed no product source.

## Required scope and contracts

| Deliverable | Actual source and conclusion |
|---|---|
| D01 DB publish/status/owner route | Reuse genuine current protected publisher/status and 0659 owner route. Four-key status `{ok,route,published,receipt}` and eleven-key SOURCE_LOCAL receipt remain validated by the existing Worker; mapped NEXT result has fourteen fields. The sole added SQL body is atomic WAIT0821 permission support, necessary to meet the explicit current-payment-authoriser rule in the final transaction. |
| D02 orchestrator | Existing protected-action-orchestrator.mjs is unchanged. All six actual actions remain routed through the installed prepare/context and final owners. No external C1 start/status is required to confirm a saved local decision. WAIT returns before calculation and does not zero or replace its target. WITHDRAW and RECORD_NOT_WORKED use the existing complete target composition; reconciliation retains the genuine native component identity. |
| D03 target preview | protected-next-preview.mjs is unchanged. Existing qualified native event/segment identity, schedule, ordinal and actual target authority are reused; no browser amounts, imported repricing or different Candidate/Client/Contract are introduced. |
| D04 editor | Existing editor plus its genuine workspace/main controllers retain the original request/key/literal inputs on uncertainty and issue one identical POST per deliberate retry. Confirmed LOCAL COMPLETE/PENDING_FREEZE, mapped NEXT and WAIT close through the existing parent hook independently of published/application status. Late context callbacks now use the exact actor/frame/parent guards reviewed below. |
| D05 presentation | Pure protectedActionConfirmation recognizes only actual flat LOCAL11/12/13, NEXT14 and WAIT5/6. Exact outer keys, UUIDs, safe numeric or positive-int64-text versions, exact decimal text, authority/state/first-authorisation ties and the genuine deferred-release variant are checked before clearing pending. SOURCE_LOCAL status envelopes are validated by Worker and returned as their real flat result; an unused parallel Office decoder is unnecessary. No success is called APPLIED. |

## Exact implementation checks

The new `public.weekly_exceptional_pay_wait_atomic_v1(jsonb)` definition in 0821 differs from the complete original1534 definition only by the comment and `perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY');` after the original request/key validation and before completion replay/fresh Source admission or business locks. Original request hash, replay/collision, family/run locking, current decision capture, version/hash checks, audit and unchanged-target statements remain exact. Original definition SHA-256: `5a12e0fa3df72e7b42f5c6e736ce70794838351f2210264bb6af4a95c748d603`; new definition: `ca098fba1db5d7e5420c606dfaf08bce406f729dd591853f3536c727feef3a45`. Existing public identity and owner/service-only ACL membership remain. Registry adds the previously absent WAIT chain with exact original1534 predecessor and 0821 root/physical winner; unrelated chains have no semantic change.

The protected-only routes adapter uses exact SQLSTATE/message pairs for original stale/local-owner and Office authority refusals, the exact Source admission busy pair, actual Banking busy envelopes, and owned WeeklyProtectedActionError. It preserves code/status/retryability without a routing decision, fallback owner or success receipt. Unknown database/transport errors keep the original handler. The six action branch alone invokes the adapter; authenticated actor overwrite and recursive assertNoBrowserFinancialFacts remain before dispatch. No producer, public trigger, command kind, financial calculator or money owner was added or replaced.

Office H01/H02 from the separate Office review are closed at the listed presentation/controller hashes: no automatic changed recovery payload or second POST, no permissive NEXT/WAIT or fabricated SOURCE_LOCAL confirmation. Actual initial frame capture, actor/exact frame admission, response/catch/finally guards and genuine parent capture are present. The following two earlier readback findings are retained as review history and are **CLOSED** at the final hashes below:

1. **Workspace late context finally:** `js/weekly-source/workspace-actions.js:1097`, SHA `0dd6fa6c58ebf12c56cc2f8fce8f52444eafaf0428d82499c14f07126d22b926`, still admits `finally` using only `sequence === readSequence`, then calls `rerender(kind)` even when the awaiting context read returned because actor/frame changed. Require `sequence === readSequence && frameCurrent()` before busy/rerender; the timeout already checks the frame. Existing review-context load has its rerender guard.
2. **Main parent-refresh catch:** `js/main.js:178861`, SHA `688fc15cabac9089c3cd9f82a26ef963e5083249f063b1dc769641b7b3cd7bdc`, still checks actorCurrent rather than parentCurrent before its late refresh-failure toast. Start and post-await success already use parentCurrent. Require parentCurrent in this catch so a same-actor replacement parent receives no old completion UI effect. The true child frame is now captured immediately after showModal.

Both findings were sent to the builder and corrected. Fresh workspace line1097 requires `sequence === readSequence && frameCurrent()` before busy/rerender, with the timeout also checking frameCurrent. Fresh main line178861 requires parentCurrent before its late catch toast, matching the start/post-await guards. The child and genuine original parent are captured before deferred callbacks. Final workspace SHA is `4c86b1b39870169db9e5cbbf3d9fe5f9d8795d8d68e13a86fbc296302bfb0b17`; main SHA is `3a51abe4360398ad21136c54587c89e1ee8c0f3197ecc4bef763208b61c38a27`. No outstanding product/source finding remains. Registry final SHA `88c03a24ed35aef5539614543446c876f6ca514a98d83cc50a64a77d68efb28d` has exactly one new WAIT chain and zero modified/removed existing chains or other top-level semantic changes.

## Documentation and limits

BUILD_LOG has a separate line for each of the six actual actions and records every installed piece, including SCX019/020/023/024/025/026/027/078/085/111/116/117/124 and NX010/017, plus permission and nested financial-input rejection. **Finite documentation finding:** card05:270 explicitly requires one line per action and per piece. SCX020/023, SCX024/025/027, SCX078/NX010/NX017, SCX116/117 and SCX111/124 currently share lines. Split their existing factual entries into individual piece lines with the same genuine closed/internal contracts; no product change or proof framework is required. This is a documentation closure, not a missing financial owner. SP-06-COMPLETION identifies genuine flat action contracts and distinguishes receipt closure from Banking application. SP-06-JOINED-NOT-RUN includes authorised8h→7h, unauthorised first-Authorise/no-WORK, local published=false/pending-freeze closure, exact lost-response retry, identity stability, complete withdrawal/reconciliation, permission/browser facts, actor/frame lifetime and parent refresh. Those joined actions remain **NOT_RUN**. Builder-reported local source/load/seal/verifier checks are separate evidence, not executions by this reviewer. Full application/release verification remains pending as required.

## Reviewed raw hashes

| Source | SHA-256 |
|---|---|
| BE/supabase/repeatable/10102026_0821_banking_pay_next_protected_wait_permission_v1.sql | 47f7f1befc74f5e5fc8e891d1e8b5b199ac3467ff047ef4555226be89a96f6a0 |
| BE/broker/src/weekly-source/routes.js | 3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11 |
| BE/broker/src/weekly-source/protected-action-orchestrator.mjs | 5352ef8bcc66e87b090d5a1d4f40ba872cf7a542d38f60aaf690ec1eea80f3c3 |
| BE/broker/src/weekly-source/protected-next-preview.mjs | 49ed6b5e24c1ebf6afd9dbada7de0d9d321c8297584491610353c1ee2a2bfc4a |
| BE/docs/bpay-next/final-definer-closures.json | 88c03a24ed35aef5539614543446c876f6ca514a98d83cc50a64a77d68efb28d |
| BE/docs/bpay-next/design/SP-06-SCOPE.md | 881ecc5ed1d8826296f4884a1c782326eccd344b708b73bb92b634038e85aba0 |
| BE/docs/bpay-next/BUILD_LOG.md | f356c4cd87f4b9a27efa843d864d5e16a59633fd21accbbf5a3f283bf64ae040 |
| BE/docs/bpay-next/SP-06-COMPLETION.md | c9d065feb5268696ff7d92a23e38a6a4c8c446d2e8dd1cc33c5ec7e527d3fdb3 |
| BE/docs/bpay-next/SP-06-JOINED-NOT-RUN.md | 62eddd6f5a1c96c8de9210de3da9e7c634fa2363d2ba325ec06049e37b4701a4 |
| FE/js/weekly-source-presentation-v1.js | 8a0064242756964f9accd7cb9ab2245badbe2fa0ea7abae7ac3bf185a2d0d9b1 |
| FE/js/weekly-source/protected-shift-editor.js | a9725b6713da4ee875f67c7b48d9a5509ad9c0b54589949d97f761662a26d81e |
| FE/js/weekly-source/workspace-actions.js | 4c86b1b39870169db9e5cbbf3d9fe5f9d8795d8d68e13a86fbc296302bfb0b17 |
| FE/js/main.js | 3a51abe4360398ad21136c54587c89e1ee8c0f3197ecc4bef763208b61c38a27 |
| FE/index.html | 90d08e1c63ded349e92eb1853fc1fad613722c75b909203d92729be83f85c169 |
| FE/BANKING_PAY_BIBLE.md | 1139a81feb04a7049ea36831575a09d010085e7d34b9e04c89fa5a558ceb4562 |
