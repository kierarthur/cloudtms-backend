# PF-07 independent current-source review — sequence 111

**SOURCE PASS for reuse under the approved DR-04 identity-wake contract. No required product delta found.** Reviewed on 11 October 2026 against full card06:1805–1829, current ITEM/D01–D06 rows, review/R6.json, master08:195/496, dependencies DR-04/SP-14/F-11 and the saved current investigation. Product was read-only. No SQL/database/business calls, fixtures, tests, new tools, hosted actions or joined acceptance were performed.

## Exact scope and conflict

Card06:1809/1817/1819 and D01–D03 still require the historical full version/action/lease/epoch/cursor envelope and expressly say “not literal {id,kind}”. That conflicts with the subsequently approved DR-04 ID-only notifications and native fresh-CLAIM/one-STEP contract, also retained by BD122. **BD123 must explicitly record that historical wording as superseded**, preserving all behavioral clauses. Do not restore token messages or create a second intake/command/proof owner to reconcile obsolete prose. The current command/lease/checkpoint metadata remains closed and validated in the native CLAIM/STEP RPC; only queue transport is `{id,kind}`.

The current final registry resolves D01/D04 intake and enroller/service to complete2041 bodies, not old0270. Receipt1404, enrollment0541 and wake0560 remain current. Correctly built source can be reused. PF-08 owns later bounded repeated-failure/needs-attention behavior; no such scope is brought forward here.

## Nine required behaviors

All PASS entries below mean **source verified**, not executed application results.

| Card/checklist clause | Current source verdict and native authority |
| --- | --- |
| 1. Monotonic accepted sequence | PASS. Receive1404:33–58 takes module SHARE and the singleton command-clock UPDATE lock, returns exact existing ID/kind/epoch sequence, otherwise increments and inserts atomically. Transaction rollback consumes no accepted receipt sequence. |
| 2. Original duplicate receipt | PASS. Receive1404:40–47 and current typed intake delegates retain existing request/accepted receipt identity. Exact replay is not a new job or financial input. The current financial intake2041:242–288 keeps service/admin/NEXT admission and closed typed owners. |
| 3. All N enrolled before N+1 | PASS. Enroller2041:567–570 selects only `last_enrolled_sequence+1` under the native enrollment lock. Partial PREPARE enrolls one original sealed Candidate/member/job at a time and advances the clock only when the original expected membership is complete (:675–712). Original unsealed positive Create stays waiting; exact expired/discarded zero-job retirement stays separately qualified. |
| 4. Enrollment independent of effects | PASS. Enroller2041 inserts READY metadata jobs and completes enrollment without waiting for financial job DONE (:689–712,747–968). The per-Candidate financial CLAIM retains the independent earlier unfinished-job guard; these two orderings are not conflated. |
| 5. Due scheduled action enrollment | PASS. Durable request/enrollment is native; wake0560:59–82 discovers bounded READY/BLOCKED/expired-LEASED identities without changing availability. Actual SIMPLE_CANCEL claim0260:210–224 enforces server `available_at_utc`, exact pending request and earlier Candidate jobs. Service2041:1273–1289 preserves due/predecessor eligibility and never reuses an active lease. No queue/browser date decides maturity. |
| 6. Visible recoverable failed enrollment | PASS. Enrollment0541:28–40 rolls back the attempted member/job/clock subtransaction, retains only SQLSTATE/time on exact N+1 and returns the closed blocker; success clears it. Runtime223–237 validates this closed state; background35/73–75 retries. Native PROGRESS0814:2853/2887 exposes `enrolment_failed`; existing Office1686/1695 shows the recoverable message and Continue processing. No failed enrollment is reported as financial READY or a new REVIEW outcome. |
| 7. Metadata-only messages | PASS. Financial queue33–44 requires exactly two enumerable data fields, a finite native kind and native ID; extra payload, token or live object is rejected. Background18–30 and index41030/199074 send the same closed identities. Legacy classifier immediately recognizes those through the same validator. Monetary inputs/cursors remain native rows. |
| 8. Busy not REVIEW | PASS. Job-step391–397 maps returned 40001/40P01/55P03/57014 to known rollback RETRY_CALL; arbitrary55000 retains finite native rejections and timeout remains outcome_unknown. Exact Pay Query busy is observed at412 without changing authority. Current service2041:1120–1129 admits only fresh PREPARE NEW/PINNING before module/business locks and retains advanced/replay exclusions. This is the named SP-14 direct pin/STEP behavior, not a claim about unexecuted broad contention journeys. |
| 9. No retained connection between steps | PASS. Index40999–41016 creates per-invocation fixed HTTP transports. Queue143–176 performs fresh CLAIM plus at most one STEP; no database client, transaction handle or live financial object crosses messages. Native durable checkpoints and the next invocation's own claim determine continuation. |

## ACK, uncertainty and bounded recovery

Financial queue156–176 ACKs only definite native terminal/done outcomes or after the awaited successor send. Definite Position failure can ACK only through the awaited native recording callback; logging alone returns RETRY (index41009–41014). Unknown CLAIM/STEP/send never ACKs a successful financial outcome. An active lease returns native busy; a later fresh claim recovers committed phase/cursors after native expiry rather than adopting the stored token.

Service2041:1295–1310 exposes only a newly owner-returned lease. STEP validates the owner's actual native result before yielding its own exact nonce/epoch to READY (:1428–1484), so each identity continuation starts from committed native state. Original exact direct STEP replay remains available. No message carries financial authority.

Background48–99 explicitly disposes one message; its publish helper awaits sendBatch before ACK. Minute recovery106–153 gives independently caught bounded lanes a turn, including one ENROLL and <=100 READY/LEASE_EXPIRED pages; send failures do not suppress unrelated lanes. Wake0560 uses SKIP LOCKED and rotates native wake time without altering availability/status/epoch/tokens. Runtime RPC has fixed ENROLL/WAKE targets, 1KiB request, 32KiB response, <=100 rows and 8s deadline, closed bigint/identity/blocker schemas, duplicate-JSON rejection and no implicit retries. Job RPC fixes CLAIM/STEP to the original service only.

Index41050–41097 rechecks NEXT per delivery and awaits the actual native consumer; the configured TEST consumer is one message per batch (`wrangler.toml:250–258`). Scheduled recovery is wired at201728–201743. Accepted Office preparation199055–199082 is already durable before its best-effort ENROLL hint; failure is retained for native minute recovery. That HTTP receipt is not a queue STEP ACK. Existing LEGACY retirement is separately proven before ACK and has no financial writer fallback. No deployment/configuration or hosted binding was inspected or changed by this review.

## Final source checkpoint

| Reviewed saved source | SHA-256 |
| --- | --- |
| 2041 current intake/enroller/job service | `80675b1f77af852977da427d0ce0702bfb2a2da6fafcea2a5e05b731810de44b` |
| 1404 receipt | `be3c7dd17ecea6cbff631ed0b39c9b09947e6284b93db04ecdf517e320fed3e1` |
| 0541 enrollment service | `2854fd9f5f51d9654193c59d74258620afa8bdf020f45dfa8bfd78c3f47fc4b8` |
| 0560 wake service | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa` |
| financial-queue.js | `19f08cbdd3ba40b9439b99cc274d7fa48b98daed82bbd31207ca58c5c638cd68` |
| background.js | `0524712d1bf52209eea3a7f8846491bac4fc452b2ea593978c85162476049514` |
| runtime-rpc.js | `55e682eb8421314ab603c13d5913d365cfe90dcb0399a83167d01dd7a7aa232c` |
| job-rpc.js | `65454073073b13c740db5557a9a84a3cd6f64ef71f8493b2ef8a1fbfae7f5b26` |
| job-step.js | `d1a1349423ff00c7ed2713ea053e7d334c2015eeb250e8f2a33aa8aa42b07608` |
| index.js | `96e6692e52fc85ceb6fb60646bd5e369865dd6e6975d15dd002ac77574c4f3b0` |
| legacy-retirement.js | `7850a2fc72c72c462b67fef51d29a8258904f4b605127cb2c3f1d2d6a0ad588a` |
| wrangler.toml | `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d` |
| PF-07-current-investigation.md | `c798020817043b562f423a40a88214189c3699006ffde58da7140ad4856fba7e` |
| final-definer-closures.json | `a482c107d56b32b2bac71027ad933327463e9838d42f2d04228821bd9b70527e` |

No actual required product gap was found in the named source owners. Root must retain explicit BD123 conflict and current reuse/provenance documentation. D05/D06 joined browser-close/reopen, lost/duplicate/send-failure, before/after-click, contention, due-action, runtime/minute, timing/no-starvation and financial-effect acceptance remain **AUTHORED_NOT_RUN**. Source review does not tick application acceptance or prove installation/runtime behavior.
