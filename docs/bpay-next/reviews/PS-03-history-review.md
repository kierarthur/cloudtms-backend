# PS-03 Worker and Office history source review

10 October 2026. Read the full PS-03 card, exact D06/D07 rows, current BD033 scope, current Worker/Office history changes, their complete surrounding validators/read/navigation/render lifetimes, current 0400 member shape, registered SQL history contract and existing ordinary check sources. No execution, tests, SQL, fixtures, runtime, network, secrets or product edits. This review covers the two named callers; member-guard/index installation and joined conservation acceptance are separate.

**Final verdict: PASS for the finite D06/D07 caller source and D05 future checklist. No open actionable finding remains.** No SQL history successor, new wire field, new origin or money calculation is required.

## PS03-H01 — closed: retained-context actor lifetime

The initial Office `createClient().originContext()` implementation was a newly exported accessor to retained actual transfer/projection facts without the existing `actor()` guard. After `currentActorId()` changed, it could return the previous actor's context before a subsequent read cleared the maps. The production member renderer also calls this accessor when repainting an already retained page.

Root added `actor()` at `originContext()` entry, before member projection or either map read. Fresh saved lines 225–233 confirm the correction. The existing close-and-refuse behavior clears both maps and returns ACTOR_CHANGED for a changed actor or closed client. No observer, timer, transport, authentication framework or broader render rewrite was added. The source finding is closed; this reviewer did not execute the checks.

## Source behavior verified

- Both callers apply the same typed member projection and require each MEMBERS row's `transfer_id` to equal the admitted request parent. WORK permits only its genuine run-line UUID; CASE requires genuine instruction/result UUIDs and permits a NULL hold only for a stored zero contribution; NET_ADJUSTMENT/CASH_REISSUE require all member line/Case origin fields NULL. This matches 0400:70–77. The exact signed contribution is bounded decimal text, never Number/arithmetic. The projection object and nested origin are frozen and remain local; the RPC response still has its original fields.
- Worker request/auth/service-RPC, one bounded page, closed 52-section manifest/data schemas, cursor, streaming byte limits and safe errors are unchanged. Root's designated 1206 SQL history reader is unchanged. Its existing exact parent ancestry remains the authority for all added link requests.
- Office retains only already validated, deeply frozen TRANSFERS/PROJECTIONS pages after the read's actor/epoch/scope checks. Each map is local to one client and bounded to 128 rows, replacing the same ID and evicting the oldest retained ID. Close clears both maps. Context binds the member's actual transfer ID and worker, actual Candidate manifest, and that transfer's exact projection ID/worker. It never picks a latest projection, changes Candidate scope, requests hidden context automatically or writes persistent storage. The accessor now applies the same existing actor guard before returning retained facts.
- The member description shows WORK's exact frozen line; CASE's exact instruction/result/optional real hold; NET's actual containing-transfer projection and its retained exact request when read; and reissue's actual original-transfer/return-cash/request IDs. Missing retained context is explicitly described with the existing path to read it, never converted to an inferred ID or false zero. Raw row details remain available with NULL shown as Not recorded.
- Existing child links are used with actual parent identities: WORK uses RUN_LINE children; CASE uses INSTRUCTION children; NET/reissue use their actual worker's TRANSFERS/PROJECTIONS; a reissue TRANSFERS row uses its actual original transfer for RETURN_CASH. Context is retained only for the parent kinds that permit it. An unavailable Timesheet ancestry remains a real refusal; these links do not manufacture an alternate current owner or Candidate context.
- Existing whole-leg `cash_amount`, `member_count` and `member_cash_sum` stay raw server facts. The renderer displays signed contribution as a constituent and explicitly rejects Timesheet/Client allocation. It does not sum paginated members, add a cash calculator, touch payment preparation/journals, dispatch a financial command, export CSV or call a provider. No PaymentSummary layout is introduced.
- Stable focus keys continue to use the exact section/key or link tuple; the new descriptive paragraphs introduce no focusable control. Existing Back navigation, busy repaint, parent opener restoration, frame disposal and late-response suppression remain intact. The asset revision change is the finite support needed to load this updated renderer.

Existing normal check sources preserve SQL/Worker/Office metadata parity, exact scalar/cursor/field boundaries, authenticated fixed RPC, all-section graph reachability, bounded client reads and actor/close behavior. Fresh added ordinary cases cover the four member kinds, zero CASE/no hold, wrong shape/value type/parent, exact retained NET/reissue IDs, absent-context wording and actual original RETURN_CASH link. Root reported the current 11 frontend checks passing; this reviewer read their saved source and executed none. The existing actor/close cases plus the fresh accessor source guard substantiate this bounded source verdict; no builder execution or app acceptance is claimed by this review.

Re-read the complete saved D05 `PS-03-JOINED-ACCEPTANCE.md`. It accurately keeps all actual installed/app/business/Worker/queue/minute cases NOT_RUN and separately covers the four genuine origin types, zero CASE, wrong/foreign/multiple/concurrent singleton origins, frozen contribution and original builder conservation, replay and immutable history. It distinguishes ordinary NET and Case/destination NET formulas, retains unsupported future Umbrella/Case shape refusal, and does not impose premature request.transfer_id linkage at reissue member insertion. It introduces no new producer, calculator, fixture or execution claim. The only index.html product delta is the versioned history asset; unrelated assets and main entry behavior are unchanged.

Read checkpoint SHA-256:

| Source | SHA-256 |
|---|---|
| Worker `broker/src/bpay-next-history.js` | `b5bb109e546787fc65a83df1916dfd29c1b21d32ae6372fd3fd04ae97baa0d50` |
| Office `js/bpay-next-payment-history.js` | `66dbe8750a28e051d5c4f29a9e1c6742b6186af830255a510ea3ef1219f3c750` |
| `design/PS-03-SCOPE.md` | `c654440738e4d93563fb231ee05423d5e6ae46f126f1094a81fc1df0cce9e76a` |
| Current backend history checks | `ffe2265114027202e988efa2aecabdc14d5f2fe0b3a424f67281bdcff93c7378` |
| Current frontend history checks | `3e9a279f30363cf830863298b66844969a8a2da0fd7a672a86d1a9f8a174a502` |
| D05 future joined checklist | `05c98eb5cc3ea96f1772ffff841bad1c70737555bd644858375be2d707c3efcb` |
| BUILD-FE `index.html` history asset input | `a58adcd37479be1b0138594d4196c9d5d9f47b1f0755fbd045d40dd0353962ec` |
| Unchanged registered SQL history reader | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |

Only this review document was written. Joined app, installed, Worker/queue/minute and actual financial acceptance remain NOT_RUN.
