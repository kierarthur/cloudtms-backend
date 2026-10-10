# F-08 contract alignment — independent source review

10 October 2026. Read the complete R3 F-08 card, current D01–D04 checklist, BD019/BD020/BD021 scope, current designated final registry, captured contract investigation and actual current SQL/Worker/Office source. Read the three backend gate diffs and the bounded frontend old-journal gate correction. No product edits, test execution, SQL execution, runtime, fixtures, hosted reads or credentials were used.

**Final verdict: PASS for the F-08 source scope; no open concrete defect found.** The source-gate browser-ACL coverage finding F08-01 is closed in the fresh saved source. F08-02 is withdrawn after reviewing normalization before dispatch. The active contracts agree as detailed below; the final completion table and later app acceptance remain separate.

## Findings

### F08-01 — closed: browser GRANT assertion must inspect actual ACL source

The initial gate change built `service` from only the final intake/page CREATE statements, then retained `assert.doesNotMatch(service,/grant[^;]*(?:anon|authenticated)/i)`. Those bodies contain no GRANT statements, so the negative ACL assertion became ineffective. Positive REVOKE/service grants do not rule out a later browser grant.

Root corrected the assertion to inspect `intake.closure+'\n'+pages.closure`. Fresh preparation gate SHA `5ceb752d39b24061a39d0d8c6e4c69db4e1acfd2c9f27836ba8884c46670d1a7` preserves the original negative ACL coverage alongside exact final signature/body and positive ACL checks. No open action remains for F08-01.

### F08-02 — withdrawn: UUID arguments already normalize before dispatch

The initial review incorrectly treated the admitted raw UUID strings as the receipt validator's comparison values. The actual complete path prevents that mismatch:

- `normalized()` at preparation.js lines 173–176 recursively freezes arrays/records and lowercases every UUID string.
- `commandCall` returns `args:normalized(args)` at line 182; `pageCall` does the same at line 196 after input validation.
- `handle` constructs that normalized call before dispatch (line 389), sends its exact `call.args` (line 396), and validates the reply against the same call (line 401).

Uppercase/mixed-case accepted UUIDs therefore reach both SQL and receipt scope comparisons in canonical lowercase form. Numeric strings and channels retain their original exact comparison semantics. No product change or new test is required for this withdrawn finding. Preparation source SHA remains `9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749`.

## Current M1–M4 agreement

| Object | Verified source contract |
| --- | --- |
| M1 / D01 | Final 2018 CONFIRM private/public owners return the same run, DRAFT, review revision, replay and 1–2 exact Draft addresses. 0236 includes zero/cancelled worker channel membership and orders PAYE then UMBRELLA. Worker and Office enforce exact row shape, count/order and same-run scope; Worker UUID arguments normalize before dispatch. Office retains/open/selects each address and clears only channel-dependent worker/detail scope. |
| M2 / D02 | Final 2018 SQL has exactly 18 active page kinds. In-memory source comparison found every active kind's argument-key list equal to the corresponding current Worker list. CHANNEL_DRAFTS raw rows/complete becomes the closed five-key Worker/Office envelope; CHANNEL_WORKERS limits/order/row-channel and cursor agree; CHANNEL_STATUS verifies real run/channel/worker SQL membership and the same 25-field header/transfer cursor contract. Typed money and integer values remain strings. |
| M3 / D03 | CANCEL_REQUEST has base command/worker keys, with run_id/pay_channel strictly paired when either is present. SQL/Worker/Office reject partial, wrong-type and extra subset keys. SQL verifies actual worker run/channel before the whole-Candidate owner. Reply command/worker/sequence/phase/cursor/released count/blocked code/replay and BLOCKED equivalence agree; no run_id is incorrectly required in that reply. |
| M4 / D04 | Parsed current source registries contain exactly 52 SQL/Worker/Office sections. Section names, parent kinds, key types/order, field arrays and amount_scope match with no differences. Request/cursor scope binding and string/null values remain closed. SQL and fixed authenticated Worker reader are activation-independent; Office open retains its separate F-16 ownerReady boundary under BD021. No financial activation permission or anonymous/admin bypass follows from history. |

The current Worker also retains two **parked** F-25 page parsers, ACCEPTED_CANDIDATES and ACCEPTED_WORK. Their presence makes its full input object 20 entries; neither is an active branch in final 2018 SQL. The completion table must describe agreement of the 18 active kinds, rather than incorrectly claiming the entire current Worker object contains exactly 18 entries. This review does not reopen the parked protocol.

Current cancellation scope mismatch is `23514/BPAY_NEXT_CANCEL_CHANNEL_SCOPE_INVALID` (2018 line 3237), mapped to `BPAY_NEXT_COMMAND_CONFLICT`/409. Partial keys/type/channel enum failures are REQUEST_INVALID/400. The old investigation note's claim that actual scope mismatch is 22023/REQUEST_INVALID is stale; the final completion table should use the current mapping.

All eight current SQL definitions checked are byte-identical after LF normalization to the investigation's captured definitions, and their designated paths match the current registry. Historical F-25 producer paragraphs in that investigation are superseded by rule B. Current Worker preparation and Office hashes differ from that old note and are recorded below.

## Gate changes

The backend intake/preparation gates now resolve the registry's exact signature, read its designated final physical source, scan SQL statements and require exactly one matching definition. Body assertions inspect that statement; ACL assertions inspect the actual closure. Authority and argument helpers are selected from their own current final roots, rather than allowing predecessor bodies to satisfy a successor's assertions. History follows its final registered reader and retains the 52-section assertion and all existing request/auth/route cases.

No literal existing test case was removed: command 26→26, preparation 23→23, history 7→7 (dynamic cases remain unchanged). The added channel-status positive/wrong-channel and paired cancel/no-RPC refusals are inside existing cases. The frontend correction removes the v4-only accepted_prepare property when constructing an old v1 journal, expects actual v4 adoption and null accepted_prepare, and preserves count, invalid-journal and exact unknown-request replay checks. No new harness or product behavior is introduced by that correction.

Root reported the ordinary backend 83-case and frontend 60-case source gates passing. This reviewer did not execute them and does not treat them as database/app acceptance.

## Read checkpoint SHA-256

| Source | SHA-256 |
| --- | --- |
| Final 2018 foundation/intake source | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Final 0236 channel readers | `a90b3b2fe01e079e7177bedde583d5e0b66dd037b2344272a6a8fb288d008719` |
| Final 1206 history reader | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| `broker/src/bpay-next-preparation.js` | `9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749` |
| `broker/src/bpay-next-command.js` | `95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059` |
| `broker/src/bpay-next-history.js` | `81d4ca2fb90b1bbec7dea3a41698933f70b306833079484d45a89e4e77738451` |
| BUILD-FE `js/bpay-next-office.js` | `ad62e30d80660757529b402e0f260f263b57184d56da2dd4dc9f89595acadd9c` |
| BUILD-FE `js/bpay-next-payment-history.js` | `715a1035f88204b39771932a479f733b98ef7c971656c77767bd396861a24e9b` |
| `tests/bpay-next-command.test.js` | `b1051f3dcf536c0616d8699ee7bf98c4fe4c4658af8a57113e93e0645ee8bade` |
| `tests/bpay-next-preparation.test.js` | `5ceb752d39b24061a39d0d8c6e4c69db4e1acfd2c9f27836ba8884c46670d1a7` |
| `tests/bpay-next-payment-history-caller.test.js` | `8a09560333f72c485564777980d0c82af4451d0f4e73f9e02f55cc01f54fc98c` |
| BUILD-FE old-journal frozen-detail gate | `a9cb950db10f77791bb239a64f2526a41bbbbd0355f70c901db75e8ea165a2ec` |
| `final-definer-closures.json` | `875f3b481cdd9e617a408ff49ff66556f14714ae7b5d1de421351858d149c28a` |

Mixed Draft, channel cancellation and history app journeys remain NOT_RUN here. No hosted installation, activation or payment execution is inferred from this source review.

## Final completion-note review

10 October 2026. **PASS: the final completion note and contract appendix now accurately document the current F-08 source contracts. No open documentation or product defect remains in this review scope.** Re-read both saved documents and their current underlying validators; performed only in-memory source/Markdown comparisons, not tests or runtime calls.

All 18 appendix argument schemas match the active SQL/Worker key lists and Worker validator types. All 52 history rows match the current SQL/Worker/Office metadata exactly, including native composite-key type/order, field order, parent and amount scope. The added STATUS header has exactly 25 key/type pairs matching current source; its seven-field transfer row, complete/null versus continuation cursor, channel check, revision inequality and enum sets agree. The ordinary history page's 13 keys, manifest's nine keys, six-key section declaration and nine-key cursor agree with the closed validators, including terminal/null relationships, context/component scope and exact unavailable arrays.

M1 and M3 exact request/reply addresses and paired channel cancellation remain correct. The error/status descriptions match the existing preparation/command/history maps, including actual cancellation scope `23514`→COMMAND_CONFLICT/409, mutation uncertainty, 502 malformed/oversized history transport responses, and 503 SQL history byte-cap refusal. Authentication remains before parsing; history's independent service admission and separate F-16 Office opening boundary are stated without broadening access.

The note was corrected to say history is **individually** fence-exempt, rather than the only exempt NEXT route. The appendix now states the actual history checks: declared composite-key types, exact field allowlists, and STRING_OR_NULL data scalars. The note also distinguishes all 18 SQL/Worker kinds from Office's consumed subset, including the three channel kinds. The finite missing STATUS/history reply-key schemas were added. These were documentation corrections; no product successor or parked protocol work was required.

Final reviewed SHA-256:

| Document | SHA-256 |
| --- | --- |
| `docs/bpay-next/F-08-COMPLETION.md` | `23c5f573629c933d7b0fe226e8091859b54237540792708fa52a8441272f3ea7` |
| `docs/bpay-next/F-08-CONTRACT-APPENDIX.md` | `9851dcf2e46af896e6525ac8c8b4b34621dcf6f64e3ba31383a0a97bee50a118` |

The documents preserve source-check versus installed/app acceptance separation. Their reported ordinary checks belong to the builder's receipts; this reviewer ran no tests, SQL, hosted inspection or app journey and changed no checklist boxes.
