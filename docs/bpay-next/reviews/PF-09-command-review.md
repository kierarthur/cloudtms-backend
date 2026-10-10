# PF-09-D02 command reuse and D03 future checklist review

10 October 2026. **Source PASS for D02's existing exact command transport/integration and D03's saved future joined checklist. No actionable defect found in this scope.** Independent source-only review: no tests, SQL, product/runtime execution, fixtures, network/hosted operations or product edits. D01's new targeted guard is separate and is not approved by this verdict. No installation/app-tested claim.

Re-read the full PF-09 card/current three-row contract, current command handler and fixed RPC transport, actual normalized command route/auth/wake seams, existing command/RPC assertions, F-08 active contract evidence, previous transport census, and `PF-09-JOINED-ACCEPTANCE.md`. Current command/RPC hashes are unchanged from the independently inspected census and there is no saved product diff for either module.

## D02 exact existing contract

- INPUTS/OUTPUTS close all 13 current financial actions, including identity-only BIND_STORED_CREDIT, payout-only CASE_PAYOUT and zero-cash INTERNAL_SETTLE. Optional cancellation run/channel keys are strictly paired, PAYE expected revision is typed, and WRITE_OFF amount is permitted only for AMOUNT scope. Arbitrary actor, endpoint/function name, subset, derived pricing/bank facts and extra keys fail before the owner call.
- Command cash/NET are nonnegative decimal strings with at most 16 whole digits/two fractional places; zero PAYE NET is legitimate, while CSV settlement/return evidence is positive. Case write-off exact decimal strings retain the existing byte-bound arbitrary-precision grammar. Int64 identities/counts/revisions are decimal strings with BigInt bounds. Money is not converted to Number, exponent form or binary float. BigInt pennies checks validate the actual authoritative reply and requested amount; they do not calculate a new entitlement or reprice a target.
- Command/result/worker/transfer/instruction/case-component identities and retained replay scope are checked. CSV evidence replay correctly retains the original command identity, rather than substituting a new caller command. WRITE_OFF applied/remaining/protected values, event/issue and phase relationships remain coherent; the fixed case identity is retained. Blocked, cash-reissue, posting and expiry counts have their existing typed state/NULL relationships.
- Text remains named and bounded: receipt 256 UTF-8 bytes, reason 1,024, filename/code 256, CSV 120,000; request 2,048 and response 131,072 bytes. The actual stream is bounded before decode/parse; the deadline is 8 seconds. CSV text is a separately named bounded export with filename/SHA/status/payment-recorded fields, not an opaque financial record. Unknown responses remain sanitized with original IDs/body for retry, without automatic repeat or false mutation success.
- The raw RPC module has only fixed public financial intake and original-credit binding endpoints. It injects the validated Office actor/service headers and AbortSignal, uses OPERATION_NUDGE, refuses redirects and returns the raw response for the handler's byte/closed-schema validation. Binding accepts only command/legacy-case IDs; no caller money or function selection.
- The actual index normalizes duplicate/trailing slashes only for the scoped NEXT route, authenticates the current user and rejects nonadmin before constructing the service RPC. POST command dispatch passes that trusted user and the existing configured service headers. The underlying SQL role/admin/NEXT authority and explicit numeric-text projection are retained, as already recorded by F-08 and existing final-owner source assertions; no SQL successor is introduced here.
- A successful durable intake retains the existing ENROLL wake, registered with waitUntil or awaited when unavailable/throwing; failed wake is safely reported without converting an accepted command to rejection. The minute recovery remains durable authority. Financial actions/queue schemas/ACK/retry/one-call and unknown-outcome semantics have no PF-09 product change.

The existing preparation/history projection contracts retain separate Source ex/realised/held, target ex/VAT/inc, worker NET, bank cash and Case principal fields. Signed six-place factual preparation values and the fixed history STRING_OR_NULL field registry remain distinct from nonnegative two-place command cash/NET. PF-09 does not justify changing those established type meanings.

## D03 future joined scope

The saved checklist explicitly covers the card's planned 300-Timesheet Draft line/hold/worker/transfer penny agreement using authoritative DB-calculated decimal strings; distinct money kinds even when numbers agree; honest NULL/pending values; large/zero/signed values in their valid interfaces; numeric/exponent/extra/missing/unbounded/type refusals under the corresponding existing typed contracts; exact command identity after uncertainty; and the required final F-23 float/opaque/select-star negative with legitimate private typed point locks preserved.

It reuses the existing joined Draft receipt and expressly adds no second timing check, local data, scenario runner, fixture tool or testing programme. Every application/full Worker/queue/minute clause stays **NOT_RUN**. Rule B expected revisions/captured Candidate fields/query snapshot/pin remain the admitted authority; parked accepted-cut/resolver objects remain unused. Stored Workbench checks need no completed UI, and hosted release/readback/final guard rerun are separate later steps. This is a source-scope checklist PASS, not proof that those business clauses ran or that D01 is complete.

## Source evidence and hashes

Existing command units preserve large/zero amounts, refuse numeric/exponent/overprecision/extra reply fields and raw opaque result trees, check actual final SQL authority/typed projection, and retain authentication, stream cap/deadline/late response and original-ID uncertainty cases. RPC units cover fixed endpoints, actor/header/signal injection, unconsumed raw bodies, stored-credit refusal and actual index dispatch. The builder reports 90/90 existing combined command/RPC/preparation/history checks and four syntax checks passing; this reviewer inspected source and did not execute those checks.

| File | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-command.js` | `95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059` |
| `broker/src/bpay-next-command-rpc.js` | `060ce9bdab50576aec731d0b9b3c6499a608e3cd71c17a3c9bdf54483db1b68e` |
| `broker/src/index.js` | `164abc3f3836abbc95af562493fd25291014f0b6ec335e562b5af006e77f3f22` |
| `docs/bpay-next/PF-09-JOINED-ACCEPTANCE.md` | `898ba02a980c671b0362794ade216d041bc78a11260df2af62734ab2b180f84d` |
| `tests/bpay-next-command.test.js` | `b1051f3dcf536c0616d8699ee7bf98c4fe4c4658af8a57113e93e0645ee8bade` |
| `tests/bpay-next-command-rpc.test.js` | `60ce0eecd5da3c5ad6be7cca3d65ac3e274b4fb0f489d3288cd8d57e397ae9a9` |

The shared index verdict covers the existing command auth/dispatch/wake seams only. Hashes bind current saved source and required D02 reuse; no unrelated owner/route/source change or D01 guard implementation is implied.
