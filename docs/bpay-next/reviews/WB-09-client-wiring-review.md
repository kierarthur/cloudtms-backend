# WB-09 client and wiring source review

Verdict: SOURCE PASS — both initial findings are closed in the fresh frontend source. No open client/wiring finding remains. Source-only review; no product edits, SQL/runtime/tests/app/browser/hosted calls were performed. Root alone authors product.

Reviewed the full WB-09 card (02_WORKBENCH.md:388–420), all seven checklist objects, the saved WB-09 reader contract/transport design, and the new FE helper plus shared wiring. This is a review of client/route transport; it does not certify the evolving SQL owner or future Workbench screens.

## Initial findings (closed by the fresh source review below)

1. **Environment request / known refusal (P1).** FE `js/bpay-next-workbench.js:465` invokes the genuine `authFetch` without `X-CloudTMS-Banking-Environment`. The shared wrapper only copies supplied headers (`js/main.js:5185`); it does not manufacture this header. BE `broker/src/bpay-next-environment.js:4–16,23–30` requires an exact configured TEST/LIVE request header and otherwise returns HTTP409 `{ok:false,error_code:'BPAY_NEXT_ENVIRONMENT_MISMATCH',retryable:false}` before any business RPC. Thus every normal call through the initial helper is refused. Add the same finite environment dependency/validation and header used by existing Office transport (`js/bpay-next-office.js:262`), and preserve redirect:error for the raw authenticated read. FE lines469–471 also discard this genuine three-key refusal into generic WORKBENCH_UNAVAILABLE. Recognize only the exact known environment envelope and retain its code/retryability; do not accept arbitrary server payloads or relax the ten-key SQL refusal schema.

2. **Late failure callback actor boundary (P2).** FE `js/bpay-next-workbench.js:477` invokes optional `onReadFailure` even when the captured actor is no longer current, including its own ACTOR_CHANGED error or a delayed timeout. Positive rows are correctly rejected after the actor changes. Apply the same captured-actor boundary before invoking an old callback so that a future consumer cannot paint the previous actor's error into a new session. This needs only a finite callback guard, not a new UI/lifetime framework.

## Source checks that pass

| Boundary | Exact evidence |
| --- | --- |
| Shared FE streaming exemption | `js/main.js:5164–5178` adds only normalized exact GET workbench-page; APILOG stays false for this endpoint, so existing clone.text/log paths do not consume or log financial replies. auth401Mode:return at helper465 makes the existing wrapper return401 without token-refresh/retry. |
| Static include | `index.html:3027` includes the one new ordinary script before application main logic; no browser RPC or eager read on module load. |
| BE authentication/environment order | `broker/src/index.js:198775–198834` adds the exact GET to the existing NEXT Office block: requireUser, admin403, existing environment guard, then fixed service-RPC construction. No blanket /next/* exemption. |
| Legacy fence | `broker/src/bpay-next-legacy-fence.js:162–175` adds one exact normalized GET exemption. Wrong methods, unknown NEXT routes and unrelated old financial paths remain subject to the existing fence. |
| Fixed RPC / trusted actor | Worker442–448 fixes `public.bpay_next_workbench_page_v1`, creates only `{p_request:{actor_user_id:trustedActor,kind,...closedArgs}}`, uses service headers and redirect:error. Caller URL cannot supply an actor or function name. |
| Bounded read | FE418–423 and Worker416–421 bound actual streamed UTF-8 bytes to131072, reject invalid UTF-8/duplicate keys/excessive nesting, abort/cancel on failure. FE463/476 and Worker460/464 enforce8000ms; each helper issues one request, no automatic page walk/retry, and retains the whole-page receipt. |
| Exact row contract | Static literal comparison of FE/BE ROWS objects is equal: all10 kinds, field counts HEADER26/CANDIDATES31/PICK_CANDIDATES3/PICK_CLIENTS2/ACTION_REQUIRED38/BLOCKED38/ISSUE_DETAIL38/GROUP_LINES71/EXPORT_ROWS41/CANDIDATE_GROUPS36. Decimal and bigint values remain exact strings; no browser money calculation, fake GBP0, service RPC or raw payload logger. |
| Cursor / order / scope | FE384–441 and equivalent Worker logic validate closed kind-specific keys, strict order/no duplicates, full actor/scope/revision context, exact last-key next_cursor and page_receipt. SQL1520:366–392 uses the same complete native key tuples and applies one ASC/DESC direction to every component; UTF-8 byte comparison matches COLLATE C and canonical UUID ordering. Decimal comparison is ordering-only. HEADER is one row; group limit≤10, other page limit≤100. |
| Unavailability | Typed refusals throw a human-readable WorkbenchReadError; unknown/malformed money never becomes[]/0. Unknown subtotal coherence, nonselectable unavailable facts, exact WORK/CASE group identity and row scope are checked before returning success. |

## Initial source snapshot

BE `broker/src/bpay-next-workbench.js`: `19a567d3c064254cd13087ccfbb949a3d8df8cdc95a842d152c6816e907f6aa9`.
BE `broker/src/index.js`: `b9872851195f757a57575b90cfade390d47ffeccc1991cbd702c01dd1dc8742b`.
BE `broker/src/bpay-next-legacy-fence.js`: `3a3a467e70b35da1367376fa51909b098ba2160dda003e19a7b2740bfd3e0db0`.
FE `js/bpay-next-workbench.js`: `4c5177b4bab82e4e099afcc5b2bb8c669203271872037caf7b7240b60ef6c84e`.
FE `js/main.js`: `4c83cfbcbc253050387fe9840a5e6d0230d5f558e4f642eb8569ddf987f6d44a`.
FE `index.html`: `91f6a8720be153470278f282c80cf4b00149c09525a682d32f968cf7c4a211b9`.
Sort comparison only, evolving SQL1520: `eebe1a463cac604165a8b9e460ba2a5e82c95d45edd118806d96ae3d03736409`.

App LEGACY/NEXT journeys, nonadmin actual requests,1/17/100 walks, timeout/cancellation and future screen presentation remain NOT_RUN by this reviewer. Future WB13 UI and later estimate/resolution/snooze kinds are outside this item's client helper. This review does not change checklist ticks.

## Fresh correction closure — 2026-10-10

Read the entire current FE helper again, not just the changed tail. The initial findings above remain as review history and are superseded by this finite closure:

* **Environment finding CLOSED.** FE454–455 captures a finite TEST/LIVE value (`options.environment`, TEST default) and rejects other values before fetching. FE466 sends the exact environment header plus accept:application/json, redirect:error, the deadline signal and auth401Mode:return. FE472 recognizes only the exact three-key, nonretryable ENVIRONMENT_MISMATCH/OFFICE_FORBIDDEN envelopes, preserves their genuine codes, and leaves the strict ten-key SQL refusal decoder intact. There is no new owner decision, arbitrary-code allowance, retry or financial-success fallback.
* **Late callback finding CLOSED.** FE479 re-reads currentActorId before invoking onReadFailure, requires it to equal the captured actor, and isolates callback exceptions. A stale actor's callback cannot publish its failure into another actor's session; callback failure cannot replace the typed original error. Positive reply handling still checks actor after the bounded stream read.
* The complete stream budget remains131072 bytes; the decimal scalar validator now permits values within that same hard envelope rather than applying a smaller arbitrary8192-byte financial-text cap. It still requires exact decimal text, and the actual stream bounds the entire reply before decoding. No Number money conversion or zero substitution was introduced.
* Full helper reread retains one exact GET, closed ten-kind rows/cursors/receipts, strict order/scope, internal8-second abort, no auto-paging, automatic retry, raw financial logging or direct service RPC. Shared main/index and BE route/fence hashes are unchanged from the earlier review.

Fresh reviewed hashes:

| Source | SHA-256 |
| --- | --- |
| FE js/bpay-next-workbench.js (full fresh reread) | d043df552d24a05bd0aa0cef988a3450dba832ab98fcfbc20f22df093a847572 |
| FE js/main.js | 4c83cfbcbc253050387fe9840a5e6d0230d5f558e4f642eb8569ddf987f6d44a |
| FE index.html | 91f6a8720be153470278f282c80cf4b00149c09525a682d32f968cf7c4a211b9 |
| BE broker/src/index.js | b9872851195f757a57575b90cfade390d47ffeccc1991cbd702c01dd1dc8742b |
| BE broker/src/bpay-next-legacy-fence.js | 3a3a467e70b35da1367376fa51909b098ba2160dda003e19a7b2740bfd3e0db0 |
| BE broker/src/bpay-next-workbench.js (fresh reference hash; its independent owner review remains separate) | c96e79af5bb4a684ebe842eca71131d6218f9032d575e79d29a1550433d40aed |

Root separately reports local source/load/seal/coupling checks PASS. Those are not executions performed by this reviewer. App/auth/page-size/failure journeys remain NOT_RUN; future screens and evolving SQL owner are not certified by this client/wiring review. No checklist change was made.
