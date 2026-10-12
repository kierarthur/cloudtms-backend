# DR-07 Office banner source review

Independent read-only review, 12 October 2026. **SOURCE PASS after O07-01 and O07-02 were corrected in the final reread below.** UI/A4 and joined acceptance are not reviewed here. DR-07-D06/ITEM retain the actual DR-28 Payment Batches host dependency; a reusable adapter is not that join.

Authority: complete DR-07 approved card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:202–222`, every checklist row `IMPLEMENTATION_CHECKLIST.md:3676–3700`, `design/DR-07-build-scope.md` (BD139), the earlier screen investigation and transport receipt. Current source examined: BUILD-FE `js/bpay-next-office.js:2331–2400,2425–2435,2481–2499,2511–2579,2727–2742`; `js/bpay-next-workbench.js:816–820,965–968`; scoped Office CSS banner additions.

Initial source hashes: Office `b508d21f59953ed9183ffac5c39a363873020ae6abe4b36cf6a631da4220b903`; Workbench `60b716265fbb2f775a4869019f8c5b9020c9f32f838777f800ce58f6eb68a77b`; Office CSS `597012310e0be8ee32c772e2927dbac6dda76b5a516fa1e7900385b24d93942f`.

## Essential finding

**O07-01 — own initial reattachment can be consumed while the parent is covered.** `own()` sets `ownOpened=true` after the MINE request but before its `visible()` test (Office `:2381–2382`). A child opened while that request is pending can leave the namespace/current caller valid but the Banking frame inactive. The returned own Run is then never opened, yet later refreshes skip MINE because the once flag is already true. Set the flag only for a result accepted while the parent is interactable, immediately before the actual open (or an interactable qualified empty result). A covered result should remain eligible for the existing later refresh. This needs no new request, store or protocol.

**O07-02 — dropping a page changes the meaning of the restored scroll offset.** `read(next)` drops the oldest retained page after page four (`:2373`), then restores the previous numeric scrollTop (`:2377`). A user at the end of three full pages consequently moves from the old last page directly to the end of the new last page, passing its initial rows. The five-second refresh also collapses the retained visible window to its first page (`:2375,:2386`). Preserve an exact visible native row/offset through bounded page removal and refresh, using at most the same three pages and seek-history budget. Existing native cursor/order must remain unchanged; no all-run cache or extra reader is needed.

## Confirmed boundaries

The stable WB11 host places banners between tabs and totals and exposes only that DOM host. Actual mount follows native capability/header setup, uses the current Workbench namespace and caller actor, and closes with the owning Office frame. The banner adapter retains at most three native pages plus a bounded seek-history list; it does not collect a whole agency inventory or derive money. Replies are qualified by namespace, generation and current actor/NEXT owner before application. Owner/namespace changes clear retained rows and addresses; close removes timers/listeners and invalidates late replies. Directory creator identity is displayed independently of the viewer's progress/journal namespace.

Explicit progress opening uses the exact native Run and only the initiating viewer's matching same-Run last-pointer recovery evidence. A matching incomplete original journal stays incomplete; existing retryReadback reuses its descriptor/command/request bytes. SEALED server reattachment without that viewer's incomplete journal permits factual progress/review. OPEN progress is factual only: selection submitting wording, no omission read, no SEALED-only WAKE or Review admission, and no reconstruction of another creator's upload. Manual Resume run ID remains absent.

`setConfirmedDrafts` validates the original receipt addresses against the existing CHANNEL_DRAFTS read and retains run/channel addresses only; `openConfirmedChannel` remains the original qualified opener. No expiry/pay date, pricing, new confirmed-run directory or later batch table is introduced. The setter/export is supporting code for the later actual DR28 join, not proof of that delivery. Native window/actor/owner and exact original-address validation remain required at that join.

Scoped CSS uses the existing modern modal, bounds the banner scroll region, wraps creator copy and provides phone control minimum height. Pixels and phone target geometry need the separate A4 review, including the corrected bounded multi-page transitions; no global inventory or browser sort is needed.

No product edits, tests, SQL/business calls, fixtures, renders, captures, hosted requests or commits were performed by this reviewer. Source-only findings; the prior closed transport SOURCE PASS remains separately recorded.

## Final source closure

Fresh saved source reread: Office `4f5a7409d57315f8cd3f7be17fccd40a35dc58410d19ae64c0c686dcd28bafc5`; Workbench `60b716265fbb2f775a4869019f8c5b9020c9f32f838777f800ce58f6eb68a77b`; Office CSS `597012310e0be8ee32c772e2927dbac6dda76b5a516fa1e7900385b24d93942f`.

- **O07-01 CLOSED:** `own():2391–2392` rechecks current generation and actual `visible()` after the MINE await before consuming `ownOpened`. A covered parent keeps its later reattachment opportunity.
- **O07-02 CLOSED:** `read():2370–2387` captures the first visible exact native Run and its offset, then restores that row after rendering a next/back/refreshed bounded window. Normal refresh follows the new response's actual continuation cursors sequentially for at most the prior three retained pages, and applies the completed window only after every awaited reply passes current namespace/generation checks. Reset is explicitly separate. Page drop, back history and the native order are preserved without a full-run cache.

The fresh presentation additions use native info/success notices, strong creator/confirmed-channel copy and the same scoped bounded scroll/phone CSS. They add no financial or mutation authority. Current source fulfills the implemented Banking Pay/banner adapter slice; actual A4 and DR28 joined acceptance remain separate and unclaimed. No remaining essential source finding in this slice.

Final presentation-only reread: Office `22800816df3b8caa9cf8b68bd533c12c731dc0091ea0248e52e46438d33236bd`; Workbench remains `60b716265fbb2f775a4869019f8c5b9020c9f32f838777f800ce58f6eb68a77b`; Office CSS `b6c0bc96566445941b330ab8b6256361b880890b6030425dd6d3c46a15f469f6`. The own-read failure uses the same generic one-sentence refusal, native Candidate counts select singular/plural wording, and banner-only CSS sets row flow/left alignment and 20px spacing above the visible banner host. Context/journal/anchor controls remain as reviewed. **SOURCE PASS retained**; pixels remain separately reviewed.
