# WB-15 Candidate Banking window integration

Read-only frontend investigation, 10 October 2026. This note covers the real existing frame, Ready opener, local choice store and bounded scroll seams. It is design input; no product code, pack, SQL, runtime, application or tests were changed or invoked. Actual product screenshot acceptance remains later work.

## Authority and supplied patterns

Read the full WB-15 card (`02_WORKBENCH.md:616–653`), all current WB-15 checklist rows, and the supplied screen policy/T02/SCR-07. The exact frontend owners are Office's thin actor/journal shell and the Candidate Banking component in `js/bpay-next-workbench.js`; existing current-Work/current-Case/read-route objects remain their shared authorities.

The supplied T02/SCR-09 layout is a separate standard **Candidate Banking** window with Candidate name and TMS reference, Refresh, Ready/Waiting/Action required/Blocked, and the Payment/Client/Pay channel/Deductions/Amount ex VAT/Actions columns. SCR-07 supplies the separate Waiting treatment. Their sample money, ticks and later controls are illustrations, not input or completed implementation. Supplied pack files stay read-only under the owner's A3 instruction.

Owner policy binds the integration: SDP-29 preserves the parent's tab, scroll, filters and ticks; SDP-30 replaces numbered pages with continuous bounded scrolling; SDP-34 requires server sorting for every data column with a deterministic native tie and index; SDP-40 requires a real Candidate Banking opener; SDP-43 prohibits a separate captured-detail viewer in this window and requires genuine Draft navigation; SDP-44 describes one complete group per outer row. Do not show an execution pay date before execution or derive a Candidate Deductions total that the reader does not return. Actual applicable 42-point A4 screenshot review must follow the implemented source.

## Smallest real integration

1. Add the Candidate Banking mount/opener to the existing workbench module, using the existing qualified Workbench client and `options.showReadWindow`. Office wires it at the current Ready `openCandidate` seam (`bpay-next-office.js:733–735`) with one live-child guard, analogous to its real issue child at lines 737–738. Keep the ordinary Candidate record opener at lines 677–686 for the explicit bank-record action. A Ready-row double-click/Enter must stop falling back to that record opener, which closes the Banking parent.
2. Supply the actual validated Candidate name/reference from the opening Ready row, or one existing narrowed CANDIDATES read under the same scope. `mountReadyCandidates.open` currently forwards only Candidate UUID/context/scope (`bpay-next-workbench.js:731`); add the exact typed display fields or a typed opening record rather than manufacture a subtitle from demo values or a mutable legacy response. The standard adapter currently accepts a title but no subtitle; a small optional factual subtitle rendered as text is sufficient, without a second modal framework or HTML title injection.
3. Pass the **parent Ready choice-store handle** explicitly (`mountReadyCandidates.choiceStore`, line 772). Child reads narrow `candidate_id`; their scope must retain the parent's Client/channel scope. Do not call `createChoiceStore(childContext)`: line 682 includes Candidate scope in the namespace, so that would create a different store and split ticks from the main list. Validate the shared store's agency/actor/epoch/pay-date/parent filters against the opening parent, and validate the child UUID against the actual opening address. The child presentation scope is distinct from the parent choice authority. Later preparation freezes through the parent context, not a Candidate-narrowed context.
4. Keep choices IDs/booleans only. The current bounded shared store (lines 678–712) already preserves handles, immutable snapshots, Candidate dominance, generations and persistence status. Child Close/Escape tears down listeners and view state only; it must not clear this store, accepted journals or a pending preparation request. Group/line ticks remain the exact WB-24/WB-25 owners; do not turn generic group keys into invented Case/component selections.
5. Use ten **complete native parents** per read chunk, with independent child state and cursors. Ready includes only current, complete, non-dirty eligible groups. Waiting and issues are separate sections, not grey fillers in Ready. Keep server money as exact text with null shown as unavailable; do not sum only the first ten loaded parents for the Candidate strip or use Ready-only `group_amount` for positive Waiting obligations.

The DB investigation in `WB-15-current-scope-investigation.md` identifies the exact remaining reader facts: Case subtype/reference, standalone ROW treatment, genuine Waiting amounts and whole-Candidate section totals. Coordinate those named reader fields with strict Worker/FE mirrors. A `CASE` discriminator alone does not establish an Overpayment/Advance/Credit label; a technical accepted-run UUID alone does not establish a Draft reference or destination.

## Continuous scroll and coherent refresh

The existing Ready list (`bpay-next-workbench.js:714–774`) is a useful bounded skeleton: generation checks around reads, three retained DOM chunks, bounded cursor history, actor/owner checks, genuine server/header revision joins and logical top/bottom scrolling. The Candidate view needs its own pages, history, section, sort, highlighted native tuple and in-flight state. Reusing one client's transport is safe; sharing the parent's mutable cursor or reset state is not.

* Initial/default order is week ending newest first with native `(week, group_kind, Work-or-Case UUID)` tie. Fetch ten parents behind the scenes, load automatically near the scroll boundary and show a final loading-status row. Do not introduce Next/Previous/page-number/Load-more controls.
* Preserve an anchor as an actual retained native group identity plus row offset/focus and section/sort. Refresh stages a new coherent header/list before replacing the visible table once; keep the choice-store handle. `Ready.reset()` at line 765 clears pages/history/highlight and fetches from the top, so copying it would violate WB-15's refresh-anchor requirement.
* Never retag an old cursor with a new revision. A retained cursor may be reused only through the existing exact context/membership checks. On changed revision, acquire a fresh context and use only a server-qualified anchor if the reader supports it; if the native anchor is removed or a cursor is stale, visibly fall back to the valid fresh list. No new ad hoc anchor RPC or browser search of all historical rows is warranted. Final anchor behaviour must be resolved against the actual named keyset contract before claiming completion.
* Loading/error leaves the old list visibly unavailable and its payment controls fenced. Try again uses the captured informational read; it does not initiate preview, financial dispatch or a replacement request. Empty is shown only after a coherent current empty reply. Sorting intentionally starts at the top, with a new exact server cursor.
* If a child observes changed Workbench/adoption revision, invalidate the parent's coherent-read controls through its existing callback, while retaining local intent and journals. Do not let hidden old Ready data admit a payment because the child is current.

Current CANDIDATE_GROUPS transport has only the four week/kind/native UUID cursor fields (`validKey`, line 415) and does not implement all six data-column sort contracts. Add only the exact required sort variants/index/cursor mirrors in the named reader. Client/channel/deduction/amount sort must not be implemented by sorting the currently loaded ten rows. Unknown numeric sort values require an explicit null discriminator, not a financial zero.

## Frame, actor and callback lifetime

The actual `createStandardWindowAdapter` (`bpay-next-ui.js:32–68`) already opens the real `#modal` with `kind: 'bpay-next-read'`, no save/apply/parent-edit gate, retains parent content and reattaches it on render, and closes through the real guarded header button. Use this unchanged frame path. `alive()` means the frame is retained in the stack; `active()` means it is topmost. Reads may resolve against a retained live view only after identity/context/generation checks; buttons, focus restoration and financial/action callbacks require the exact active child and current actor/NEXT owner.

The existing `openIssueWindow` pattern (lines 851–858) supplies real parent context, changed-revision invalidation and a retained standard child. Apply the same protections to Candidate Banking: capture the opening ancestor/frame, actor, Candidate and filters; check them before and after awaits; dispose all child listeners and increment the generation on actual dismiss. Close/Escape must not call raw `closeModal`, rebuild the parent or bypass its Keep/Discard guard. Restore focus only to a connected element in the same current parent and actor; do not focus another modal after an asynchronous callback. The adapter's once-only initial focus already avoids stealing parent focus on reattachment.

Future action owners stay explicit. WB-16 owns expanded group detail; WB-17 the real retained physical Timesheet opener; WB-20 real In Draft navigation; WB-24/25 line/group ticking; WB-27 estimate; WB-37/38 snooze actions. SCR-07's Draft row has one actual Open Draft link and no additional Banking captured-detail viewer. Never bind a generic More/Resolve/Snooze/hold control by guessing an economic family. Missing later joins remain unbuilt obligations, rather than simulated controls.

## Source receipts and completion boundary

Fresh saved-source SHA-256:

| Source | SHA-256 |
|---|---|
| FE `js/bpay-next-workbench.js` | `b5ce284a12e1d923a7fbd775c01f1010c12600357e48bea35afc95950a98df1a` |
| FE `js/bpay-next-office.js` | `84c4d0f418be15ea69fbb0aa05d69dc194041bc66f6e67985818447243330aba` |
| FE `js/bpay-next-ui.js` | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
| Supplied `specs/T02_candidate_window.json` | `7f6f14de9d0808a53e4065b9b807dc5e26a0aa4da796e27ba68d7ba5758302e7` |
| Supplied `specs/SCR-07_candidate_waiting.json` | `6e8ccfda6e28a83fd8c06199c8085919e4f6a92ac6ae42ba17ada86da5bd63b7` |
| Supplied T02 dark PNG | `3a3aeb0ca222430471684957c5525db611353aec5af27578642d508b5345ade8` |
| Supplied SCR-07 dark PNG | `defb74fb33570a0f6d30b8f5cddbddcadfe7f914255cfb06399d0fc1b16ed4e8` |

This note does not approve the unbuilt Candidate view or whole WB-15. The smallest closure is the real shared-frame child with the correct parent store, exact factual reader fields/sorts and bounded coherent refresh. Rule B's raw choice/Candidate snapshot/existing PIN authority and parked F-25 producer/protocol work remain untouched.
