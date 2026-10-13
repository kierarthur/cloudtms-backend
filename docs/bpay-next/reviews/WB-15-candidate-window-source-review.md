# WB-15 Candidate window source review

Independent read-only source review, 10 October 2026. Reviewed the saved FE Workbench/Office/CSS/index diff and the exact Worker CANDIDATE_GROUPS mirrors against the full WB-15 card, current checklist and supplied T02/SCR-07/policy. No product edit, runtime, tests, SQL, business call or screen capture was performed. This is a source verdict on the new outer-window slice; WB-16/17/24/25/DR-02 and actual A4 screenshots remain explicit later joins.

## Verdict

**CHANGES REQUIRED: CW01 and CW02.** The real child-frame/shared-store wiring and strict typed mirrors are sound at this source boundary, but an initial stale read cannot recover and an anchored refresh makes earlier groups unreachable through continuous scroll.

## Open findings

### CW01 — initial revision race permanently rejects fresh Refresh

`js/bpay-next-workbench.js:937` rejects `opening && !sameWorkbenchRevision(address.context, page.context)` for every direction, including explicit Refresh. If the Workbench changes after the main Ready row is opened but before the first child read, `opening` stays true. Try again and Refresh continue comparing every fresh reply with the original stale address; the user cannot recover inside the child.

Permit the explicit fresh-context Refresh path after genuine namespace/actor/epoch/pay-date checks, then use the existing `onChanged` parent invalidation. Keep the original first-open coherence fence for an automatic initial read; do not retag an old cursor or silently accept an old scope.

### CW02 — anchor refresh removes upward access to prior groups

At lines 932–940, Refresh resolves the first visible native group as `group_key`, adopts that anchored block and clears `history`. The scroll handler at line 945 only fetches backward when `history.length` is nonzero. Thus a refresh at a later group preserves its offset but provides no path to earlier groups: upward scrolling ends at the anchor until a sort/tab reset. This is a truncated continuous list rather than complete reachability with a retained refresh anchor.

Retain a truthful bounded upward/restart path using fresh native scope/keyset authority. Do not manufacture a historical cursor with a new context. The SQL's identity anchor (`1946:1028–1040`) is a real current native seek and is suitable for preserving the visual anchor; the FE still needs to make the preceding scope reachable.

## Passed source boundaries

* Office line 735 now opens an actual `openCandidateWindow` through the existing `showReadWindow`, qualified Workbench client and live-child guard. It passes `readyLists.choiceStore()` directly. The ordinary Candidate record remains the explicit bank-warning owner; the Ready opener no longer closes the parent into that record.
* The typed Ready address at Workbench line 767 carries actual Candidate UUID, validated display name/reference, original context and parent Client/channel scope. Lines 888–897 validate the shared store namespace before narrowing child reads to that Candidate. There is no child-scoped replacement store.
* `includeCandidatePayments` at line 741 operates on the existing bounded IDs/booleans intent. It clears subordinate exclusions for only the exact Candidate, preserves other Candidates and correctly records a positive Candidate exception in NONE mode. It changes no money, accepted receipt or submission journal. Office's selection callback renders the shell, whose `onAvailability` rejoins/renders Ready, so returning from the child reflects the same store.
* The child uses ten-parent read chunks, at most three retained DOM chunks and a bounded cursor history. Automatic forward/back reads and explicit Refresh use generation/actor checks before adoption. Header reads are closed typed replies and must share Workbench/adoption identity with the list. Empty is displayed only for a coherent available empty result. Loading/error disables actions; unknown amounts remain null and display as unavailable.
* The real standard frame separates `alive()` from `active()`. Informational reads may resolve into a retained same-actor frame; actions require the active child. Actual dismiss invalidates the generation and removes the owner listener. Close/Escape keeps the parent store and original financial journals; focus restoration requires the original actor/NEXT owner and a connected prior element.
* No new route, financial RPC, browser amount calculation, Source producer, parked F-25 authority, invented Case label or Draft destination was introduced. Payment labels and native ROW identities arrive as typed reader facts. Unknown future openGroup/openDraft owners are omitted rather than guessed.
* FE and Worker `ROWS`, `validKey` and `rowKey` definitions are text-identical after EOL normalization. New group sorts carry explicit null discriminants and native presentation UUID ties; decimals remain exact text. ROW keys are restricted to the actual GROUP_LINES/CANDIDATE_GROUPS request kinds; group/parent/component identities and accepted run/command/epoch null tuples are checked in the reply.
* Added CSS is scoped to the real Banking read modal and uses current theme variables. It wraps Client labels, exposes the phone sort control and real phone Open action at 44px, uses keyboard focus-visible row styling and preserves existing shared card styles. Index cache markers are updated for the changed Office/Workbench/CSS and shared dependency group. This is source inspection, not a visual/phone acceptance claim.

## Completion limits

The current child is not a complete WB-15 item. The supplied policy's selected count and per-section tab counts are not yet rendered here; the existing strip shows only server Ready amount. Native group ticks/expansion, real Draft navigation and actual Timesheet joins remain their declared future owners. There are no fake buttons or economic calculations to stand in for them. Bank-warning metadata is carried by the exact row schema; the parent already presents its real bank-record action. Any Candidate-child badge or later control must remain factual and use that existing authority.

Actual source screenshots, full applicable policy review, twelve-parent scroll evidence and coherent refresh-anchor interaction are NOT RUN by this reviewer. Do not use this note to mark whole-item or joined-screen completion.

## Fresh source SHA-256

| File | SHA-256 |
|---|---|
| FE `js/bpay-next-workbench.js` | `b0a5dfdd4fd63617aa62f698daea9a202f1610396267a711d50516bb734b1960` |
| FE `js/bpay-next-office.js` | `8e6ba73244cf62fca4ad5c6d271cd60aecf3f9d0e47cc7916829f05f64561309` |
| FE `css/bpay-next-office.css` | `bc3a9884645ebe6b1f13e26865c626d06f269133c3f006a4002b6a0e79a0a137` |
| FE `index.html` | `6c51a35417e526415b4d19fa0733e5e999d6e82e2a29f24bf36619fdb101718e` |
| BE `broker/src/bpay-next-workbench.js` | `054077a163f6f035d8654641d06d2665991b0d201fb86aff1833060c013e8b0b` |

Hashes bind the reviewed candidate. Parent edits are concurrent; a later correction needs a fresh read and appended receipt.

## Fresh correction review — CW01/CW02 closed

Re-read the actual saved `js/bpay-next-workbench.js` after the parent corrections. **SOURCE PASS for this outer-window slice; no open concrete source finding remains from this review.** This supersedes the initial changes-required verdict, without changing the whole-item/future-join/A4 limits above.

* **CW01 CLOSED:** line 938 excludes explicit Refresh from both prior-context and initial opening-revision fences. Lines 937/939 still require closed header/list identity and exact original namespace agency/actor/epoch/pay-date. Line 940 invalidates the parent through `onChanged` before adopting a changed revision. Initial automatic loading retains its original opening coherence check, while a genuine user Refresh can now recover from the initial race.
* **CW02 CLOSED:** the new `restart` direction clears the native anchor and obtains the actual beginning of the scope (line 933), checks the same fresh revision (938), and adopts a fresh server context and bounded first block (940–943). Upward scroll, negative wheel, upward touch movement and Home/ArrowUp/PageUp reach that restart when the anchored first block has no retained history (947–949). This deliberately restarts at the beginning rather than pretending to have a previous cursor. Earlier groups are reachable again; existing history continues ordinary bounded back reads. No historical cursor is retagged.
* **Busy table fence PASS:** line 917 retains the existing table DOM while loading, disables its controls, dims the table and exposes a loading status. The row focus/key handlers now consult `interactive()` at event time (927), and `open()` also checks it (910), so retained rows cannot act through a previously captured availability flag. A completed coherent read replaces the table once; a failure renders the retained facts unavailable.

Final fresh FE Workbench SHA-256: `c11f104a180688c560b341dfafc3a2135d486b8f811ddfd2cfa3db9ee38a55de`. Office/CSS/index/Worker hashes remain exactly those recorded above. No product, runtime, test, SQL, fixture or screenshot invocation was performed by this reviewer.

## Actual A4 display correction boundary

Fresh source/display correction review is **SOURCE PASS** at Workbench SHA `0d6c5cb787e0d98d26b991698b5b4e9a5458ace3428983f99f1d7968c688580c` and CSS SHA `eba4f5c63967eea2b12ea90b628c9dbb15d372f01b505fbbc5243c1471eaee18`. The earlier source receipt is retained history. Office/UI/index remain as recorded at the preceding boundary.

The actual name/reference node is moved into the standard title using the existing active-frame `onMount` callback. The same text node is reattached rather than duplicated; it contains only the validated opening Candidate label/reference. Persistent selected-tab CSS binds actual `aria-selected`, not hover. The sort paragraph uses the existing live-copy exemption so initially empty mount processing cannot hide later sort text. Include-all uses the actual primary class and sets a plain inline local status after successful store mutation; it adds no count, money or command. The narrowly scoped phone44px rule covers actual Candidate controls/header/footer without broadening other modal families.

The initial suggestion about missing group/tab counts is superseded by the binding owner A2/BD071 disposition (`BUILD_LOG.md:683`): HEADER member counts do not establish native presentation-group totals, and no new server field/feature may be introduced only to fill the supplied picture. Omitting those unsupported decorations is truthful. This does not waive later real selected/tick joins or authorize invented totals.

Independent actual-image A4 receipts are saved in `screen-designs/SCR-09/review.md` (36 PASS/0 FAIL/6 N/A) and `screen-designs/SCR-07/review.md` (35 PASS/0 FAIL/7 N/A). All11 final PNGs were individually inspected; the final two phone images were re-viewed after the44px correction and the other9 hashes remained unchanged. These approve only the built outer/read/local-Include slices. Whole WB-15 and its ordered future joins remain OPEN; no reviewer browser, capture, runtime, tests, SQL, fixtures, business or pack tool was run.
