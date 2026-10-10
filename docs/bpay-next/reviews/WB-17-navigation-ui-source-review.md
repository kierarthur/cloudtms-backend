# WB-17 navigation UI source review

10 October 2026, independent saved-source review. **SOURCE PASS for the current bounded transport/native-navigation implementation:** NAV01, NAV02 and NAV03 are closed by the corrections below. No product edits or SQL/tests/runtime/screens/browser/network calls were made by this reviewer. Whole WB-17/application acceptance is not approved.

| Source | SHA-256 |
| --- | --- |
| BUILD-FE `js/main.js` | `0c396f0b1fae9694fb2f9d3dba662ba7484364a7b44dd190ebfbbe3b0b279bd6` |
| BUILD-FE `js/bpay-next-workbench.js` | `aa81371c4a34486221215b2faff20a21289ef691ee80e5c4e791f65fdd8ba227` |
| BUILD-FE `js/modal-modernisation.js` | `c86b30ffac3ded99d625e278ccf492e87ef50ed68504db5d0043f5bf6538228b` |
| BUILD-FE `css/bpay-next-office.css` | `3858dcdd0d53b69e9afb4cdc653b9b4f7853e134d8264870d1bed17abf397a77` |
| BUILD-FE `index.html` | `22aa2fb6ad7465190f593c7b1f483305f912cf5cb27db7f8b8711058fcc4b55b` |
| BE `broker/src/bpay-next-workbench.js` | `bc063d8205be961e18703dfa6d85dbc431a176f4bde7b37bcb484b4d49992232` |
| BE `supabase/repeatable/10102026_2105_banking_pay_next_exact_work_navigation_v1.sql` | `dad87c105db7daa58b4b62433812d13bcab89aa86481a158e012791777d2c12e` |
| Existing public frozen wrapper0250 | `ca6e50fcb6324e9e3a39e840a664ddfa53a7a7d96d9d0fb9859d65743614e091` |
| BUILD-FE `js/bpay-next-office.js` | `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0` |

## Findings

### NAV01 — CLOSED: exact physical navigation refuses a different Timesheet

Fresh-source correction: the named callback passes `__bpayNextExactPhysicalId` and the validated optional `__bpayNextExactPhysicalVersion` (`main.js:7174–7175`). Native hydration checks both before any fetched-ID overwrite or `adoptMovedTimesheetIdForFastOpen` call (`148813–148820` in this source). A mismatch throws “This Timesheet changed; return to Banking Pay and refresh.” The existing details catch stores the inline hydration error, rerenders the native frame and returns before the later details render; it does not adopt the alternative physical record. Other native callers do not carry these flags and retain their prior behavior. This closes the following original finding without a new viewer, producer, command or snapshot owner.

Original finding, retained as review history:

The new callback (`main.js:7166–7188`) passes the validated revision physical UUID into ordinary `openTimesheet` and retains actor/NEXT/parent checks. The native opener then starts its existing hydration. At `148775–148789` hydration reads `fetchedCurrentId` and overwrites `requested_timesheet_id`; at `148793–148800` a different returned ID changes `tsId`, `baseRow.timesheet_id` and invokes `adoptMovedTimesheetIdForFastOpen`.

Concrete source path: the reader/row names approved physical record A; a newer physical record B is returned by the existing mutable details path; the new button opens A initially but native hydration then displays B. The supplied physical revision/version fields are not consumed by the callback. A HEADER check after dismissal cannot undo that substitution or make B the requested revision's evidence. This can arise before Create as well as in an accidentally reused captured path: the current approved Work revision is not equivalent to every newer mutable physical Timesheet.

Required narrow fix: opt this named WB17 opener into an exact requested-physical identity boundary and refuse/mark unavailable when returned physical identity differs, instead of adopting the newer record as that evidence. Preserve the ordinary opener's existing current-navigation behavior for other callers. A separately labelled current/newer navigation link may retain its own semantics. Do not add a second frozen viewer, Source producer, business command or booking-original fallback. Post-Create captured evidence remains in the single Open Draft owner.

### NAV02 — CLOSED: scope-neutral newer-information notice

The current Candidate return check (`bpay-next-workbench.js:1046`) now says exactly “Newer information is available.” It retains the same actual HEADER/context comparison, stale-action fence and stored scroll. It does not assert an unproved Candidate-specific change or add a server field. Original finding follows for review history.

`bpay-next-workbench.js:1046` says “This Candidate has newer information” after `sameWorkbenchRevision` differs. That helper (`598`) compares global Workbench/adoption context, including agency/owner/pay date. A different Candidate's metadata/projection change can advance the same context; this comparison does not prove this Candidate changed.

Use a scope-neutral notice such as “Payments have newer information — Refresh” while retaining the real HEADER check and stale-action fence. No new Candidate version field or server feature is necessary for this precision correction.

### NAV03 — CLOSED: failed return check is distinguished from failed open

Both issue `invoke` (`841`) and Candidate `open` (`1048`) now set a local `opened` stage only after the callback finishes. A later HEADER load/validation failure says “Could not check for newer information; refresh to continue.” A failed callback retains the existing failed-open message. The same actor/context/generation checks prevent an abandoned result from changing a newer window. No automatic reopen or refresh is introduced. Original finding follows for review history.

Candidate `open` (`1048`) and issue `invoke` (`841`) combine successful open/dismissal and the later HEADER read in one catch, which reports “Could not open this payment; try again.” When only the return HEADER check fails, the Timesheet did open and close. Keep the existing conservative action pause and retained rows, but distinguish that stage with a truthful “Could not check for newer information — Refresh” result. Do not retry/open a replacement automatically.

## Passed source boundaries

The new Worker and FE CANDIDATE_GROUPS/ISSUE_DETAIL row declarations and Source-kind discriminator agree. Their exact shape adds seven factual fields: physical revision UUID/version, existing Source kind, optional retained HEAD, root UUID/version and Source week. UUID/version pairs and ordinary/Case NULL restrictions are checked; Source initial no-HEAD remains valid. Existing cursor, page receipt, actor/scope and exact decimal validators remain intact. No accepted-input/protocol/producer call is added. SQL-source correctness has its own independent owner review; this verdict concerns the transport/navigation additions.

The main callback opens only from an actual current read parent, refuses accepted-run rows, captures the new native Timesheet child, preserves the child's original `_onDismiss` with its receiver/arguments, and resolves after that hook even if it throws. Normal Close/Escape invokes `cleanupClosingFrame` (`main.js:337162–337163`) after popping the child and resumes the same parent (`337127–337131`). The wrapper does not overwrite global Close/Escape or the parent's own discard/lifetime hook. The callback rechecks actor/NEXT and parent survival after return. The legacy raw `closeModal()` helper lacks `_onDismiss`; this new promise is established for normal native Close/Escape/save-frame cleanup, not proof of arbitrary programmatic raw closes. Do not introduce raw-close calls for this integration.

Candidate navigation retains the same native page blocks/history, context and choice-store handle. Return checking is a real HEADER request, guarded by actor/owner, generation and captured-context identity. It does not replace pages, clear unticks, accept a new work revision, calculate money or dispatch a mutation. A detected changed context fences interaction and preserves scroll through the notice render. Explicit Refresh remains the only current-row adoption. Issue-detail has the analogous captured-context/current-owner checks and real HEADER read, retaining rows/scroll with a stale notice. Normal inactive parent controls cannot double-open a second child.

Office is not changed by this WB17 diff. Existing actual run-bound frozen SHIFTS/BREAKS/RATE_DETAILS/RATE_SCHEDULE transport, route and public0250/private0025/1134 owners remain unchanged. Their REVIEW/DRAFT + READY authority, captured approved-line/revision identity and <=100 bounded continuations remain the evidence authority. Neither original-booking `original_timesheet_id` nor the WB16 adjustment-parent field is relabelled captured physical identity. A3c's one Open Draft and J2e's existing captured views are preserved; no second Candidate frozen viewer is added.

## Scope still open

The Candidate amount → exact selected Ready Timesheets summary icon remains a genuine WB23/WB24/WB25 membership/intent join. The native summary helper is available for proved finite physical-ID scope, but the loaded group page is not a complete selected manifest. Actual Draft destination/uploader joins and later application acceptance remain ordered/open. No A4 screenshot acceptance, first-use, queue, hosted or timing result is supplied by this review.

## Fresh native-frame supplement

The latest main callback captures the actual parent's managed footer before opening the Timesheet, removes that parent's footer object from the child chrome, restores native tab visibility and removes only the Banking read sizing/confirmation classes. It does not remove the parent content or change its stored rows, pages, choice-store or close hook. The existing parent's mount callback owns reattachment on return. The exact opener also receives the factual Timesheet reference for both the initial title and completed hydration title; this is not an original-booking or current-record identity substitution.

The callback sets `_bpayNextEvidenceNavigation` only on the newly opened native Timesheet child. The one changed `modal-modernisation.js:1026` operand suppresses the Timesheet intro's explanatory copy only when the current frame has that exact flag. Existing tab headings, real Source presentation/permissions, form actions and other legacy callers retain their owners. This presentation flag is not a readonly/financial authority claim.

Candidate Refresh remains enabled whenever the actual current actor/owner window is not busy (`1054`), including a newer-information fence or failed return check; its guarded click performs the explicit read. Return checking itself only compares the validated real HEADER, preserves current rows/scroll/ticks and pauses action if coherence is lost. All three review findings are closed at the table's final hashes. No new source producer, financial command, captured-evidence owner or server feature is introduced by these corrections.

## Final child-presentation source boundary

The final additional main changes are confined to this named navigation: the callback adds `bpay-next-timesheet-evidence` on its real native child and removes it in the wrapped dismiss hook's `finally` before resolving. The existing overview week-ending field uses slash-formatted date text only when the row has the exact Banking physical-identity flag (`main.js:365050`); its stored date, ordering and ordinary legacy rendering are unchanged.

The six appended CSS lines apply under that class at <=700px: native tabs wrap instead of being clipped, and tab/Close targets have minimum 44px dimensions. They add no body/layout copy, hidden business control or new scrolling owner. The asset cache markers for the changed main/Workbench/modernisation/CSS dependencies agree at `20261010-next-exact-navigation-r1`, preserving their existing script order. The previously reviewed source identity, actors, close/return ownership and frozen/Source financial authorities remain intact. **The bounded SOURCE PASS remains valid at the updated hashes.** This supplement is a static source check; the separate A4 reviewer owns the saved screenshots and visual policy verdict.

### Last accessibility correction

`renderTimesheetLoadingCard` now has an optional `isExactNavigationError = false` argument and adds `role="alert"` only to the existing escaped message span when it is true (`main.js:367896–367914`). The existing fast-open details-error branch is the sole caller passing true, qualified by its retained exact physical-navigation flag (`367983`). Ordinary loading/evidence/legacy error callers keep the default and prior markup. No identity fallback, business state or retry action is added. The phone tab target selector additionally requires the existing `ctms-modern-modal` class (`css/bpay-next-office.css:251`), retaining the same native-child marker and 44px dimensions.

The Workbench and index raw hashes also changed with line-ending normalization. A pure text comparison confirms that normalizing their current text to CRLF reproduces the previously reviewed complete-file hashes (`2f90a466…` and `845a4610…` respectively); there is no new semantic change in either file at this boundary. The table binds the actual current saved bytes. **SOURCE PASS remains; whole WB-17 and its ordered future joins remain open.**
