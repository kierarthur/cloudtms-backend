# WB-11 standard Office frame investigation

10 October 2026. Read-only source investigation of the actual BUILD-FE standard modal and current WB-11 adapter. No product edit, application/browser invocation, SQL, business call, test or hosted execution. Root alone owns implementation. This note covers the shared frame seam; it is not whole WB-11 acceptance or rendered-screen evidence.

Read the full WB-11 card (`02_WORKBENCH.md:464–497`), all six deliverable rows, current scope investigation, and actual `main.js`, `bpay-next-ui.js`, `bpay-next-office.js` and `bpay-next-workbench.js`. Owner A3a supersedes the card's displayed official-date bullets: omit editable/shown pay date here, retain the server date only in the existing closed read/cursor context. No new date helper, route, session, projection or financial owner is needed for mounting.

## Minimum safe reuse

Use the existing `showModal` stack, standard header, body, actions and guarded Close. One hidden internal `form` tab avoids the record-specific `main` / `pay` / `rates` persistence branches. Supply `kind:'bpay-next-read'`, `frameEntity:'banking'`, `hasId:true`, `showSave:false`, `showApply:false`, `noParentGate:true`, `dirtyClosePolicy:'close'`, and the existing render/after-mount hooks. This is a read utility, not record Edit/Create. Retain each window's actual DOM, filter draft, controller, pending request bytes and exact initiating actor while a child is above it.

`main.js:327865–327926` creates the frame and mode; `332695–332719` retains the parent; `333070–333099` runs the async render with frame/token guards. `337098–337155` binds Close to the top frame's token, pops it and invokes its dismissal cleanup. `337181–337245` restores parent context/chrome. The raw exported `closeModal` at `351165–351223` has a different, incomplete return path and must not be the adapter's closing mechanism.

Current adapter correctly calls the actual `#btnCloseModal.click()` only when its exact captured frame is top. `active` means this frame is top; `alive` means it remains in the stack. Keep those separate: a parent needs to retain state while Filters/pickers are open, while child dispatch/focus must require its current frame and actor. Opening/Apply/Clear remain HEADER reads, never session or business creation. Existing Office controller/journal transport remains the monetary owner.

## Corrections required in the current draft

**SF-01 — preserve the screen's own control locks.** Adding `bpay-next-read` to the utility lists fixes record View gating, but the local `setFormReadOnly(root,false)` still sets every body button `disabled=false` (`main.js:327504–327508`) and clears input disabled/read-only state. Only the Weekly Source workspace is exempt at `327210–327216`. The new utility passes through this helper in `setTab`, `setFrameMode`, and the final `renderTop` pass. Current `bpay-next-office.js:551,558` and its render own pending/busy/eligibility disabling inside the retained body; generic record code can erase those locks. A later repaint/microtask does not give the screen permanent ownership. Exempt the exact `bpay-next-read` body from generic record read-only mutation while preserving existing screen control state. Do not switch to forceEdit or blanket-enable controls. Current Filters Apply and picker Choose live outside the body in `modalActions`; this particular helper does **not** itself enable those two footer controls.

**SF-02 — remove the exact owned footer on dismissal.** `bpay-next-ui.js:48–49` appends the caller footer to shared `modalActions`, but `dismissed` at line52 does not remove it. Another NEXT adapter mount removes it, whereas an unrelated restored record has no knowledge of `data-next-window-footer`. Remove the dismissed window's own footer before the parent is restored; parent mount may then reattach its own retained footer. Do not clear ordinary parent actions or invoke a second raw stack pop.

## Lifetime and return details to retain

* Both `runOnRender` and `runAfterMount` can invoke the same mount hook for one render. Reattachment must be idempotent. Defer UI painting until the captured frame is current, and keep initial read/controller setup once-only; current Office `started` at `694–696` does this. No mount callback may retry a business request.
* Hide only the internal tab row of the current read frame. Current dismissed resets `hidden`/display and the small-window class. A normal non-adapter child must get its own visible chrome; persistent changes to the shared tabs/actions must not leak into unrelated frames.
* Parent focus restoration should follow actual parent reattachment/current-frame identity and initiating actor. Current child restoration (`bpay-next-workbench.js:595–598`) uses a zero-delay timer and checks connection/actor; that timer is not a general proof of async parent completion. Prefer a retained pending focus target applied by the parent's real mount/continuity callback, or the existing narrowly scoped actual DOM/chrome observer pattern. Never focus a detached/stale frame or steal focus after the actor changes.
* The root's close sequence must dismiss only its owned children in stack order before the root. An inactive adapter close is intentionally a no-op, so do not invalidate a retained parent while a foreign child remains on top.
* Retain the ordinary modal's Escape/Close and keyboard behavior. The adapter must not introduce another body overlay, generic save handler, editable record mode, independent modal stack, automatic POST or automatic financial retry.

The initial mode/parent-gating and raw-close concerns are corrected in the current saved draft: the utility is explicitly recognized and the adapter closes through the real guarded button. SF-01 and SF-02 remained open at this read. Root may change source concurrently; re-read immediately before a closing verdict.

## Source boundary

Observed source hashes (SHA256, saved disk):

| BUILD-FE source | SHA256 |
|---|---|
| `js/main.js` | `2980caa0785222d67daa5759b2d4c4de931ff94afe92868ec070016e4f625ba6` |
| `js/bpay-next-ui.js` | `aa7615bd756c886c52baef9aa0e53c0b5c2a47c6f7c3471503552388c268f5a5` |
| `js/bpay-next-office.js` | `980055953f2be0301f10b445746979a3782ff67d9058270dd40f33860ef2bb57` |
| `js/bpay-next-workbench.js` | `0a9279221020503a8391c7fe2cb317c12df1f451833f78867e7f40156ab25e4a` |

No source-gate, unit, keyboard, DOM or screenshot result was executed by this investigator. Existing source contracts and future screenshot/joined acceptance remain distinct from this bounded design investigation.

## Fresh adapter correction review

Re-read the saved draft after root's changes. **SF-01 closed:** the local `setFormReadOnly` now immediately returns for the exact current `bpay-next-read` kind (`main.js:327065–327068`), preserving screen-owned loading/eligibility/request locks. **SF-02 closed:** adapter `dismissed` removes its exact owned footer (`bpay-next-ui.js:52`) before parent restoration. The seven read-utility classifications, guarded header Close, exact active/alive distinction and idempotent DOM reattachment remain. New read frames center once with `_nextReadCentered`; subsequent return/drag uses the existing anchor, without changing money/cursor authority.

**SF-03 — finite keyboard support still required at this read.** Shared Tab containment checks only `isRecordOwnedFrame(top,parent)` (`main.js:337911`), whose entity set is Candidate/Client/Contract (`325665–325668`). A standalone Banking root and Banking Filters/pickers do not qualify, and neither the new adapter nor Office installs the former local focus trap. Add the exact read kind to that existing keyboard predicate, preserving the top-frame identity check; do not broaden record validation/dirty behavior. Initial root focus is likewise absent: the record initial-focus helper excludes Banking, root onMount only renders/starts reads, and Office continuity with a null token does not focus. Give a newly active read frame one visible initial focus/panel focus; ordinary reattachment must preserve focus. The existing modal HTML already has dialog semantics and tabindex -1.

The earlier zero-timer point is a design limitation, **not a demonstrated actor or frame escape** in the finite current read-parent path. Child dismissal schedules focus only if the original node is actually reconnected and the root/current actor still qualifies. The retained Banking parent's mount synchronously reattaches its DOM and its promise/mount microtasks run before this timer; disconnected nodes are skipped. Do not infer support for arbitrary asynchronously rebuilt unrelated records, but no extra lifetime protocol is required for these concrete read windows. Root's Close continues through real token binding and cleanup.

Hashes at this reread: main `75cc6ad369cb7a41231b1e0e200759495652a8e1508ce9e4e482c6cd5e405200`; UI `8ed3df981b2d100a6c559b6388027917acb62fce8ef504637283783d3df7e857`; Office `980055953f2be0301f10b445746979a3782ff67d9058270dd40f33860ef2bb57`; Workbench `5414057202ef8e2da15e400c2c919f0f3f0f0114c2f1e44db359c56c99aa2f51`. Source-only frame review remains conditional on SF-03; rendered/keyboard acceptance has not been run.

Final follow-up: root corrected SF-03 with exact top-frame Tab containment and once-only actor/owner-qualified initial focus. Independent fresh source review found SF-01/02/03 closed and no remaining concrete defect in the bounded frame seam. Final hashes and scope are recorded in `../reviews/WB-11-standard-frame-review.md`; rendered acceptance remains unexecuted by this investigator.
