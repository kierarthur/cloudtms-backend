# WB-21 Close, Escape and local intent review

10 October2026. Independent saved-source review of the new actual main modal pre-close seam, shared read-window adapter, Office Keep/Discard/beforeunload and Candidate-record handoff. Read the complete WB-21 card and current close/store design. No product changes, imports, tests, runtime, SQL, browser, business/data/network, hosted or fixture operations.

Inspected SHA-256:

| Saved FE source | SHA-256 |
|---|---|
| `js/main.js` | `e66e48eed68cd2c8db817bb0838df0cc126bedc8f147a50f952d93bf665acf1b` |
| `js/bpay-next-ui.js` | `652bf71860c8cbb2d1cb313d23dad2a801829f05e7e875d12725cfe7a6cf773c` |
| `js/bpay-next-office.js` | `e31bee995d3e76d584d0c917b779d46f75aaaf05298f32fb381bdafefb4fe2a8` |
| `js/bpay-next-workbench.js` | `047e6b7e16b1e4d0545ce7563fc4c24fa0927b10d1239b699aa81266fdd9ff46` |

## Open finite findings

### CL01 — lost owner/actor authority prevents even local dismissal

Adapter `ui:58` invokes the callback only when `active() && canFocus()`, and requires the same authority after it resolves. Office `:559` also returns false if actor or NEXT readiness changed. Office's current root `canFocus` is precisely that actor/NEXT predicate (`:717`). Consequently, after a mounted root becomes paused/unavailable, header Close, Escape (`main:337913–337918`) and `sheet.close()` all reach the pre-close seam and refuse to dismiss, even with no dirty choices. Office merely paints the paused notice (`:628,637`); it does not dismiss that root. The original page is trapped behind its own read window.

Allow a safe local dismissal when authority is lost, retaining every choice/journal and avoiding Discard or any record handoff. Keep the exact frame/token/stack guards. For a still-authorized dirty window retain the genuine question and generation checks; a lost-authority outcome is not permission to mutate another actor's intent. The post-close `requireActor` in the actual record handoff must remain.

### CL02 — memory-only Keep is presented as durable saving

Office `:563` always says `Keep saves your ticks for next time.` Yet the reviewed store deliberately reports `persistenceAvailable() === false` when storage is unavailable or fails (`workbench:693,699`). Keep preserves memory but cannot promise retention through page/browser reload. No current Office/Ready consumer checks that flag for the close presentation.

Use the actual affected stores' flag to explain current-tab-only retention when storage is unavailable; Keep should continue to preserve the bounded in-memory choices. No new storage framework or accepted-journal fallback is needed. This is a presentation correction on the real optional-storage path.

## Confirmed correct boundaries

The main hook is retained only for `bpay-next-read` (`main:327897`) and sits before any native close/pop, following the existing current-frame and confirming/closing guards (`:336532–336543`). It captures the original stack objects/token, blocks a second parent attempt, awaits strict true, catches failure and refuses changed ancestry. Escape already clicks the same token-bound header, so no second event or modal framework was added. Ordinary modal kinds preserve their existing behavior.

Office asks through a real standard child, captures every affected local store generation and resolves after that child is dismissed. Escape/no decision preserves the parent; Discard resets only local intent, while Keep preserves it. The new parent `close` requests the guarded read-window close and leaves all reader/shell cleanup to the actual `onDismiss` (`Office:573–574`). It no longer disposes the parent before an awaited answer. Candidate-record handoff awaits a true close result and rechecks actor/owner before opening the real record (`:528–537`). No bank tuple, money, server discard or replacement run is passed.

The financial controller/journal remains unchanged in this diff: exact pending bytes, accepted preparation identity and retry retention are not cleared by either local choice action. The beforeunload handler is the native synchronous browser warning, not an asynchronous custom modal. The finite256-namespace store bound has separately passed source review and does not evict dirty state or active handles.

**Verdict: CL01/CL02 open for this close/presentation slice; no whole WB-21 or runtime completion claimed.** Later submit/readback and actual UI acceptance remain their genuine implementation owners.

## Fresh closure review

Re-read the complete adapter and exact Office close/handoff blocks. Fresh hashes: UI **`05bfa93bbc6f37d7b72d77a642bff99bbe8e548495c6b79ad87e51f3a35f212b`**, Office **`6200f7acba83ad0847591c19b9fd5906a865c26a2b00732a1f976f6a3801c1c1`**. Main and Workbench hashes remain as listed above.

**CL01 closed.** The adapter now qualifies the exact active frame and unchanged original stack before/after its callback, without using lost business authority to prohibit local dismissal (`ui:58`). Office's lost actor/NEXT branch returns true before reading/discarding any intent (`:559`). That path preserves local choices and accepted/unknown journals. The normal authorized dirty path still requires original actor/readiness/generations after the confirmation before Discard; a mid-question authority change refuses that decision, and a later safe dismissal can retain it. The actual Candidate-record handoff still awaits successful dismissal and calls `requireActor` again before opening the record (`:535–537`).

**CL02 closed.** The question uses the actual affected stores' persistence flag: durable copy only when all are available; otherwise `Keep leaves your ticks in this tab.` (`Office:563`). Storage failure remains a guarded memory-only choice path, never a financial request dispatch fallback.

**Verdict: SOURCE PASS for the current local store/Close/Escape/Keep/Discard/beforeunload/handoff slice, no open concrete finding.** Actual capture/runtime acceptance and remaining WB-21 submission joins are not asserted. Read-only source review, no tests or product execution.

The owner's later actual SCR17 capture exposed three finite visual/accessibility issues (danger class, confirmation header/role/focus,36px phone targets). All are independently closed in the refreshed source/images, with the complete42-point A4 review in `screen-designs/SCR-17/review.md` (28 PASS/0 FAIL/14 N/A). Latest inspected Office `d2acc6113ce462f24db8347ca726023ccf06aa2b99d38c986adb5fb64066d79a`, UI `34e3286cc697c150542bb7f2ab61b5d6033e2cdfdc36272c3620d9b445a5dfd1`, CSS `a7b8101b73150360b21335e9f8079ff86f8da911d86f98545ff4789fd7f836ce`; main remains unchanged. The optional confirmation mode is confined to the existing actual adapter, restores its parent role/classes, focuses safe Keep, and retains original close/prefix/token behavior. SOURCE PASS remains for this slice; no whole WB21 or business runtime completion.
