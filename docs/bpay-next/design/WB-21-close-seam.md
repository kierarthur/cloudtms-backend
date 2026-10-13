# WB-21 finite pre-close seam

Read-only saved-source investigation, 10 October 2026. No product/browser/runtime/test changes or execution. Exact inspected sources are BUILD-FE `js/main.js`, `js/bpay-next-ui.js` and `js/bpay-next-office.js`.

## Actual path and existing guards

- `showModal` creates a stable frame object with `_token` at main327866–327904. It retains optional `_onDismiss`; no pre-close callback exists.
- `bindClose` main337126–337130 sets `btnCloseModal.dataset.ownerToken=frame._token` and the shared `onCloseClick`. The latter337104–337122 refuses missing or mismatched current-frame tokens before calling `handleSecondary`.
- `handleSecondary` main336531 starts with `currentFrame()===top` and refuses `_confirmingDiscard`/`_closing`. Existing utility, bulk, dirty edit/create and financial branches must retain their present order and behavior.
- Escape main337902–337908 checks that the key listener owns the current frame, refuses confirming/closing, then clicks that same token-bound header Close. It already joins the required path; do not add another Escape listener or close implementation.
- The final native tail337061–337101 sets closing, sanitizes geometry, pops one frame, calls `cleanupClosingFrame` and resumes its parent. Cleanup337132 invokes `_onDismiss`, so it is necessarily too late to ask Keep/Discard.
- Adapter ui31–63 retains the real frame and uses the actual header Close; small child windows share the same shell. Its `onDismiss` removes the footer and calls the owner cleanup. `dirtyClosePolicy:'close'` does not implement the required question.

## Smallest optional support

1. Add optional `beforeClose` to the existing adapter input and pass it to `showModal` as a narrowly wrapped callback. In frame construction retain it **only** when `opts.kind==='bpay-next-read'` and it is a function, e.g. `_beforeClose`. Other kinds get no hook and continue untouched.
2. In `handleSecondary`, immediately after its existing current-frame/confirming/closing guards and before any close/pop/dirty policy, add one `top.kind==='bpay-next-read' && typeof top._beforeClose==='function'` branch.
3. Capture the actual frame/token plus current stack prefix/depth. Set the existing `_confirmingDiscard` flag before invoking/awaiting the callback; this suppresses a second parent Close/Escape while the confirmation child is on top. Child frames use their own guards and still close normally. Do not disable the shared header DOM globally, since it is rebound to the confirmation child.
4. Catch callback errors and treat every result except strict `true` as refusal to close. Always clear this frame's pending flag in `finally`. After resolution require the original frame to be current, its token unchanged and its original stack prefix restored. A child still open, removed/replaced original frame or changed ancestry means return without popping anything.
5. If allowed, fall through to the existing native tail once. Do not recursively click Close, call `closeModal`, replay the handler, set a persistent “skip confirmation” flag, or pop inside the callback. The existing tail handles original cleanup/parent resume. There is no new frame engine.

The stack check should compare frame objects/tokens, not an old `_renderToken` or the reused header node's previous token. Opening and closing a normal confirmation legitimately rerenders the original parent and rebinds its shared header; that should not invalidate a genuine allowed choice. A new child or actor replacement must invalidate it.

## Adapter and Office responsibility

The adapter wrapper captures its own original frame and can require `active()` and the supplied `canFocus()`/current-authority predicate before and after the owner's callback. The owner also checks its initiating actor, NEXT/environment authority, exact original local namespace and intent generation before opening the question and after its await. No authority failure clears an earlier accepted/unknown journal.

The owner callback shows the supplied SCR17 through the standard modal as a child: `Discard your unsaved selection?` / `Keep saves your ticks for the next Draft.` with Keep first and Discard danger. Resolve only after that exact confirmation child is dismissed/restored. Keep persists that unsubmitted intent; Discard clears only it. Dismissal/Escape/no choice returns `false`. With no dirty unsubmitted intent return `true`. No action may clear pending accepted request bytes, create a run, issue another command or imply cancellation. If optional storage fails, preserve guarded in-memory intent as specified; do not use a close decision to discard it silently.

**Required caller correction:** Office556 currently executes `issueWindow?.close(); workbenchShell?.close(); readWindow?.close(); dismiss();`. That bypasses an awaited guard by disposing the parent immediately. Its parent-close path must request `readWindow.close()` and let only the successful native `_onDismiss` run `dismiss()` and reader/shell cleanup. An actual child must close through its own frame first; do not run parent disposal while a child owns the shared header. Header Close and Escape already reach the same beforeClose hook. Existing callbacks that deliberately open another native record after requesting Office Close must not assume an awaited close succeeded; they require the actual original-frame-dismissed result before their handoff, or remain on the existing safe path until that finite caller is wired.

`closeModal()` main351176 is an immediate low-level stack pop, separate from the token/header policy path. This proposal does not globally wrap it or change unrelated callers. The actual WB21 adapter/Office close should use the standard guarded header route; never use that low-level function to force the answer. Browser beforeunload is a native synchronous warning and cannot await this modal.

## Review boundaries

Source review should confirm one question per close attempt, strict allow result, no parent pop when confirmation child remains, original actor/namespace/frame preservation, exactly one native cleanup and parent restore, and identical behavior for all non-hook modal kinds. No runtime or scenario is claimed here; A4 actual product screenshots and the required ordinary source verification belong to the implementation owner.

Inspected SHA256: main `cd8cd5077d61d5d48182904fbd7c5b4b35efa4df1177994101d6c63f0cbb583f`; ui `dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272`; Office `73239f720c497fd17daea4e09cfbe2fead8a0a0443d0067796c2ad7e8a1344f1`.
