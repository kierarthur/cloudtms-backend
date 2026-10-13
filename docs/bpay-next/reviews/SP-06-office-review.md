# SP-06 Office source review

Current verdict: **PASS — D04/D05 source review; H01/H02/H03 closed**. Read-only review of the current D04/D05 batch on 10 October 2026 against the full SP-06 card (05:248–276), all five checklist objects and `design/SP-06-office-census.md`. No product source, tests, SQL, runtime, fixtures or hosted actions were executed or changed by this reviewer. Joined application acceptance remains NOT_RUN. D01–D03 are separately reviewed. All historical OPEN readbacks are superseded by the “Final source closure” section and its final hashes.

## Historical initial findings (closed)

**H01 — The workspace still retries automatically with different literal inputs.** `js/weekly-source/workspace-actions.js::saveProtectedOnce` retains its previous implementation: after an unknown first POST it immediately sends `{...payload,recover_unknown_outcome:true}`; `C1_DURABLE_RECOVERY_NOT_REQUIRED` can cause another original POST in that same click. A subsequent click also begins with the changed recovery payload. The new “Retry exact request” labels do not change this transport behavior. SRQ-028 requires a deliberate identical retry; SRQ-029 requires that Save does not depend on the missing C1 start/status route. The finite correction is one awaited `issueCommand(pending.action,pending.payload)` per user click, retaining that object and original key on uncertainty. Do not replace it with another recovery protocol.

**H02 — The new confirmation helper accepts incomplete/mismatched receipts as confirmed.** `weekly-source-presentation-v1.js::protectedActionConfirmation` currently accepts a NEXT result missing its required agency sequence, family bound version and approved target amount, and does not enforce the real exact 14 keys. WAIT accepts arbitrary extra keys and unsafe numeric versions through `String(v)`; the actual source has precise fresh six-field and replay five-field forms. The optional SOURCE_LOCAL branch checks only outer key count and a few receipt fields, then recursively accepts the nested result without checking the actual 11-field receipt's request/publication/generation ties, replay flag, first-authorisation and state/result agreement, or published/banking-receipt consistency. Consequently a truncated NEXT reply or a COMPLETE envelope containing an inconsistent PENDING result clears pending and closes the actual editor. Validate the existing finite Worker contracts, including positive int64 text and exact decimal text; numeric WAIT versions must be safe positive integers. LOCAL flat results must still allow the actual additional publication/deferred-release fields. Do not require financial application/APPLIED to confirm a valid local receipt.

## Correct reuse observed

The main approved-hours modal now retains the actual frozen `{endpoint,body}` command before its first POST, reuses it on retry and disables mutable controls/other actions after an unknown outcome. It no longer generates a new key for that retained request. Both actual workspace editors call the result helper before clearing their pending object or closing, so correcting the shared finite parser reaches those genuine callers. The labels for pending approve/amend/wait/reconcile now say “Retry exact request”. Confirmed save wording preserves first-authorisation, pending freeze and NEXT publication as separate from payment processing.

The named edits stay in existing editor/presentation/controllers. They introduce no command, Stage, money calculation, financial application claim, SQL or producer change. Existing parent refresh remains the parent hook; SP-35 owns later targeted refresh work. Before a final source PASS, reread the corrected helper/controller bytes and the changed asset versions. Full request/actor/frame lifetime behavior is still an application acceptance item; a late completion must never close or refresh a replacement actor's frame.

## Reviewed initial source hashes

| Source | SHA-256 |
|---|---|
| weekly-source-presentation-v1.js | f70d8e42cce9cb69ad8d20e5905c773bd53cfe78a27c0049a619239ba46f1f5d |
| weekly-source/protected-shift-editor.js | a9725b6713da4ee875f67c7b48d9a5509ad9c0b54589949d97f761662a26d81e |
| weekly-source/workspace-actions.js | 0fdac50a2bd5c75ff6ec0ebff3f4c67079fed21f36fc582752e1fb27f06b624a |
| main.js | 0bf4a8167761d8302fb4beca64252b26af56292bb2f7142d4e3602d34236548f |
| index.html | 90d08e1c63ded349e92eb1853fc1fad613722c75b909203d92729be83f85c169 |

## Final source closure

Fresh final reread closes H03. Main captures the actual parent before opening and the actual child immediately after `showModal`; deferred wiring checks that bound child before attaching handlers. Its real click, awaited Save result, refusal handling and close effects require the same initiating actor and child. Deferred parent refresh requires the original parent and actor before/after its await, including its rejection/notice branch. The workspace captures each actual protected child once after `openChild`, uses the bound actor/frame in existing context load/reload, wire, submit, prepare/save success, catches and final repaint, and checks again after the SourceChanged context await. Its delayed overlap recheck also requires the original frame. Thus old results cannot bind a replacement same-kind dialog or direct their completion refresh to a replacement Timesheet.

The final literal retry remains one original POST per deliberate click. Valid flat LOCAL initial/deferred receipts, exact NEXT and fresh/replay WAIT results clear pending and close through the existing parent hooks; published/application readiness is not a closure prerequisite. Unknown or unrecognized results preserve the actual original command/key/inputs, with locked edits and the deliberate retry label. Actual typed local-owner HTTP and returned refusals receive the plain copy in both workspace editors and the main modal. No new command, Stage, calculator, owner fallback, financial application label, SQL producer, global modal rewrite or SP-35 targeted-refresh implementation was introduced by these screen changes. The four changed script asset markers are present.

No actionable source finding remains for D04/D05 at these final bytes. This PASS proves the bounded source implementation, not installed UI behavior, Worker/queue/stored financial positions, or joined application acceptance; those remain NOT_RUN and D01–D03 retain their separate reviews.

| Final source | SHA-256 |
|---|---|
| main.js | 3a51abe4360398ad21136c54587c89e1ee8c0f3197ecc4bef763208b61c38a27 |
| weekly-source/workspace-actions.js | 4c86b1b39870169db9e5cbbf3d9fe5f9d8795d8d68e13a86fbc296302bfb0b17 |
| weekly-source-presentation-v1.js | 8a0064242756964f9accd7cb9ab2245badbe2fa0ea7abae7ac3bf185a2d0d9b1 |
| weekly-source/protected-shift-editor.js | a9725b6713da4ee875f67c7b48d9a5509ad9c0b54589949d97f761662a26d81e |
| index.html | 90d08e1c63ded349e92eb1853fc1fad613722c75b909203d92729be83f85c169 |

## Historical H03 first guard readback (superseded)

The corrected callbacks now refuse dispatch/late Save results for a different actor, compare the exact per-request workspace frame after prepare/save, and guard workspace catches/finally. Main guards success/refusal/close against its captured actor/frame. Both workspace HTTP refusal catches now choose the plain local-owner wording from the exact typed code, independently of a raw server message. These are correctly bounded changes.

H03 is not yet fully closed at this readback:

* Main captures `initiatingFrame` in the deferred `wire()` callback. An old scheduled wire can instead find a newly opened same-kind dialog and capture its frame. Capture the actual child immediately after `showModal`, then require that identity in `wire` before binding its DOM.
* Main's deferred `refreshOwner` checks actor only. `refreshSimpleWeeklySourcePresentation` obtains its Timesheet from the current global `modalCtx`, so a same-actor replacement between child close and timer can redirect the original completion's refresh to another Timesheet. Capture the genuine original parent frame and check it before the deferred parent refresh and its late UI effect.
* Workspace's known SourceChanged catch invokes `load()` after a guard, but `load` awaits context and unconditionally rerenders whichever same-kind child is now current. The protected shift's context reload has the same lifetime seam. Bind the actually opened protected frame once, use it in its existing wire/context read callbacks, and retain the per-request guards. Do not extend this correction to unrelated workspace actions or shared UI machinery.

Guard readback hashes: workspace-actions.js `15656b2503bb106587ec112a9cd2e7c60ac1a36cb8439f53dfd9b4de4029bea1`; main.js `49ba80a14f504ceac1d25f240de66addc9907671b66cd39424fad871fcc00d80`. H01/H02 remain closed at the fresh helper hash already recorded. No execution was performed.

## Historical correction review (superseded)

H01 CLOSED: `saveProtectedOnce` now contains one awaited POST of the retained `pending.action` and `pending.payload`, with no recovery mutation, implicit second call or owner fallback. The editors reuse that pending object; main reuses its original frozen command instead of rebuilding a key or expected version. Unknown responses keep those exact inputs and expose the deliberate retry.

H02 CLOSED: the pure helper now recognizes only actual flat LOCAL 11/12/13, mapped NEXT 14 and fresh/replay WAIT 6/5 keys. It checks genuine authority/state/first-authorisation ties, native IDs, safe numeric versions or positive int64 text, and exact NEXT decimal text. The 13-field branch includes the genuine deferred-release receipt ID. The unused SOURCE_LOCAL status decoder was removed under BD044: the actual Save Worker validates the closed status/local/banking envelope and returns the genuine flat LOCAL `.result`; no current Save caller consumes the envelope. This satisfies accepted SOURCE_LOCAL completion without duplicating a Banking proof decoder.

The extra LOCAL `publication.ok === true` requirement is valid. `04102026_1257:380–390` only preserves an immediate publisher success; `17092026_0300:3096–3103` emits an actual boolean `ok` for deferred `published:false`; `04102026_1257:515–519` emits `ok:true` for genuine deferred release. None of these checks requires Banking application or published=true. Authorised pending freeze and unauthorised TSFIN PENDING_AUTH retain distinct wording.

**H03 — Late completion is not bound to the initiating actor and exact frame.** Main's protected-hours callback can await a save and then invoke global `closeCurrentModalFrameSafely({expectedKind:...})`, falling back to clicking the current global Close button. The workspace callback calls `finishAction`, which marks and closes the current top weekly-source child rather than a captured initiating frame. Neither protected callback captures and checks the initiating actor/exact frame before its late success, catch and repaint effects. A logout or replacement frame during a pending request can therefore let an old completion close or mutate another frame; main's expected-kind check alone does not bind identity and its fallback bypasses even that. Add only the finite captured actor/frame check around these actual protected calls and late UI effects, including the retry admission and deferred parent refresh. No global modal rewrite or command protocol is needed. Ordinary end-to-end lifetime acceptance remains NOT_RUN even after source closure.

Fresh receipt-parser/controller hashes after H01/H02 corrections:

| Source | SHA-256 |
|---|---|
| weekly-source-presentation-v1.js | 8a0064242756964f9accd7cb9ab2245badbe2fa0ea7abae7ac3bf185a2d0d9b1 |
| weekly-source/workspace-actions.js | fadc2ad44c93d38df275dab774cb577755a1b779409939a094395044d23b1fd9 |
| weekly-source/protected-shift-editor.js | a9725b6713da4ee875f67c7b48d9a5509ad9c0b54589949d97f761662a26d81e |
| main.js | 0bf4a8167761d8302fb4beca64252b26af56292bb2f7142d4e3602d34236548f |
| index.html | 90d08e1c63ded349e92eb1853fc1fad613722c75b909203d92729be83f85c169 |
