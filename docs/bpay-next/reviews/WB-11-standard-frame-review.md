# WB-11 standard frame source review

10 October 2026. **SOURCE PASS for the bounded standard-frame integration. No open concrete finding in this reviewed seam.** This does not mark all WB-11 deliverables, rendered screenshots, keyboard interaction, joined application or installation acceptance complete.

Independent read-only review of saved BUILD-FE source, the actual shared `showModal`/Close paths, full WB-11 card and six checklist deliverables, and `design/WB-11-standard-frame-investigation.md`. No product edit, source-gate/test execution, browser/runtime, SQL, hosted or business call. Root alone made the source changes.

## Finding closures

| Finding | Final source evidence | Result |
|---|---|---|
| SF-01 record code erased screen-owned disabled controls | `main.js:327065–327068` returns from the local read-only helper for the exact current `bpay-next-read` kind. Existing Office loading/pending/eligibility flags remain owned by its controller/render. The seven utility recognitions avoid record View/parent-edit gating without putting Banking into Edit/Create. | CLOSED |
| SF-02 custom footer survived dismissal | `bpay-next-ui.js:52` removes only the dismissed frame's supplied footer before ordinary shared cleanup and parent restoration. Parent reattachment can restore its own retained footer. | CLOSED |
| SF-03 standalone Banking lacked Tab containment and initial focus | `main.js:337904–337929` first requires the actual current frame to equal the listener's frame, then permits the exact read kind alongside existing record-owned frames for the shared Tab trap. Escape invokes the real guarded Close. `bpay-next-ui.js:33–50` performs initial focus once, after onMount, only for an active frame passing canFocus; it chooses a visible enabled content control or real header Close. | CLOSED |

The caller supplies actual initiating-actor and current-owner checks: root `bpay-next-office.js:694–696`; child `bpay-next-workbench.js:591–598`. The child first-focus flag prevents later reattachment from replacing the restored parent focus. Banking was not added to record validation/dirty classifications.

## Preserved framework and authority

The adapter uses the existing stack and one hidden internal `form` tab, with Banking entity, exact read utility kind, noParentGate, and explicit no Save/Apply. It retains the same DOM and distinguishes top-frame `active` from in-stack `alive`; mount requires exact frame identity. The render and after-mount hooks may run twice, so reattachment is idempotent and root initial reads retain the existing `started` guard. Mount starts only existing read/capability work, never a business mutation/retry.

Adapter close requires its frame to be top and clicks the real `#btnCloseModal`, whose token binding/cleanup/parent restoration remain the shared implementation. It does not call raw `closeModal` or pop the stack itself. Header Close, Escape, parent cleanup and tab/geometry restoration retain the actual framework. `main.js:333172–333178` centers each new read frame once; later return retains the saved anchor/drag placement.

The zero-delay child return callback is not treated as general async-layout proof. For these finite retained Banking parents it focuses only the original actually reconnected node while the initiating actor/current owner still qualifies. Parent read mount synchronously reattaches retained DOM; the mount microtasks complete before the timer. Disconnected nodes are skipped. No concrete actor/frame escape was found, and no new lifetime protocol is needed for this current path. Arbitrary unrelated async record reconstruction is outside this claim.

Current filter results may update their retained live parent while its picker is top; picker paging and focus checks require the active window. Request generation, exact actor, cursor context and closed reader validation remain in the existing transport. Apply/Clear alter read scope and issue-list reads; there is no session open/create/rebuild or automatic financial request. Financial controller/journal bytes and existing money owners remain separate from this mounting support. A3a hides the displayed/editable pay date while the actual server date remains in the existing closed read/cursor context.

## Narrow modernisation support

Read the complete `modal-modernisation.js` diff. The exact read kind avoids a conflicting generic layout. Empty live-copy placeholders are retained only when marked `data-next-live-copy`, preventing the existing empty-node cleanup from removing their later error/status target. Preformatted text is excluded from generic currency rewriting only under `data-next-formatted-money`; the current producers supply existing UK-formatted display text, not wire/request amounts. No whole-window exemption, transport conversion, financial calculation or unknown-to-zero substitution was added by these three changes.

## Reviewed saved hashes

| BUILD-FE source | SHA256 |
|---|---|
| `js/main.js` | `9a13516c5f0d9eb1d184db13088e7249d302c27a6083ff47803f0b82f1d29767` |
| `js/bpay-next-ui.js` | `2b741e79dab714de2b7b1648fdc63d5b7526bb6aa3a6922459e658e470c3918a` |
| `js/bpay-next-office.js` | `34ec7991ec930be29f260b71522ab7d01c370f7430742e09b9755a972769efff` |
| `js/bpay-next-workbench.js` | `87eede7806bf7291549ec4056692f3fe02e5dca479c3b2b112f0309867ad3587` |
| `js/modal-modernisation.js` | `9bf06878d57960ae82e1967e71452ceadec0cf3a603c946effc1df49d88df36e` |

Source PASS binds these saved bytes and this narrow frame scope. Actual rendered screen/42-point review, phone geometry, live keyboard behavior and later owned UI work remain their own acceptance evidence; none was executed by this reviewer.

## Final saved-source refresh

Re-read current diff and saved frame/caller source after the final wide/small classes, picker hint/error copy, cache markers and main-file EOL preservation. **SOURCE PASS retained.** The adapter now toggles exactly one wide/small class for its active frame and removes both on dismissal; the root/child widths use those exact scoped selectors. The control-preservation, exact current-frame keyboard guard, once-only qualified initial focus, retained first-focus flag, owned footer removal and guarded shared Close remain. Picker error handling now retains the stale-context distinction and presents Refresh versus ordinary retry without automatic dispatch. Narrow live-copy and already-formatted text markers retain the same reviewed semantics. Static dependency order is preserved; cache query markers change no business owner. No new concrete frame defect found.

Final snapshot SHA256: main `7cde52d8f113f9c35f19f2f0be198390e2d742118c991b98e29ca06dcc645845`; UI `dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272`; Office `34ec7991ec930be29f260b71522ab7d01c370f7430742e09b9755a972769efff`; Workbench `2208826185c9947f0d010e806e4c98ef5095be4b042b818ac88a123462e788af`; modernisation `9bf06878d57960ae82e1967e71452ceadec0cf3a603c946effc1df49d88df36e`. Related source identity only: CSS `52203a80b9dbc464d3d87f7d02cdfe3e4d6ce3f2c59fa253e96dd66dad85e145`; index `d67ad31b78c1f66de0bd9ece500f64d1496e2c2810179fd4c88f4ce7910d9f91`. This refresh still makes no rendered or whole-card acceptance claim.
