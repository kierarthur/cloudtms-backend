# FC-09 Worker and shared Office source review

11 October 2026. **Worker/shared/native SOURCE PASS** after the final amendments below. Earlier OPEN findings are retained as review history and explicitly superseded by the final closure. This receipt covers closed transport, shared journal/footer, native form/main integration and exact Draft navigation, not A4 or the whole FC09 item. No product edits, SQL/DB calls, tests, fixtures, hosted operations, render or screenshots were performed. Runtime/application acceptance remains NOT_RUN.

Authority: full FC09 card1196–1215, all eleven current checklist rows3158–3186, BD119 and [independent design review](FC-09-design-review.md). SQL was read only to match emitted typed contracts; the separate SQL reviewer owns its complete source verdict.

## Actionable findings

1. **Required Draft identification is missing in the displayed blocked pill.** Current FE Office `paintCaseEditCards` at1974 renders only “Batch blocked: PAYE Draft”/“Umbrella Draft”. The nested protection has native run_id but no user-facing identifying reference. Card1203/1208/1213 requires the protected run reference; supplied SCR122b identifies its Draft. Carry/display only a factual native identifier or exact native navigation which identifies that run. Do not invent an official pre-execution pay date or display a guessed reference. This is a narrow card-required reader/presentation operand, not a new financial owner.

2. **New stale-state copy has two sentences.** Shared Preview `message` at132 says “This Case has changed. Refresh and try again.” OwnerA3g requires one sentence; use the same words joined by a semicolon. A4 will separately verify actual rendering/roles/phone controls.

Both findings were sent to root immediately. Native form/main integration and its final actor/parent/owner/Close/remount behaviour remain pending independent source review. No SOURCE PASS is claimed for that unfinished layer.

## Worker closed contracts

CASE_EDIT has an exact closed input consisting of command/Case/positive expected revision, the existing fourteen creation fields and edit token. Shared token validates exact input/context members, native actor/owner/FVR/channel/default floor/policy plus Case/rule versions and the requested Case/revision. All positive counters are bounded bigint strings; money/date/text validation reuses original creation controls. Full command limit49152, token32768 and input12288 are finite; CASE_CREATE32768/other commands2048 remain unchanged. The outer streaming reader now admits the edit maximum, then action-specific checks still enforce the old limits. No automatic retry or browser financial derivation is added.

Intake receipt is bound to requested Case, original command/new rule and expected Case revision+1. Native `case_edit_receipt_v1` carries that same intended revision/rule even for BLOCKED; its phase and finite issue_code distinguish non-applied outcomes. The shared reader validator preserves this distinction instead of asserting a blocked revision actually became current.

CASE_PREVIEW admits exactly both edit operands or neither. CASE_COMPONENTS admits optional exact edit_command_id through its closed existing page variant. Required nested edit_state/edit_receipt fields are validated, Case/Candidate identities and requested edit command agree, extra/missing members fail, and pagination/bounds remain native. Preview requires exact entered-input echo and Case/revision token equality. Original creation Preview and row contracts remain supported.

Financial single-step CLAIM/STEP, RPC and runtime/queue kind maps all add only CASE_EDIT. Claim accepts native NEW/null cursor; STEP accepts only terminal CASE_EDITED/BLOCKED with finite issue_code qualification, DONE and released lease. Existing ID/kind delivery and fresh native claim own continuation; there is no active-lease adoption or new message protocol. Native service validates the command/request/result tuple before publishing the shaped result.

Named edit refusals are scoped to CASE_EDIT intake or CASE_PREVIEW rather than broad generic relabelling. Existing actor/admin checks,8-second Worker deadline, streaming response ceiling and unknown-result semantics remain. Queue maps do not alter unrelated financial phases.

## Shared Office journal and footer

Version6 adds one nullable accepted case_edit receipt. Exact version2→3→4→5→6 migrations exclude only the known newly introduced fields; old pending bytes survive untouched and are recanonicalized using their original action. No old request identity or token is replaced. Accepted edit receipt is bound to stored Case and command/new-rule ID. Pending first dispatch versus previously unknown retry retains the original distinction: only a proven initial refusal can clear pending; a later refusal cannot erase a previously uncertain command.

The Case-qualified footer chooses CASE_EDIT explicitly, checks pending Candidate/Case/original expected revision, and never dispatches fresh Preview while pending/accepted. Targeted readback uses original edit_command_id. It requires complete bounded page, uniform receipt, exact accepted command/rule/revision; PENDING stays Refresh, BLOCKED is handled as blocked and never success, CASE_EDITED requires exact current Case/rule/applied command before success. Finalize clears only the acknowledged Case edit. New controller mutations are barred while the accepted edit remains unresolved.

The newly saved native forms now restore editContext from pending original expected_case_revision, or accepted receipt revision−1, before current reader revision. This is the required recovery direction when a lost acceptance already changed the server. Full integration review will verify those forms independently; this receipt does not substitute for it.

Parent/actor/NEXT checks still surround transport awaits and the footer's current predicate. Successful/blocked callbacks and final native form remount/closure will be re-read at integration completion. Shared pure shape validators match BE/FE additions; no stored journal migration obtains fresh financial inputs.

## Reviewed current bytes

| File | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-case-contract.js | 4652c0dfdfcedf4b38e895729173b555e7ebb3379d9816ed96319e7be6a76a6e |
| BE broker/src/bpay-next-command.js | 48bf0258b8d20e58138a463041ebb857bbc827699e5a2f234769781e6cd75b53 |
| BE broker/src/bpay-next-preparation.js | dce1444b7ea1cca30443a3949db4b80d61a3c63e2cc723c5f7316f3937cf1540 |
| BE broker/src/bpay-next-job-step.js | 2b56864e8f1b9899826af41a85feeff9f527b016bbd37eab784da0980b663894 |
| BE broker/src/bpay-next-command-rpc.js | 75ed9501ef5173c19bcccbee7249b6c747a4d9eb6a544c69e2103219760c7fce |
| BE broker/src/bpay-next-job-rpc.js | 2f54d867de172c1ff99e5c9ba8a0ee7931b8ae5ebf2588c04c429f02a32ed8a1 |
| BE broker/src/bpay-next-runtime-rpc.js | ab1f2927876eac6455fe94ac875a5963cbef8674ffcd386e745333fa1f5c3a3f |
| BE broker/src/bpay-next-financial-queue.js | 19e30182452b671a8237d5f6b30c09210c07635dc8bf994c9c154388e4eb1d79 |
| SQL1939 (contract match only) | 152c665e631aed507dfcfa4b84045e9521ed9d73eb524b5d153efe5bfebf53ee |
| FE js/bpay-next-office.js | 2403fe59b1a99ad058a9143aaa2591f3c3807c1f65536b97db6f255fd32546fd |
| FE js/bpay-next-case-preview.js | d1e4faed63d4ac16fae19df30d3bfb9a67b47eb31b9237b99c04245f76d050ca |

Hashes are the saved review snapshot while root continues integration; later product bytes need a final amendment. Item/installation/A4/runtime boxes are not changed.

## Native integration review checkpoint

The one-sentence stale copy is repaired. An exact native Open Draft action now uses the protected run/worker/channel rather than inventing a reference or pre-execution date. Its confirmed-Draft path reads CHANNEL_DRAFTS and CHANNEL_STATUS and seeds the existing run-qualified controller so Office skips REVIEW. This closes the original identification approach in principle; final modal/target handling remains under review.

**OPEN: blocked-edit footer remount races journal acknowledgement.** In each native Advance/Credit/Debt form, onEditBlocked queues mount() with queueMicrotask. Office's onBlocked awaits that async callback before finishCaseEdit. The queued mount therefore runs before the accepted case_edit journal is cleared, builds another accepted/Refresh footer, and then the old wrapper clears its journal behind it. Source anchors at this checkpoint: Office2007; Advance108; Credit89; Debt116. Remount only after acknowledgement, or clear the acknowledged receipt before invoking the remount callback. Root was notified immediately. No runtime execution was performed.

The native forms qualify current actor, NEXT epoch, original modal-stack prefix and parent context, use the existing CASE_COMPONENTS point reader, and restore the original pending/accepted edit revision before fresh reader context. Payment activity locks the full form, including Note, with no unsupported note-only command. The old bank-details helper/action removed by main had no remaining caller; separately owned ordinary Candidate bank changes are not altered. Final source verdict remains pending this concrete recovery fix and the Draft target check; Worker SOURCE PASS is retained.

### Fresh source amendment: remount fixed, two remaining recovery findings

Office now calls finishCaseEdit before onEditBlocked. The old race above is superseded and closed: all three queued form remounts now observe the acknowledged journal. The native Open Draft callbacks also catch reader/navigation errors inline. Stale-state wording is one sentence.

1. **OPEN: existing Office sheet must retain exact target and caches.** The new adapter uses sheet.controller, binds the requested channel/worker and focuses the sheet. Same-run different-worker/channel rebinding does not clear or reload Office's cached status/lines; render can show the former worker's monetary figures against the newly bound action target. Existing Office.open returns its singleton without refresh. Focus only an exact actor/run/channel/worker sheet, refusing other active targets before any binds, or invoke an existing guarded navigation seam that clears caches and reloads the target. Anchors: Office1681,1840–1850,1986–1990.

2. **OPEN: later live edits must not strand an earlier completed edit journal.** Shared Preview151–154 requires live current Case/rule/applied command to equal the accepted original edit. A second administrator can legitimately make a later pre-activity edit after the first completed but before its lost reply is recovered. The original CASE_EDITED readback remains proved by native immutable request/event, whereas current rows contain the later rule/revision; the first footer then refuses forever. Native SQL1939 receipt130–145 positively proves the original command and its intended amended rule/Case revision. Acknowledge that exact immutable CASE_EDITED receipt independently of later live prefill, or qualify a positively newer projection and report that details changed. Retain original command/input/token bytes; do not infer success from live history.

Both findings were sent immediately. This is source-only evidence; application/A4/runtime remains NOT_RUN and no item/checklist claim changes.

3. **OPEN: Draft-link late effects require the actual edit caller fence.** Each native openDraft checks its current()/frame.active() before awaiting the adapter, but passes outer options.current, which only preserves the main parent stack prefix. Another child can become active during the two reads while that prefix remains. Supply the form's current plus frame.active predicate to the adapter so its final pre-bind/pre-open check refuses the late effect. This is the same existing caller fence already used by the Preview footer, not a new identity protocol.

### Next saved source: original three findings repaired

The exact original uniform CASE_EDITED receipt is now acknowledged when the current Case revision is positively newer; older revisions refuse, and equal revisions still require the exact current rule/applied command. All native Draft callbacks pass their actual current plus active-form predicate. Existing-sheet navigation now uses a guarded selectDraft callback, clears old status/line/component caches before controller changes, and reads the requested worker through CHANNEL_STATUS. A different nonnull run refuses. These repair the original three findings.

Two final operands in that small callback were reported immediately: clear netInput alongside subject caches, since resetting statusScope suppresses loadStatus's old-subject net reset; and after awaiting selectDraft, recheck the supplied actual caller/actor/owner fence and focus only the captured still-current sheet. Otherwise old PAYE net input can prefill a different worker, or a closed/replaced caller can cause a late focus. Source anchors at this snapshot: Office1885,1928–1937,1997. Final shared/native SOURCE PASS awaits these narrow corrections only; Worker PASS remains.

## Final functional source closure

Fresh Office1928–1937 clears netInput/resumeInput with all prior subject caches, before changing the controller. Its exact existing-sheet navigation refuses pending/busy/accepted action conflicts or a different nonnull run, binds only the reader-qualified Draft/worker/Candidate and awaits native CHANNEL_STATUS. The adapter captures that target sheet and rechecks the actual caller, actor, NEXT owner and same registered sheet after the await before focus. Fresh-run navigation seeds the same run-qualified journal used by Office.open and therefore skips REVIEW. Non-DRAFT protection opens the existing run-qualified worker review. No new money reader, command or guessed identifier/date is added.

Blocked acknowledgement precedes every native queued remount; original successful acknowledgement uses the exact uniform immutable command/rule/revision receipt even after a positively newer live Case projection, while older or mismatched equal-revision projections refuse. All original pending bytes and expected revision remain retained. Native forms preserve locked Candidate/type identities, tax/schedule/reference validation, full committed read-only restrictions and current actor/NEXT/parent fences. Main qualifies ownership before either NEXT edit or legacy entry; the proven no-caller bank action removal does not alter separately owned bank editing. **All recorded functional source findings are closed; Worker/shared/native SOURCE PASS.** A4 and runtime remain separate NOT_RUN evidence.

One packaging finding was sent immediately: unchanged index.html still loads the changed Office/Preview/three forms/main/CSS with FC04/FC05/FC06 cache versions. Update those existing version URLs before final packaging closure. No additional functional investigation is pending.

| Final saved file | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | f084491a777011b59ba41e57c49602f026aacd24d7c289f80f578ec76c6465cf |
| FE js/bpay-next-case-preview.js | 36fb31a3403e99b726f9330aa34ef2e572b88eb88ba8c731e10ff7af8770738e |
| FE js/bpay-next-payment-advance.js | 4f29b45d73a204ade66fd97a0024f5b66fb0af663bfbeac0358f1f994e337f79 |
| FE js/bpay-next-manual-credit.js | c33e0f1c6513dc628889e5f58d5feeb136ff6ccf5752a4e4bf777651817530af |
| FE js/bpay-next-manual-debt.js | 00487271e75348bda1eab2ee5f5efa361f38aebfb63459712a6f8540fd2d56bf |
| FE js/main.js | e59fd9163b59f95236e398041513b65fd484ec00a46651e412c98a4e927b48fa |
| FE css/bpay-next-office.css | 5179b1bfa514066b8f528a26af2ba65da3ea5ee48936bac7c2886eade7751527 |
| FE index.html (before cache bump) | 0215f79b7f5829c3146104863cbe5b910ba531289919a59987d9720c0f61359d |
| SQL1939 (typed receipt/reader match only; SQL reviewer owns full verdict) | 7b723a61096ac50853dbc609acbba6437b403f11aa796a6f77e39a78b8308f66 |

All eight BE Worker hashes in the first snapshot were re-read and remain unchanged. The final SQL typed receipt still proves the exact original immutable request/event; the changed full SQL hash above supersedes the earlier contract snapshot, without claiming a full SQL review here.

### Packaging and native factual presentation amendment

The seven existing index asset URLs are now exactly20261011-fc09-r1; packaging finding closed. Current edit header uses the existing native subtitle, edit mode suppresses only the creation-mode empty schedule, and committed Credit displays existing server prefill Amount/Tax/Comment/Note as read-only key/value facts. No Note-only action or reader/financial change is added. These source changes preserve the closed journal and caller checks.

One small truthful-copy finding was sent immediately: generic OPEN/noneditable Status currently says “Payment activity” even for HELD_BY_DRAFT from an accepted PREPARING cut without economic activity. Qualify that caption by the actual issue (for example “In a Draft”) and reserve payment-activity wording for HAS_ACTIVITY. The existing blocked pill and exact link already identify its protection. Final functional verdict otherwise remains PASS; final caption/hash closure and A4 are separate.

### Final current-source override

The HELD-only caption is now “In a Draft”; actual activity keeps its own wording. Native edit facts retain the Candidate/channel subject, the duplicate accepted subject is omitted only in edit mode, and the committed Credit amount/tax/comment/Note remain factual read-only values. All seven existing assets use FC09 cache versions. The closed command/receipt/journal, original unknown bytes, acknowledged-rule/revision checks, bounded point readers and guarded exact Draft navigation are unchanged. **Final Worker/shared/native functional SOURCE PASS; all earlier functional and packaging findings are superseded.** A4 is owned by the separate screenshot reviewer; no visual/runtime pass or item completion is claimed here.

| Current final source | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | 67fbf22d2d90d56d3bb82b3a18c8fbec886e8e10e84da64f81781efc24cdc7b6 |
| FE js/bpay-next-case-preview.js | 36fb31a3403e99b726f9330aa34ef2e572b88eb88ba8c731e10ff7af8770738e |
| FE js/bpay-next-payment-advance.js | 6074e95557b2197fade86958393eee6d4368e0465e4b05918d906168c8647c28 |
| FE js/bpay-next-manual-credit.js | 23505cb844f4691a66595172d478a991232a38ad7b6ab8c1a04036064c6a2c74 |
| FE js/bpay-next-manual-debt.js | 25f68670229d13cedf69f7cbf05b3ab2c2e45d1483e80a69f290d7129015a28a |
| FE js/main.js | e59fd9163b59f95236e398041513b65fd484ec00a46651e412c98a4e927b48fa |
| FE css/bpay-next-office.css | c54f42284c55aef1a31328502c61c26b81610629a943ad6d92dfbc6a0acb1122 |
| FE index.html | 946d7c49baa4f662b6c53e5b9e53cf5e702c8ca87af7dc863bed6cf79c6e4c43 |

One A4 presentation-scope advisory was sent: add the required .ctms-modern-modal prefix to the new footer-hide selector. That tiny selector/hash amendment, if made, does not alter the functional verdict; the separate A4 receipt remains pending. BE Worker hashes and the latest typed SQL receipt match stated above remain current.

### Final CSS advisory closure

Fresh saved CSS now qualifies the footer-hide rule with #modal.ctms-modern-modal.ctms-kind-bpay-next-read. The existing protected-Draft button also has min-height/min-width44px below480px, scoped to that same modernised NEXT modal and blocked card. Both source advisories are closed. **Final SOURCE PASS retained; no remaining source finding.** A4/screenshot/runtime ownership remains separate; this amendment performed no execution or product changes.

Final FE css/bpay-next-office.css SHA-256: edd1b7958c59a4612c8e7c705d64ce8846b33ada85fd4c06c452759b112de7ae. This supersedes only the earlier CSS hash; all other final source hashes above remain unchanged.
