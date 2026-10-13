# WB-31 Office source review

13 October 2026. Independent read-only source review of the native single-component choice module against the complete WB-31 card, BD172, the previously reviewed closed DTO, original resolution controller and native window adapter. Product belongs to the root builder. No product code, SQL, business requests, tests, fixtures or screenshots were executed or changed by this reviewer.

## Initial checkpoint — SOURCE OPEN

The first module snapshot was `95070d59168afb33d358190da83bed7de635a79d697518a1b93183d0aa0fe8f1`. Findings were sent promptly to the root. The builder is adding native entry/CSS wiring concurrently; this checkpoint does not approve unfinished integration or A4 visuals.

| Finding | Current state | Evidence and required correction |
| --- | --- | --- |
| O31-01 — obsolete-parent effects | OPEN, partly corrected | Original `mount` used `canFocus:alive` and dismissal restored focus with actor/owner checks but no caller-current check. Latest observed source qualifies mounting/focus with `current` (allowing a truthful accepted-result frame to remain) and checks `options.current()` before returning focus. `refreshParent` still awaits the external refresh and closes the frame based only on `frame.active()`. Recheck the captured caller/actor/owner/frame after the await before this close; no new identity protocol is required. |
| O31-02 — suggested selection displayed manual operand | CLOSED in observed delta | Original input always used retained manual `value`, including while Suggested was selected. Current source displays the genuine suggested rate for the suggested rate mode (gross suggestion remains labelled in its native option), retaining manual input separately; it no longer presents a different manual value as the selected suggested rate. |

The original controller remains the sole journal and request owner. Startup recovers a retained original request before live reads, readback proves absence before exact-byte retry, and no replacement request is created while one is pending. Manual canonical rate/amount and UTF-8 reason bounds are checked before dispatch. Editing clears effect without a network call. Show effect and Apply use the exact disclosed native rows/revisions/decision pairs and common context; server quote/effect fields supply pay, charge and margin. Apply lists each affected native Work and obtains separate deliberate-zero consent. Accepted receipts remain authoritative if refresh fails. Linked scope defaults off and is taken from the closed reader, never discovered by a browser sweep.

Further source checks: verify the completed real entry wiring and final parent refresh fence; record final hashes once the builder freezes the slice. Existing design/transport PASS remains scoped to those reviewed artifacts. A4 and joined application acceptance remain outside this source receipt.

Supporting reviewed source hashes: original FE resolution helper `df4a0bec13a3d1dab602ea34c7009b20bca0511096caf4b796bff618c01bff57`; native adapter `f535e7e75579428ca7b97b57d406ccaeadaebbf0fe15e5935574f435b772248d`; BD172 `2ae3e7cbf4dc348859f7afe026cdb05520074afa2ec1c5f376994e822d7b26cd`. Concurrent module snapshots were reread for the described finite closures; final module/entry hashes are intentionally pending.

## Completed entry/CSS source delta

Reread the new Workbench entry paths and exact diff against its current HEAD. Pay-lines actions bind the original native member to Candidate/Work/revision/approved line and native family/code, require its actual TAXABLE fact, AVAILABLE/nonselected scope and the exact actionable or chosen-Ready branch. Additional code is taken from its native component key where its associated rate identity is absent. Standard bucket actions use the actual bounded native rate rows. The original COMPONENTS/RATES pagination and all financial values remain unchanged. The retained Candidate menu asks the original controller for the existing same-Work journal and reopens those original bytes, including Processing rows. Parent detail retirement closes the owned component child; new assets load before any user invocation and cache versions are updated.

The scoped stylesheet uses the existing native frame and component-only classes; selectors using the shared `bpay-next-component-choice` name are specifically `div`-qualified so the existing selection labels do not acquire window layout. This is source scope review, not a pixel or 42-policy verdict.

Current inspected hashes: component `ecaaf9fbbaf12294d004e49e83ac637fd90223f21c9a81505e7f931420ab84ff`; Workbench `5da7a70a73c370a1347facfb1c97a5764fe04d5324ac96abee63d593899c9b43`; CSS `29be6d9352587c0e53faf7b8e66b2cb88e5937e60abeaf3cfbe5a67fd58c7a31`; index `5fea9261819a511c2c9756698869edf71ec98f2fc1f35209ca5527705ea82eca`. O31-01's post-await refresh close remains the only open source finding. The deliberate-zero path now uses one distinct confirmation containing the actual figures and safe Go back / Apply £0.00; a duplicate prompt is not required for its consent semantics.

## Final source override — SOURCE PASS

Fresh full-module reread confirms O31-01 is CLOSED: after external refresh, `refreshParent` requires `current() && frame.active()` before closing the owned frame. Mounting and return focus remain qualified by the native caller, actor and owner. O31-02 remains CLOSED. Explicit native aria labels preserve the field's visible name independently of inline error content. The final deliberate-zero confirmation discloses the actual affected native Work and amount and requires Go back / Apply £0.00; the typed `zero_confirmed` operand is set only after acceptance.

Zero open source findings. This overrides the earlier OPEN checkpoint for the completed module and native entry wiring; it does not approve A4 pixels or claim joined application execution. No browser pricing, new request owner, receipt schema or financial authority was introduced. Existing design and transport PASS are retained.

Final inspected SHA-256:

| FE file | SHA-256 |
| --- | --- |
| `js/bpay-next-component-choice.js` | `1b9635acdd124ac270e4aa3f04f630f3332bcfd48c7934ae8e8984861f5aca7b` |
| `js/bpay-next-resolution.js` | `df4a0bec13a3d1dab602ea34c7009b20bca0511096caf4b796bff618c01bff57` |
| `js/bpay-next-workbench.js` | `5da7a70a73c370a1347facfb1c97a5764fe04d5324ac96abee63d593899c9b43` |
| `css/bpay-next-office.css` | `29be6d9352587c0e53faf7b8e66b2cb88e5937e60abeaf3cfbe5a67fd58c7a31` |
| `index.html` | `5fea9261819a511c2c9756698869edf71ec98f2fc1f35209ca5527705ea82eca` |

A4 wording/geometry remain separately reviewed. In particular the retained stale sentence currently has two sentences; the higher-priority owner one-sentence error rule should be applied during that presentation pass. This advisory does not alter any request, recovery or financial predicate.

## Precision-marker supplement — temporary SOURCE OPEN

Reread the presentation delta and original native formatter. `modal-modernisation.js:284` positively opts `data-next-exact-input` out of `isMoneyInput`; both decoration and blur's `formatMoneyInput` therefore skip the exact decimal input. That prevents the legacy `Number(raw).toFixed(2)` path from changing entered six-place rates or native large amount text. The owned currency prefix is presentation only. `formatVisibleMoney` at line352 skips text beneath `data-next-formatted-money`; the main component root now carries that marker and its own decimal-string/BigInt labels remain exact.

O31-03 OPEN: the confirmation uses a separately created body without that money marker. The formatter scans all native body text, so its actual six-place `£20.123456` consent labels can still be rewritten to two places while the original request remains exact. Mark the owned `question()` body too; this is a required precision correction to the new window, not a generic formatter rewrite. The root was notified immediately. All earlier source closures remain valid.

Observed current hashes before that correction: module `393c33eab2ad09aec62de8e67d24204747370d36dce2faf92861d632b142f757`; CSS `5011ea3ea7a4feece415634fe77dcb8f3fbaa0e02d09c6d90811a359d72e4d44`; original moderniser `5eea89b05fbea6320656bfddb7dcab6574afc742563f6e2b4e7885f732e610e5`. Resolution helper, Workbench and index retain the preceding reviewed hashes. Gross mode omission/reordering removes an unavailable control, not an allowed intent; supplied Candidate/Client confirmation facts come from the existing frozen header. Currentness, zero consent and original W2/controller semantics are retained. Pixel/A4 approval remains separate.

## Final precision closure — SOURCE PASS

O31-03 CLOSED by fresh source reread: the owned `question()` body now sets `data-next-formatted-money` before the native frame mounts, so actual-rate and native amount consent text receive the same legacy-formatter exclusion as the main component. The six-place input's `data-next-exact-input` prevents both decoration normalization and delegated blur normalization. These are existing opt-outs; the generic modernisation module is unchanged against FE HEAD. Currency prefixes and all component figures remain decimal-text/BigInt presentation, with no coercion into Number or browser pricing.

Final current module `87d025c83bf8d3b7491590ac4b427d4c7954b0eb30051d800f084a5f03b3b9ef`; CSS `5011ea3ea7a4feece415634fe77dcb8f3fbaa0e02d09c6d90811a359d72e4d44`; original moderniser `5eea89b05fbea6320656bfddb7dcab6574afc742563f6e2b4e7885f732e610e5`; resolution helper `df4a0bec13a3d1dab602ea34c7009b20bca0511096caf4b796bff618c01bff57`; Workbench `5da7a70a73c370a1347facfb1c97a5764fe04d5324ac96abee63d593899c9b43`; index `5fea9261819a511c2c9756698869edf71ec98f2fc1f35209ca5527705ea82eca`.

This latest override restores SOURCE PASS with all three findings CLOSED and zero open source findings. A4 screenshot/policy approval and application execution remain separate; the previously recorded one-sentence stale-copy advisory remains a presentation-pass obligation.

## Final money-label and Standard-entry amendment

Fresh reread confirms the final two finite changes: negative currency uses UK `−£` followed by the unchanged exact decimal-string/BigInt formatter (including signed-zero fractions); Standard Review payment is offered only when the actual native `approved_hours` is positive under the existing exact `decimalCompare`. Zero bucket rows are still displayed, with their action cell as a dash. No quote, request, scope, journal or financial predicate changed.

SOURCE PASS retained. Current module SHA-256 `2981523653ff51b4e0801b03ae9ac3884f1d5f683dfede89ebde4e48486e163c`; Workbench `4d8e76c5f828f54467ed2d58acc30aef4902fd6f6d17607d377efdb35492e5be`; CSS `5011ea3ea7a4feece415634fe77dcb8f3fbaa0e02d09c6d90811a359d72e4d44`; index `5fea9261819a511c2c9756698869edf71ec98f2fc1f35209ca5527705ea82eca`. All original controller and exact-input/confirmation marker closures remain valid. No screenshots or business operations were executed by this reviewer.

## Final known-stale and accepted-status amendment

Fresh full-module reread confirms the finite stale-state refinement: native reload/stale faults persist `stale=true`; every new edit, Show effect and Apply requires `interactive()` and therefore refuses the known stale offer. Invalidation does not remove that stale fault. Refresh uses the separate current/active/nonbusy/no-pending window predicate, remains the sole primary for stale state, asks the original unsaved-choice discard question where applicable, refreshes the parent explicitly and rechecks current/active before close. It never substitutes a new revision into the old accepted command or retries a new request automatically. Pending-original readback/retry remains the original controller's path. The accepted paragraph now has native status/live-copy semantics; tone/spacing changes are scoped presentation.

SOURCE PASS retained with zero open source findings. Latest module SHA-256 `83e71ed54a9cefc3c66b64558a33f553eafe2bac8e918bf67cfb9596556912d9`; Workbench `4d8e76c5f828f54467ed2d58acc30aef4902fd6f6d17607d377efdb35492e5be`; CSS `b81c3b69b42e2e42c543a94d146a791f704cdd004e237d9ddfe5d9446f234d57`; index `5fea9261819a511c2c9756698869edf71ec98f2fc1f35209ca5527705ea82eca`. Source only; final pixel/policy verdict remains the A4 reviewer’s responsibility.
