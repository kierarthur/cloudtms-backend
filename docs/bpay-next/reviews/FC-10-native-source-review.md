# FC-10 independent native source review

Source verdict: PASS for the callable native Case/Candidate entry, closed V7 journal and exact status readback. Read-only review of the actual FC-10 native module, main caller qualification, Office V7 journal and shared validators against the full card/ITEM/D01–D10, BD120, native investigation and supplied SCR-123/123b/123c. All five source findings below are closed or explicitly bounded. A4 remains PENDING; joined acceptance is NOT_RUN. This source pass does not approve the initial duplicate footer Close or CSS-prefix presentation finding already sent to root.

## Essential findings

| ID | Finding and smallest required correction |
| --- | --- |
| FC10-O01 | The initial module always opens confirmation:true and continues the Pause/Resume question, consequence and question icon after an applied/blocked terminal result. Use ordinary result dialog semantics/title and remove confirmation-only question controls once the original outcome is shown. Root is addressing the native adapter while this checkpoint is saved; fresh module integration must be re-read. |
| FC10-O02 | Existing main.js Pause/Resume controls at11532/11540 have only data-finance-case-id; the new native caller at102779/102793 requires candidateId and candidateDisplayName and substitutes empty identity otherwise. That actual DOM path cannot open the new strict native module. Supply the exact qualified native Case/Candidate seed through the owning caller/context; do not infer Candidate from the legacy list or add a future Cases page. A directly callable confirmation with a correct explicit native seed is already available, but does not prove the existing DOM path. |
| FC10-O03 | notice and fieldError are initially/ordinarily empty p nodes without data-next-live-copy. Existing modal-modernisation.js1291–1294 removes p:empty unless marked. Subsequent busy/unknown/error/invalid Reason updates can target detached nodes. Preserve these dynamic owned paragraphs with the established marker. |
| FC10-O04 | Before finishCasePause, reject a current Case revision below immutable applied_case_revision. Larger/current status alone correctly does not settle the journal, but an internally contradictory current read must not be rendered as a valid completed/current composite. Also distinguish immutable BLOCKED protection facts from fresh pause_state when that protection has since been released: current present-tense 'already in a Draft' cannot come only from the old outcome. |
| FC10-O05 | String-formatted money hosts lack data-next-formatted-money. Existing formatVisibleMoney (modernizer341–357) re-coerces their £ values through Number/toFixed, removing grouping and potentially losing native pennies on large exact money. Mark the factual money hosts with the established formatter opt-out; no browser calculation is required. |

Concurrent correction re-read: setConfirmation now switches the owned native frame to ordinary result dialog, replaces the terminal question and hides its icon/consequence; O01 is closed in source. notice/fieldError now retain data-next-live-copy; O03 is closed. O04's lower-current-revision guard is saved before journal settlement and historic protection has an explicit request-time prefix plus changed-position Refresh instruction. Recheck the companion notice for historical/current consistency at final closure. O02 is an actual legacy DOM-seed limitation; the direct callable native confirmation accepts an exact seed, and a future Cases page must not be added to fill this limitation under FC-10.

Final source closure: O04's terminal refusal now says the request was refused because the Case was in a Draft; its reference/amount is explicitly historical, while changed current protection asks for Refresh. O05's exact string money survives native modernization through data-next-formatted-money on facts and protection money. O01/O03/O04/O05 are CLOSED. Root explicitly accepted O02 as the owning boundary: current confirmBankingFinanceCasePause/Resume and openBankingNextCaseStatus are real callable native entries with an exact Case/Candidate seed; the existing legacy DOM markup is NOT_RUN and deferred to FC-02's native Cases renderer. No new reader, legacy lookup or invented future renderer is introduced to fill that gap.

## Verified source behavior

- main.js openBankingNextCaseStatus qualifies current actor, native parent/context/stack and module ownership before the original confirmation bodies' legacy reads. Only explicit LEGACY falls through; NEXT/unknown failures produce a native notice. The DOM caller also branches before findFinanceCaseById, subject to O02's seed gap.
- Closed CASE_PAUSE/CASE_RESUME schemas use original command/Case/expected revision and nullable bounded Reason, action-specific16384-byte JSON limits. Receipt action/Case/expected revision are explicitly cross-checked; Candidate derives from the accepted native Case and is checked against the journal's bound Candidate.
- One .case-pause.Candidate.Case journal covers both actions. V7 adopts exact old schemas with a null slot, preserving pending bytes; snapshots freeze copied recovery metadata. Accepted request_bytes is retained. Opposite action cannot bypass pending/accepted state.
- Existing dispatch(true) replays identical original bytes. Later refusal/auth does not clear earlier uncertainty; initial definite safe refusal alone may clear. No automatic replacement UUID/retry is introduced.
- CASE_COMPONENTS retains existing route and receives only typed pause_state plus optional original pause_command_id/pause_receipt. Complete bounded page and exact Case/Candidate/row-state/receipt equality checks precede terminal handling.
- Terminal receipt must match original command, Case, Candidate, action, sequence and expected revision. PENDING retains accepted state; BLOCKED never says Paused. Applied status/revision is validated by the shared finite receipt validator. Current later status is disclosed without treating it as this command's admission.
- No CASE_EDIT no-activity predicate is transplanted; new recovery controls can handle funded recovery. Actual PAUSED allocation and financial terms remain server owned.
- Protection uses exact native count, nullable amount and original address; plural count is described as Drafts. Existing openCaseProtectedDraft is reused; no synthetic PB name, pay date, cancel/release action or future allocation promise.
- Optional Reason, safe dismiss, one Pause/Resume action, exact-byte Try again and accepted Refresh have meaningful payroll purposes. A qualified applied outcome invokes the supplied onCaseChanged callback with the exact receipt/page and returns a named bpay_next result on dismissal. The existing child-result guard suppresses legacy Workbench refresh for that result. A future native caller must consume its own callback/result; actual parent interaction is NOT_RUN here. Pixel closure remains pending actual captures.

## Checkpoint identities

| Saved frontend source | SHA-256 |
| --- | --- |
| js/main.js | CB156512EF05E0D85D05EDD7F599CAD6FA415C170F3B2834D28B415CEAD6B2BC |
| js/bpay-next-office.js | EBF5237C32438A7FF3B1E94944D0C07221BC7391B027621ECD473A8FC8E433EB |
| js/bpay-next-case-preview.js | DAC5DBF44B4DCBBD9825412239D312750409CF63D0A9658965093F6DF6837226 |
| js/bpay-next-case-pause.js | F8FBD567DC9F2B972F96320CDF9E5237AEDAF904EAF8E239900612718D8D847C |
| js/bpay-next-ui.js, concurrently changed adapter | F535E7E75579428CA7B97B57D406CCAEADAEBBF0FE15E5935574F435B772248D |
| css/bpay-next-office.css, presentation still being corrected | A6D4BF4DFF90FB957AF88009DA03C39291C74C5BF3EFB9B55A8EE0DCA61EF39D |
| index.html | 3DCFA6ED5A90854C457301F75C453E971C54FE163D6345088D7F06A9DA08A7AC |

No product, screen rendering, fixture, database, test, hosted or business action was performed. These source observations are not a runtime/financial acceptance claim.
