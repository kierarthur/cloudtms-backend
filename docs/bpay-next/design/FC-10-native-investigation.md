# FC-10 native caller and journal investigation

Read-only investigation, 11 October 2026. Sequence 108; FC-09 is closed. Read the full FC-10 card (1217–1236), ITEM/D01–D10 rows, BD120 and the current investigation, actual main.js/Office/native adapter/shared Case preview, and supplied B9 SCR-123/123b/123c JSON and PNGs. This note identifies required integration; it approves no authored implementation, screenshot or joined acceptance. Product author remains root.

## Smallest owning seams

| Existing object | Current evidence and required change |
| --- | --- |
| main.js confirmBankingFinanceCasePause / Resume, 20589–20907 | Both currently read legacy banking state and send legacy /finance/cases/:id/pause or /resume. Add the early actor/parent/NEXT ownership branch before those reads, using the existing creator pattern at 12988–13008. Proceed to the unchanged legacy path only on explicit LEGACY ownership; failed/unknown qualification must not fall through. |
| main.js native action callers, 102756–102777 | Current caller uses findFinanceCaseById before opening the confirmation. Follow the existing NEXT edit caller at 102676–102681: obtain exact case_id and candidate_id from the qualified native context/row before legacy lookup. Do not require a CASE_CREATE receipt: automatic Overpayment recovery is legitimate. Display labels are presentation facts, not authority to substitute another Case. |
| Office INPUT / RECEIPT / requestBody / receipt, 126–150 and 432 onward | Add only CASE_PAUSE and CASE_RESUME closed schemas. Both carry original command_id, case_id, expected_case_revision and required-nullable reason. Enforce BD120's reason ceiling, no NUL and action-specific 16384-byte JSON ceiling; preserve every unrelated ceiling. Explicitly cross-check Case and original command/action in the accepted receipt: the generic receipt loop currently does not include case_id. Do not reuse CASE_EDIT's rule_id=command_id invariant. |
| Office createController, 1011–1101 | Reuse journal admission-before-send, exact pending bytes, dispatch(true) recovery and immutable snapshots. Add a finite nullable recovery receipt slot with strict prior-version adoption, rather than placing status recovery in case_edit. Both actions use one actor/Candidate/Case qualifier, so Resume cannot bypass an uncertain Pause in a different journal. |
| Office CASE_COMPONENTS DTO/query and readCaseEditContext, 187–214 / 1964–1971 | Use BD120's existing reader addition: nullable recovery_state/control facts and optional original pause_command_id readback. Reuse complete bounded page, Case/Candidate/channel consistency checks. Do not transplant edit_state.editable/no-activity eligibility: recovery after funding is FC-10's intended use. |
| Office openCaseProtectedDraft, 1993–2006 | Reuse the native run review for PREPARING/READY and reader-qualified existing PaymentBatch window for DRAFT. Pass the actual native run/worker/channel/Candidate tuple. No new page, route, Draft reference, pay date, release command or cancellation control is needed. |
| bpay-next-ui.js createStandardWindowAdapter, 32–74 | Reuse actual Office frame, Close/Escape, parent stack, focus and owned footer. Small confirmation with safe first DOM action uses the existing alertdialog/question label. Terminal result uses ordinary dialog/modalTitle. No new modal framework. |

The existing creator ownership guard captures actor, parent frame, modal context, stack prefix and qualified NEXT module epoch. Apply it before native reader dispatch and again before rendering/mutation/readback; check actor and owner again after asynchronous work. The seed identifies the target; authoritative reader Case/Candidate/revision/status determines the permitted intent. Refresh uses the same original Case/Candidate, not whichever Case later occupies the legacy row.

## Journal and truthful completion

1. One recovery journal must retain the canonical original action/Case/revision/reason bytes before sending. Validate exact closed keys and bounds on reload, preserve all existing V6 fields and pending bytes, and freeze a copied accepted recovery object in snapshots. Old-version adoption adds a null slot only after validating the exact old schema; do not discard unrelated pending work.
2. Fresh submission requires no busy/pending/accepted recovery. Initial definite refusal can clear only under the existing safe transport contract. Any unknown response retains the original request; explicit Try again repeats those same bytes and UUID through dispatch(true). A later auth/refusal or larger current revision alone does not unlock a replacement.
3. Accepted admission is not Pause/Resume completion. Preserve the original accepted command identity until CASE_COMPONENTS readback for that exact pause_command_id supplies its qualified immutable terminal outcome. Verify Case, Candidate, action/target and original command/sequence against retained admission. PENDING remains pending; APPLIED and BLOCKED are distinct. Clear the accepted slot only after qualified terminal readback has been shown/handled.
4. Current OPEN/PAUSED alone cannot prove this command. A later opposite change must not turn an old applied Pause receipt into a claim that the Case is currently paused. Show the original outcome and fresh current status truth separately where they differ; stale/read failures retain recovery and offer manual Refresh.
5. Applied Pause reports Paused — not collected; applied Resume reports Recovery resumed. Protected R3 outcome reports refusal/required release or cancellation and preserves the captured deduction. Pending unknown protected amount is an em dash/plain unknown; never manufacture £0. Multiple bindings are described as Drafts, not one first reference owning the aggregate.

The private immutable request/outcome and original command/job progression in BD120 provide these readback facts. They are the necessary backing for the two existing intake actions, not a new public reader or financial feature. This investigation does not review their forthcoming SQL or Worker implementation.

## Supplied appearance and control purpose

| Reference or control | Required appearance/meaning |
| --- | --- |
| B9 SCR-123 pause confirmation | Actual Candidate/Case subject, known native weekly/outstanding facts, optional Reason, consequence that new Drafts take no repayments until resumed. Keep collecting dismisses unchanged; Pause recovery submits one qualified original intent. Safest dismiss action receives initial focus. |
| B9 SCR-123c resume confirmation | Same native small question layout, optional Reason, Keep paused and Resume recovery. Replace the illustrated exact next-Draft £75 promise: selection, affordability, floor and capacity still govern allocation. Resume restarts collection without promising an amount. |
| B9 SCR-123b protected result | The supplied green Paused result while £150 remains protected conflicts with revised R3. Required deviation is a truthful blocked result, protected facts and existing Draft navigation/next action. No Paused/COMPLETE success while protected. Unprotected applied result may use the supplied positive tone. |
| Reason | Optional status-change audit text, bounded native text, blank normalized to null. It is not a new Rule or economic term. |
| Try again / Refresh | Try again replays original uncertain bytes only; Refresh reads original accepted outcome or current refusal context. Omit duplicate controls and explain any disabled control. |
| Open Draft / native Close | Open Draft identifies the real retained protection through the existing native adapter. Close/Escape dismisses presentation without forgetting pending/accepted journal state. No extra release/cancel capability in this card. |

No future Cases-page background, pay date, fabricated PB code or exact allocation is drawn from the artwork. The earlier investigation's optional pay_date point-join is unnecessary under current BD120/owner A3a; use existing genuine address/channel/status and known native amounts. Any remaining visible difference must be recorded as card/owner required or fixed at A4.

Required later actual local capture states: both unprotected confirmations with populated/empty Reason; zero-outstanding unfunded Advance; applying/pending; uncertain original-request recovery; accepted pending then applied; stale and read failure/recovery; exact protected single/multiple/unknown amount; native Draft navigation; actor/parent invalidation. Inspect dark, device-light and 390px phone, including safe focus/control order, 44px targets and status/alert roles. Supplied background lists and joined business/queue execution are outside this investigation.

## Source identities observed

FE HEAD b0be40bb7e42395a7d4ff6df202d1d625c6ad1ef; these saved source hashes precede FC-10 implementation.

| Source | SHA-256 |
| --- | --- |
| main.js | E59FD9163B59F95236E398041513B65FD484EC00A46651E412C98A4E927B48FA |
| bpay-next-office.js | 67FBF22D2D90D56D3BB82B3A18C8FBEC886E8E10E84DA64F81781EFC24CDC7B6 |
| bpay-next-ui.js | 718C2AC4BD4827CBDD07218FA846DB5CCFAF9F5457FEA0FF7EF7B4FCC8911462 |
| bpay-next-case-preview.js | 1EAE57DFB80AF14AA18417A4AC52DC14A03508FAD9B457D2B4E826C6541998DD |
| FC-10-builder-design.md | 90F97F427FAB50ED605D4BF10607A0B0552802DFFF0446A32813AF44A5B99C3A |
| FC-10-current-investigation.md | 4D28D4EC2768C0831C390940597B9A5BE3F2B3D742CB27963FB4558CDDDB6D9A |

Investigation complete. BD120's narrow native integration is supportable with the conditions above; authored source review and full A4 are NOT_RUN. No product, pack, checklist, fixture, database, hosted or business action was performed.
