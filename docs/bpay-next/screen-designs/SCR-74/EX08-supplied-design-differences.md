# EX-08 / SCR-74 supplied-design comparison

Authority: owner A1–A4 and 11 October screenshot-next-to-PNG correction; supplied B6 `SCR-74_pay_channel_change_impact_dark.png` and matching spec were read, never edited or copied. Actual native screen captures are local-only, using the five fixed response rows in this folder; no database rows, hosted calls or command dispatch. Nine states in dark, device-light and phone widths. Compare preview beside the supplied PNG, then inspect every state.

Visible differences requiring justification:

- Native screen retains the existing Office frame, focus outline and actual Candidate background. The supplied image has a design annotation strip; that strip is not product content. The frame is the supplied medium980 width, native grid title/subtitle and reachable footer. Outer sections use16px and inner table13px per supplied design/SDP23. An early phone title squeeze was fixed and the screenshots replaced.
- First card remains Stays payable with server count2, exact converted£672 and unchanged£25. The illustration's reimbursement explanation is omitted: the closed row provides classification and amount, not a reimbursement tax/bucket label. “Stays£25.00” and the returned Mileage label give the actual supplied facts without adding a server feature.
- Needs a decision says “Pay channel decision needed”, because this reader supplies no Night/rate bucket or later WB29 decision. “PAYE, as authorised” is required by ownerF; the saved source amount£296.45 is not replaced with a guessed target. It never claims the later classifier/resolver has already run.
- Not changed has an exact server count rather than the illustrated “Already in a Draft or paid” title, because its card scope also includes unchanged unavailable future work. Protected Work shows “PAYE Draft” and “—”: there is no short Payment Batch reference or exact per-Work accepted amount before the later batch reader. Its technical raw subline is omitted. The illustration's PB-0412-P, aggregate2Timesheets and£734.20 are not supplied facts. Protected Case shows the supplied Payment Advance amount£150 and original Draft channel, “Stays as agreed”. No UUIDs, fake aggregate or second detail viewer.
- PAYE target has no Umbrella picker. A same-channel company change shows “Umbrella to Umbrella” and the actual selected company; this is a required EX08 transition, not another screen. Empty, initial-loading, failed Preview/Refresh, retained unknown/Try again and accepted/Close are required lifecycle states missing from the illustrated example. Every error is inline; no toast, technical code or pre-execution date.
- Phone uses existing native card-table conversion with the same fields and44px controls, continuous body scroll and fixed reachable footer. No Sort by exists because SDP34 exempts this Preview dialog's finite impact rows. Device-light remains the same dark Office. The scroll capture reaches the final Case using the same closed cursor/context, with no page buttons.

Control purpose / payroll expectation:

| Control | Needed here / expected purpose |
| --- | --- |
| Header Close | Back out safely; preserve dispatched uncertainty, or return the accepted result to its original Candidate form. |
| Umbrella company | Select the future company; any target change clears Preview and rereads the exact target. Existing enabled-company choices only. |
| Apply change | Sole primary; accept the exact server Preview context and future method/company. |
| Refresh | Secondary, visible only after a failed/changed Preview; never retries a command automatically. |
| Try again | Sole primary only for the retained earlier request; sends those exact bytes and operation. |
| Accepted header Close | Returns native accepted method/company; no extra footer Close or stale unconfirmed message. |

No row buttons, carets, ticks, bank inputs, sorting system, rate-choice controls or extra viewer are supplied or needed. Source/API contracts keep incompatible/protected money unchanged. Card/owner-required facts and recovery differences above are the only intentional differences; unexplained differences must fail independent A4 review.

Initial A4 found and root fixed technical protected-row copy, accepted-state stale message/duplicate Close, failed-state misleading picker prompt, retained hidden-picker CSS, error live roles, phone control heights and wrapped header clipping. These initial captures are superseded by the final measured native captures; approval applies only to those final bytes. Each native visible phone button/select is measured in EX08-local-captures.json, including the real header and footer targets.
