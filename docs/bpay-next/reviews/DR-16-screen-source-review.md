# DR-16 independent UI source review — checkpoint

12 October 2026. **UI SOURCE PASS / A4 PENDING.** The seven checkpoint findings below are now closed; see the fresh recheck at the end. Full DR16 card03:425–447, all checklist D01–D11, R3 remaining contract, BD134 scope, current native Office review/NX104 seams, supplied B4 SCR44/44b/44c/44d specifications and the 42 policy rows were read. Actual final captures have not yet been approved. Root alone edits product; this reviewer reads source/designs and writes notes only.

| Finding | Essential current source issue and smallest correction |
| --- | --- |
| P16-S01 | New CSS at the current DR16 tail uses !important for expanded-detail padding, all cell widths and phone alignment. Policy item11/SDP20 prohibits new !important; use named modern scope and native specificity/adapters. |
| P16-S02 | totalsCards formats every count as candidate_count + ' Candidates', including one. Preserve exact server count and singular Candidate at1, with native comma grouping. |
| P16-S03 | Expanded Loading payments… paragraph has no role=status. Reuse the retained native loading/live marker and status role; no additional component. |
| P16-S04 | New CHANNEL/DEDUCTIONS sorting is offered and sent correctly, but the summary's fallback always says Candidate A–Z/Z–A. Add exact Pay channel / Deductions wording from the actual native sort key, without browser sorting or counts. |
| P16-S05 | Expanded REVIEW_CASES rows currently have no row focus key/tabIndex/selection or Arrow/Home/End continuity, unlike the surrounding native Candidate table. Reuse that native row pattern for keyboard reading of the real Case lines. There is no supplied Case action to fabricate as Enter/Open. |
| P16-S06 | New header says Captured before the actual owning capture timestamp; policy.json neverShow explicitly bans capture/captured. Keep that required timestamp but use plain wording such as Figures from. Do not fabricate Prepared time. |
| P16-S07 | net_recovery_reserved is labelled Recoveries from PAYE net on Whole/Umbrella too. Current native sample has £150.00 Umbrella net-stage deduction and known cash, so this implies an unsupported PAYE source. Keep the field but use a truthful stage label for Whole/Umbrella (for example Recoveries at net); retain the PAYE label only on PAYE. |

The whole/PAYE/Umbrella figures remain server-owned and captured; original journals/actions are reused. Missing PAYE cash stays plain unknown, never manufactured zero. Original selected/excluded and reduced-repayment notes are supplied by REVIEW_CASES; no live Case balance or fabricated held amount should be added. Future DR19 Newer information and unsupported picture-only filter strip are deliberately omitted under BD134/owner A2. No SQL/money verdict, action dispatch, joined/browser, queue or business acceptance is claimed here.

Initial source SHA-256: Office 868bb1e22ee63c6ca8c729e47c19b9ffa40c3a70d36650e630a076f43c6df8e1; CSS b09f5946f3e7fec5adca5282ccff621d61ffc9f59ea5be3128b1c3cee8c1806c. These are checkpoint hashes, not final capture provenance.

## Fresh saved-source recheck

All seven findings are closed in the current saved presentation source. New CSS has no !important; singular Candidate is correct; expanded loading has role=status; sort summaries identify Pay channel/Deductions; passive Case rows use row-qualified selection/focus/Arrow/Home/End; actual timestamp says Figures from; Whole/Umbrella use Recoveries at net. Existing review-row CSS supplies the selected highlight to nested Case rows. No unowned Case Open/Enter action was added.

**UI SOURCE PASS; A4 PENDING actual final PNGs.** This is presentation-source evidence only, not a joined keyboard/scroll/read-failure execution or financial/SQL approval. Rechecked Office SHA-256 c5ff9d358f0bf1fa569ea3a52182b9516b8c81294d9c7cd4d6abde243380fe3b; CSS 484cec3bb67026453b6f4650628937be7668dbbe03db9c03aeecb6f259d6fc6d. Final capture provenance must refresh hashes if root changes these bytes.
