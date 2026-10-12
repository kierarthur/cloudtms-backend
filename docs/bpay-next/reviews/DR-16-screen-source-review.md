# DR-16 final independent presentation-source verdict

**UI SOURCE PASS / A4 PASS — 38 PASS, 0 FAIL, 4 scoped N/A; all 17 actual PNGs reviewed.** Full final closure and current source hashes follow the historical checkpoints below. Current A4 receipt: [SCR44/DR16-review.md](../screen-designs/SCR-44/DR16-review.md).

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


## Final saved-source and actual A4 closure — 12 October 2026

**UI SOURCE PASS / A4 PASS: 38 PASS, 0 FAIL, 4 scoped N/A; all 42 items.** Earlier PENDING statements above are historical checkpoints superseded by this closure. All 17 final actual local PNGs are independently inspected; final [SCR44/DR16-review.md](../screen-designs/SCR-44/DR16-review.md) records full policy, visual differences/control purposes and hashes.

P16-S01–S07 remain closed. Final P16-S08 found the two newly added NX104 grid rules missing the modern class. Readback proves both exact new selectors now begin #modal.ctms-modern-modal.ctms-kind-bpay-next-read; older rules were preserved. FE original DR16 product 22732041f56c0d734689dfbb5bbf7075f80c33a0 plus scope-only follow-up 9fdc4f841462ea77d22ee14a304c27cb91943ca0. Actual captured windows already contain that class; pixels unchanged.

Exact native decorator exception is limited to kindSlug bpay-next-read plus .bpay-next-worker-review [data-next-secondary]; unrelated native actions and original sorter/reauth exceptions retain their behaviour. NX104 uses summary.target_pay_channel to qualify PAYE recovery wording, literal six native STATUS values, existing formatted-money markers, no browser sum. Parent final header is nonshrinking and wraps capture time; Due is right aligned; passive Case highlight/keyboard path has no invented Open.

Final source SHA-256:
- js/bpay-next-office.js: `d6e6f914e1a83994ebe0ca2082e6f2456a41c765445cc1e9c2afd8d94bb2b185`
- css/bpay-next-office.css: `254cc0f85b911a4267d735f90787273b84fe0ad18b8d758e80cbf7e7d1e2d9d8`
- js/modal-modernisation.js: `5eea89b05fbea6320656bfddb7dcab6574afc742563f6e2b4e7885f732e610e5`
- js/bpay-next-ui.js: `f535e7e75579428ca7b97b57d406ccaeadaebbf0fe15e5935574f435b772248d`
- index.html: `3e34174753fe71d17c5ce4b4cc5bc23bdb0d58a9ae5ca9c90d9cc3b66a2898fc`

No SQL/money verdict, product edits, command dispatch, installed/joined/queue/financial test or hundreds-row acceptance was performed by this reviewer. Original guards/actions/journals and source roles are reviewed definitions, not claimed runtime execution.
