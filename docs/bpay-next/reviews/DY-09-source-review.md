# DY-09 independent source review

11 October 2026. **SOURCE REVIEW: one essential recovery finding remains in the reviewed bytes below.** No product edits, database calls, tests, fixtures, render or hosted actions were performed. Actual A4 screenshots are pending and no pixel or joined acceptance PASS is claimed.

Read full DY-09 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1094–1117`, all checklist D01–D05 at `IMPLEMENTATION_CHECKLIST.md:2657–2682`, master sequence96/0130 editor chain at `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:63,180`, final registry, BD108 `DY-09-design.md`, independent design review and current investigation. This source slice reuses ordinary Umbrella WORK, captured identity and the existing frozen detail/status owners; later Case/Additional/conversion and DR15 parent work are outside this delivery.

## P2 — totals Refresh erases a separate failed detail read

`FE/js/bpay-next-office.js:1030,1096`: `act(fn)` unconditionally sets `error=''`. The new totals-only Refresh is dispatched through that same wrapper even though `readSummary` has its own `summaryError`.

Concrete path: initial STATUS fails, then LINES fails. Both original detail error and summary error are shown. Clicking the totals Refresh can succeed, clear summaryError and also erase the detail error. The saved `failedRead` still points to LINES, while `lines` remains null. The original detail Refresh is only rendered when `error` is set; switching to Lines does not initiate a new LINES read. Thus that read becomes inaccessible without closing/reopening. The same interference applies to an independently failed Shift/rate detail while totals remain failed.

Required smallest correction: preserve the existing detail error when executing the summary-only Refresh. Keep the existing detail request/cursor and its explicit retry owner; no new reader, schema, refresh framework or automatic retry is necessary. Root was notified immediately.

## Source conditions otherwise satisfied

- All three actual READY entrypoints (button/double-click/Enter at1191–1193) pass the accepted parent runId and native row Candidate ID. The child requires both UUIDs and worker ID at1018; there is no guessed identity or current Candidate lookup.
- Summary performs exactly one existing STATUS request, `after_transfer_no:null, limit:1`, at1038. Current actor, owner and original preparation namespace qualify the late response; exact header run/worker/Candidate checks precede adoption (1024,1039–1042). Shared transport rechecks actor before/after response at373–380 and validates closed STATUS shape and run/worker/projection consistency at551–554.
- Native STATUS0639 at400–428 joins exact run/worker and LEFT-joins only the worker's accepted non-retired projection. Frozen gross ex VAT/VAT/inc VAT come from the frozen worker; cash_amount comes from that native projection. NULL projection facts must all remain NULL under the typed client contract.
- Umbrella Lines shows only the four returned values, using existing decimal text formatting, not sums or gross-to-cash arithmetic. NULL is a dash and genuine zero remains money. The label does not call projected cash paid/settled. PAYE's existing detail layout is retained.
- Current2018 BEGIN at1580–1602 proves why an ordinary Umbrella READY worker can have frozen inc VAT but no accepted cash yet: request/projection revisions remain0 until the native BEGIN event inserts projection1. Displaying a dash does not infer cash from gross.
- Summary failure retains line, origin, selected Shift, bounded detail pages and cursors. It adds one plain error sentence plus explicit Refresh, without clearing frozen line facts or automatic retry. The independent-error interference identified above is the only required correction found.
- The same five Lines/Shifts/Breaks/Used/Available tabs, native captured revision/origin guards, three-page retention, bounded history, original scrolling and controls remain. The final reread places totals below the Lines table and only on Lines; no duplicated totals are added to Shift/rate detail.
- CSS only scopes the native dark definition-list strip: four desktop columns, two below700px, tabular money and wrapping. CSS/Office cache URLs advance together. A4 must confirm actual desktop/device-light/phone containment and all required supplied-design differences/control purposes.
- No SQL/Worker product difference is present. Existing CLAIM0130, BUILD/guard0533, BEGIN/settlement/post2018, preparation0559 and publisher1001 remain current. Their hashes match the reviewed design census. Ordinary NULL-Contract Daily WORK and original captured Umbrella identity remain accepted through the same native owners, with Source-only Contract qualification untouched. No new Daily formula, protocol, table, command or producer; Policy X preserved.

## Reviewed raw SHA-256 before recovery correction

| Source | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | `71b7bd9f37cd5309f482e3f83ff7f0cdd43d9755c6f7a0a82c0f287492e6668a` |
| FE css/bpay-next-office.css | `c93e02a13dc6b53f553a04078eb63b5b2de5c32f58c4cd63508e1986fc63c1b4` |
| FE index.html | `ae526887a3d301a5157a82f29b0a825e4d4e57e69e10b84f8ee626ebabac886e` |
| BD108 DY-09-design.md | `eb5d5e483e205435c87337059f727a5402d276ff217d56cfa1f2ed56f121f13d` |
| Current CLAIM0130 | `763176b54482954adf8c07e7415a86e005baae71848d03bba3245ee8c385907a` |
| Current BUILD/guard0533 | `a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314` |
| Current foundation2018 | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Current preparation0559 | `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6` |
| Current publisher1001 | `b5f2f9ec742a11a88ede05ae0eb433725b410907827bf10300b3540824a92a72` |
| Current STATUS0639 | `15c88f4c45051087976ed9187a741da494c69a052611c2a41892417f9665a22d` |

D03/D04 are preserved historical shared-owner receipts, not new SQL installation tasks. D05 actual joined Daily/weekly one-transfer/bridge/settlement/captured-company journeys remain AUTHORED / NOT_RUN. Root normal checks and A4 actual screenshots are separate evidence.

## Final source closure — P2 corrected

Fresh saved source was reread after root's correction. **SOURCE PASS; the P2 above is superseded and no essential source defect remains.** Office1030 now accepts preserveDetailError=false and clears the detail error only when that flag is false. The unchanged button helper forwards the flag; the new totals-only Refresh at1097 passes false for primary and true for preserveDetailError. Summary success/failure therefore leaves the existing detail error and failedRead request/cursor available for its own original Refresh. Other actions keep their prior behavior. No extra fetch, reader, schema or automatic retry was introduced.

Final Office SHA-256 is `b70ba97d9ae8f70298121e851f71512948cccf2f271e2c8b4f554ded457bddbb`. CSS remains `c93e02a13dc6b53f553a04078eb63b5b2de5c32f58c4cd63508e1986fc63c1b4`; index remains `ae526887a3d301a5157a82f29b0a825e4d4e57e69e10b84f8ee626ebabac886e`. This closure is static source review only. Root is producing the combined STATUS+LINES failure/recovery captures; A4 and joined D05 are not independently certified by this source note.

