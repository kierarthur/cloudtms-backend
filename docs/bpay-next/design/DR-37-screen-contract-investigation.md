# DR-37 native screen and contract investigation

12 October 2026, sequence 130. Read-only investigation of full card03:716–738, addendum `R2_EXACT_CONNECTIONS.md` §2:27–46, all DR37 checklist/owner rows at `IMPLEMENTATION_CHECKLIST.md:3819–3848`, current final registry, existing STATUS/worksheet/Office source, and supplied designs. No product/spec edits, SQL/business requests, tests, fixtures, generated tools or captures. This is not implementation or visual PASS; root alone writes product.

## Supplied screen mapping, directly inspected

Ordinary backend pack root: `codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`.

| Native supplied surface | Exact pack files | Required facts |
| --- | --- | --- |
| SCR60 PAYE net refused / replacement status | `batches/B5/png/SCR-60_net_refused_dark.png`; `batches/B5/specs/SCR-60_net_refused.json` | Previous accepted net/cash remains primary; proposed amount separately “not accepted”; plain refusal, selected Candidate, Retry and eligible cancellation seam. |
| SCR58 pending row pattern reused by DR37 | `batches/B5/png/SCR-58_enter_paye_payments_saved_dark.png`; `batches/B5/specs/SCR-58_enter_paye_payments_saved.json` | Accepted £400 stays visible beside “Pending £420”; Updating state; literal entered £0.00 differs from missing/cleared. Full editing/import/save window belongs to DR22/23. |

Inventory/index explicitly map SCR60 to DR37 and these two designs (`DESIGN_INDEX.md:75`, `SCREEN_INVENTORY.md:116`). Both PNGs were individually viewed, and both JSON specs were read. SCR60 shows the original accepted bank amount unchanged in the refused row, distinct failed proposed net, one plain reason and row controls; SCR58 shows the same distinction while updating. There is no separate DR37 phone PNG in this inventory entry: use the existing responsive native worksheet/table and its established phone policy, rather than inventing another screen. SCR57 is the existing DR21 worksheet pattern; SCR63 is the later DR26 totals panel, not additional DR37 product ownership.

Owner A2/A3 directions override picture-only actions/data and wording: no invented pre-execution pay date/reference; no raw technical failure code on screen; one-sentence plain errors where applicable. The supplied “deductions changed” example is not authority to relabel every refusal with that cause. Retain native typed codes in contracts/journals and map exact known causes truthfully; unknown business reason can direct Office to contact support. No new illustration/table totals are calculated in the browser.

## Current native parents and real caller seam

| Owner | Current exact source and useful anchors |
| --- | --- |
| Worker STATUS | `private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)` in `12102026_0442_banking_pay_next_run_review_totals_v1.sql:200–275`; accepted net/cash at 228–238, accepted projection point at 246–247. |
| Worksheet | `private.bpay_next_paye_worksheet_page_v1(uuid,uuid,uuid,text,text,jsonb,integer)` and row view in `12102026_1019_banking_pay_next_paye_worksheet_v1.sql`; current row logic 60–96, native context/generation 145–147. |
| NET intake / apply | Current registry points `private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint)` to 11102026_0559; `private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer)` to 11102026_2129. Do not restore original 0390 wholesale. |
| Public read / closed Worker | Current public preparation merge parent 12102026_1125; `broker/src/bpay-next-preparation.js` STATUS_HEADER 387 onward and strict header qualification at 978–979. |
| Native Office contract | `js/bpay-next-office.js` STATUS_HEADER 317–319; exact response/identity/pending check 1207; original PAYE_NET/CANCEL_REQUEST inputs 261–263 and receipts 288–290. |
| Native worksheet | Office `mountPayeWorksheet` 2204–2281 and `openPayeWorksheet` 2283–2285; current confirmed channel entry 3015–3018. This is the real reusable table/scroll/search/sort/detail host. |
| Existing thin worker caller | Office 3022–3031 already reads accepted STATUS, enters native PAYE_NET and gates transfer on net_pending/projection. Its cancellation uses existing protectedMutation/EX01, not a new DR37 cancellation command. |

The worksheet currently retains three pages plus bounded 32 KiB cursor history, actual server sorts/search/context, frozen line details, actor/owner/current-namespace retirement and header/phone sort controls. Extend this exact renderer with native refusal/progress facts and optional existing-owner callbacks, not another all-Candidate inventory or a second batch host. Full future DR29/30/DR22 hosts stay explicitly later.

## Exact existing gaps for the replacement contract

**SC37-01: accepted display qualification.** STATUS0442:246–247 filters accepted projection `retired_at_utc IS NULL`. Worksheet1019:86–90 does the same and requires the old accepted allocation state COMPLETE. If bounded COMMITTING pages retire that old state/projection before the final worker pointer switch, these reads lose the accepted amount even though the card requires it to remain displayed. The implementation must either retain those display qualifications until the final atomic switch or read the immutable original state/projection by the exact accepted pointer while its replacement is unfinished. Never expose the BUILDING target as accepted. Old immutable accepted results remain the deduction/cash authority; no duplicate cache or requested-value fallback is justified by this screen.

**SC37-02: refusal survives request equality restoration.** Current worksheet1019:68–72 labels pending/refused only when the request selected at `q.request_no=w.net_request_revision` is greater than the accepted revision, and derives refusal from worker REVIEW, allocation REVIEW or failed job. DR37 requires business refusal to keep worker READY and restore request/projection equality, while preserving high-water and an append-only refusal linked to the refused request. Merely adding REFUSED text to that old condition loses the real refusal after equality restoration. Readers must point to the exact current/latest replacement request/refusal using the new native high-water/pending binding, separate from accepted projection. Job infrastructure failure is resumable work, not a business refusal after partial swaps.

**SC37-03: native progress/read fields.** Existing STATUS has accepted values plus one `net_pending` boolean, but no replacement/request identity, stage, requested amount, expected/processed counts/cursor or append-only refusal reason. Publish the card's exact closed native replacement facts through the existing STATUS route and worksheet row/contract, under its exact run/worker/Candidate/request identity. Stages are the card's PRECHECK/STAGING/COMMITTING/CANCEL_REQUESTED/CANCELLING/COMPLETE/REFUSED/CANCELLED, not inferred from transient lease entry. The old header validator's equality check at Office1207/Worker979 must match the new explicit pending association while preserving the genuine no-Case path; high-water is not accepted revision or proof of pending. Constant-size final pointer completion is the only accepted switch.

**SC37-04: page currentness.** DR21 context already includes native totals_revision and current-request agency sequence. Its observational job hook covers the old REQUESTED/APPLYING/REFUSED transitions. Native DR37 stage/refusal/pending-marker changes can affect rows even if accepted money/worker status does not change. Ensure those exact transitions advance the existing reader generation in the same transaction or are included in its exact native context. Otherwise refresh/page continuation can mix old and new statuses. Keep native identity cursor/keyset/whole-server sorts; if displayed status/reason facts change the sort operand, qualify the native sort with the same fresh row facts. No timestamp sum/digest/witness is required.

## Required presentation and control semantics

- Accepted net, deductions and cash stay from the last completed accepted projection during pending, refusal, infrastructure recovery and partial commit. Requested net is a clearly labelled secondary fact. If no accepted net exists, show Awaiting PAYE net/unknown rather than £0.00. A literal accepted zero remains Entered £0.00. Payout-only stays excluded from applicable payroll missing count and must not acquire a fake net-entry action.
- A precommit business refusal is “Not accepted” with the exact plain reason; worker remains READY and previous money stays in force. PRECHECK/STAGING/COMMITTING or recovery says pending/updating/resuming according to actual state. After the first destructive page, no late business REFUSED badge is valid. Execution remains disabled by stored pending/readiness facts even while an accepted amount remains visible.
- A lost/unknown request uses the existing controller journal's SAME-command/SAME-byte retry (`retry` at 1604); closing/reopening must not replace its identity. A positively terminal REFUSED replay returns the stored refusal. Deliberate retry/correction after that terminal outcome requires the later DR22 owner's new request/command, expected accepted revision and next native high-water number; the picture's “same amount again” is not permission to redispatch a terminal command as a new operation.
- Whole-Candidate cancellation is only the genuine EX27 eligible cancellation and certified NET-to-RELEASE handoff. DR37 supplies its own bounded cleanup/certificate and current eligibility facts; do not build the future EX27 admission command early or offer authority for submitted/unknown payment. A callback without its real owner is a deferred join, not D06 completion. DR22 retry and EX27 cancel integration must remain recorded OPEN until reachable through those actual owners.
- Known controls can reuse current native row/More conventions, real frozen details and existing guarded actor/owner/controller scope. Do not add import/export/edit-window capabilities just because SCR60's full future batch illustration shows them. Root's actual captures/A4 must explain every deliberate visible difference and verify disabled pending/unknown controls, refusal with/without prior accepted net, true zero, all stages and responsive targets.

## Snapshot hashes and status

| Read artifact | SHA256 |
| --- | --- |
| STATUS0442 | `8cbbd0d0bde4fc93c507ef1002a94b050dd3a85d64107ddf592b1e2db6fd978e` |
| Worksheet1019 | `0eeb65839d0f97760ab04a3b87a5d6d3230a261b406f37f78f2179d1512006e5` |
| Worker preparation | `5566476528f95af3a0f28634d9ab1c7a4c19d73e6fcc4f464ddbd363d79fb22b` |
| FE Office | `0c9934bcd334a39e3209d2e260e2eca7c8fa13908dc01bc55fae17dbbf31c790` |
| SCR60 PNG / spec | `dcb506dc83e0ce7b8743d87db17a9bca201f2bd0fb19fcabe5c910aa9d9dc317` / `50fb957961f5116c2f50eebf34b186f906d79357f759e20411ac042df844e62f` |
| SCR58 PNG / spec | `f7379148adecb1c248c7b6513c09d230ee43239b95e0799762878233c10feaf2` / `5c67274c3de7f6eecf10df0233680529e4bc3ae319648106f783f4d7d44b84ac` |

Investigation complete for the screen/contract seam. SC37-01–04 require product source closure, and real DR22/EX27 control joins remain later dependencies. No joined financial, screen-runtime or performance acceptance is claimed.
