# FC-14 native screen and caller investigation

11 October 2026. Read-only investigation for sequence109, after FC10. Root owns implementation. No product/checklist changes, database/business requests, tests, new tools, rendering, fixtures, hosted actions or commits were performed. Four supplied PNGs were individually viewed from the read-only pack. This note is a source/design boundary, not implementation or A4 approval.

## Authority and exact supplied mapping

Full card `PLAN/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1316–1336`; all FC-14 ITEM/D01–D07 rows `BE/docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3222–3249`; `PLAN/IMPLEMENTATION_SEQUENCE.md:115–116` places FC10 then FC14, with EX50 at213. Owning remaining deliverables are D01 audit repeatable, D02 CASE_AUDIT contract, D03 CANDIDATE_CASE_REPORT contract, D05 exact existing Worker mappings and D06 native entries/screens. D04/D07 are context only. Card dependencies are FC03/F16; “Design needed first: NO” does not remove the exact reader/UI obligations.

PACK is ordinary backend `codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`, not a writable product target. Exact mappings in `DESIGN_INDEX.md:126,141` and `SCREEN_INVENTORY.md:182,202,249`:

| Surface | Read-only design paths |
| --- | --- |
| Case audit, SCR-126 | `batches/B9/specs/SCR-126_case_audit.json`; `batches/B9/png/SCR-126_case_audit_dark.png` |
| Candidate Finance Report, SCR-111 | `batches/B8/specs/SCR-111_candidate_finance_report.json`; `batches/B8/png/SCR-111_candidate_finance_report_dark.png`; `batches/B8/png/SCR-111_candidate_finance_report_phone_dark.png` |
| Report failed read, SCR-111b | `batches/B8/specs/SCR-111b_finance_report_failed.json`; `batches/B8/png/SCR-111b_finance_report_failed_dark.png` |
| Policy | `SCREEN_DESIGN_POLICY.md`; `tools/policy.json` |

SCR126 is the **Case audit overlay**, not its illustrative Cases page behind it. It shows When, Event, Who, Source, Amount, Reason and Details, oldest first, one standard modal and header Close. Building that background FC02 list here would build a later card early. SCR111 is a standard Candidate Finance Report with Candidate/reference subtitle, native summary strip, Active only initially checked, fixed business section order and rows sorted within each section. Phone uses existing labelled card rows and Sort by control. Failed read has unknown figures and Retry. Inventory explicitly says FC14's report and EX50's report are **one window**; do not create a second viewer or build future EX50 acceptance merely because the pack names both.

## Exact current native entry boundary

FE `js/main.js:49709` defines `openBankingFinanceCaseAuditModal(seed)`. It currently accepts legacy finance_case_id/id, then `:49894` reads `GET /api/banking/finance/cases/{id}/audit` into an unbounded timeline. `:102860–102865` dispatches the Cases Audit button through the legacy list lookup. NEXT must branch before the legacy Case lookup/read and carry actual native Case/Candidate IDs; do not assume every NEXT seed has the legacy finance_case_id alias. The existing audit opener remains the shared entry, with the old path retained only for genuine LEGACY.

FE `js/main.js:277882` defines `openCandidateLoansOverpaymentsModal(candidateId)`. Its loadEntry `:278029–278037` calls `fetchCandidateAdvances` at277531, which reads legacy `GET /api/candidates/{id}/advances/report` at277540 and writes `appState.candidateAdvances`. Existing record launch `:280056–280080` and Pay mount launch `:329630–329670` converge on this opener. View-mode allowances `:327516–327519` and Active-only allowance `:327669–327673` establish that opening/filtering the read-only report must not put the Candidate into Edit or alter its dirty data.

**Guarding only the popup is insufficient:** Candidate Pay mount `:329619–329623` repaints the legacy cache and automatically fetches the same legacy report; `:280082–280088` retries it. Qualify these entry/read seams under NEXT as well, so a native report never starts beside authoritative-looking old-engine figures or leaks an old cache after a switch. This does not require a redesigned Candidate Pay tab: retain its navigation, and route or suppress the old finance fetch/cache/figures in the exact NEXT branch. Use one native report implementation; do not adapt NEXT rows through the permissive legacy normalizer.

Reuse the established `bankingNextOwnerClient()` at5061 and current FC10 `openBankingNextCaseStatus` at20590–20607 / FC09 Advance entry12990–13007: capture actor, exact stack prefix, parent frame/context reference; read fresh owner before any legacy work; require active admin/NEXT/unchanged owner epoch; return without late effects if caller/actor/parent changes. Pass these checks into the existing `CloudTMSBpayNextUi.createStandardWindowAdapter` (`js/bpay-next-ui.js:32–74`) and existing Office typed transport. Read-only FC14 needs no command journal, action UUID, auth challenge or job drain. Retry repeats the same failed read operands, not a financial operation.

## Minimum native reader facts

The current Worker/Office has CASE_COMPONENTS and CASE_LIST, **no CASE_AUDIT or CANDIDATE_CASE_REPORT**. Current final component owner is private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer) in2041; current Case list row owner private.bpay_next_case_list_row_v1(uuid,jsonb) is1939, while its bounded list page remains1738. Read the final registry, not an old literal plan filename.

CASE_AUDIT needs exact Case/Candidate and native event identity, occurred_at_utc, source kind/source ID/event ordinal, event kind, actor ID plus display text (NULL system actor rendered “Banking Pay”), genuine source address/reference, nullable source and target amounts, direction/sign semantics, reason and bounded human-readable details. Native cursor must bind Case/filter/sort and the full deterministic tie. R3 strengthens the old three-key tie to unique(source,id,eventordinal); two events from one original source must not collapse. No created_at fallback fabricated as an occurrence time, current mutable hold copied into several history rows, expired snooze fabricated as an ended event, raw technical JSON dump, browser financial derivation or pay date inferred from a week. Immutable create/edit/pause/write-off/NET/hold-transition/effect receipts alone establish history. Current FC10 request/outcome can supply actual pause/resume audit; missing stored hold transitions are the genuine D01 source gap, for the backend owner to close.

CANDIDATE_CASE_REPORT needs exact Candidate/header labels and typed bounded rows with section/item/native identity, factual frozen Client association, original approved principal, funded, recovered, written-off, reserved and remaining/outstanding figures, weekly amount, actual next due date, snoozed-until state, status and notes. Current1939 row `:3129–3154` already exposes native principal counters, outstanding, active_hold_amount, current schedule/snooze fields and honest gap codes; current2041 component rows supply exact typed component facts. Reuse those factual owners without summing both Loan components or subtracting a hold twice. Pending/unsupported facts remain NULL/“—”; “General / no specific Client” requires a native explicit absence, not a missing display-name guess.

The supplied report PNG omits Funded/Recovered/Written-off/Reserved columns. The **card's required native figures still need exposure**, through the smallest table/fact addition justified by those clauses, with phone labelled fields and corresponding server sorts where new data headers are shown. Document the visible difference; the sample cannot narrow the card. Summary totals/counts and Active-only semantics must be server facts over the whole admitted Candidate scope, not loaded rows. The old report `:278064–278113` uses browser sums/counts and277899 converts missing money to0.00; neither is reusable NEXT financial authority.

Keep existing supported business sections rather than asserting only the three sample rows exist. Automatic Overpayment is native Case provenance, not guessed CASE_CREATE. No new Underpayment producer is warranted. Timesheet snoozes shown by the retained report should come from existing native WB36/WB38 facts; do not create a snooze engine here. Exact source coverage and any honest unavailable row remain explicit in the reader contract.

## A3 and presentation obligations

The owner's A3 server-sort direction and the specs control these lists. SCR126 explicitly says **every heading sorts**; its seven data headers all need ascending/descending full-scope native keysets with the unique event tie. SCR111's nine displayed data headers likewise sort within fixed business sections; default Next date ascending. The section headers are fixed business order, not globally alphabetized. Phone Sort by must call that same server sort, never sort only retained browser rows. Current FC01 supports only Created/Candidate/type, which does not satisfy either new list.

`SCREEN_DESIGN_POLICY.md:164–174` requires <=100-row chunks, automatic continuous scroll, loading-more state, whole-list count/sort footer, deterministic ties and index support; no page numbers, Previous/Next or Load more. Scope sort/index support to these named readers and actual immutable/native keys. This investigation does not prescribe a global lifecycle projection or unrelated producer rewiring. Audit time-order alone is not sufficient once the visible Event/Who/Source/Amount/Reason/Details headers are admitted as sortable.

Retain dark-only under device-light preference, standard frame, Close only in header, one labelled scroll region, current shared UK date-time formatter, right-aligned tabular money, negative −£ formatting, unknown dash, phone44px controls/wrapping, plain single-sentence inline refusal and current focus/late-read fences. No pre-execution pay date. Report has no new Refresh or Export button (SCR111 reviewNotes); Retry is its failed-read recovery. Case audit may recover a failed exact read with a necessary Retry, not a mutation. Render every real event only once; clearing a resolution must not remove old events.

## Reference snapshots

| Source | SHA-256 |
| --- | --- |
| FE js/main.js | cb156512ef05e0d85d05edd7f599cad6fa415c170f3b2834d28b415cead6b2bc |
| FE js/bpay-next-office.js | ebf5237c32438a7ff3b1e94944d0c07221bc7391b027621ecd473a8fc8e433eb |
| FE js/bpay-next-ui.js | f535e7e75579428ca7b97b57d406ccaeadaebbf0fe15e5935574f435b772248d |
| BE broker/src/bpay-next-preparation.js | ccb6ef70bf87f5a6bef6ccf0482a5f30c34db75b4b20b2a00b4c4da7c73f9db2 |
| BE final-definer-closures.json | c4c498e15da29443c51341a37e0184e2f98c8ba7b035fc75e2bf195986aeeb0a |
| Current1939 Case list row | 7b723a61096ac50853dbc609acbba6437b403f11aa796a6f77e39a78b8308f66 |
| Current2041 component/pause owners | 80675b1f77af852977da427d0ce0702bfb2a2da6fafcea2a5e05b731810de44b |
| SCR126 PNG / spec | f9721c0000118e9afa917a0a76189068abf0230c26a537b773991d40a15f26f1 / cd813c9db5c6b0358d7ca1f559188736ebc044bd508e7e23b75b4acc267f65db |
| SCR111 desktop PNG / phone PNG | e19ef4cbbf1a8e68199ffa672832e2264987097bc13c55095e3aa7d318c5ab52 / 91cf0cf8ba1a8abbaf54c869f73d7808274ad0993e36592da1c27bd05a959a06 |
| SCR111 failed PNG | e14d66685b7968801bca1a818c1cded670d6b58ebba2bb18e45a5c09d705dc74 |
| SCR111 spec / failed spec | 3ef7a8af50c4c70ec6556b9c3898310b797c0db02f74da7c84eaa1ffdabb5485 / 81ef3312980704017ed07ba5108ce47949b93eb92b716dfd5cafff534efd839d |
| Policy prose / tokens | 8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5 / 45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d |
| Design index / inventory | cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe / 3b9b035de99847483c0111fd689dd2fc6d9f9589758f02744ec7b028c6727b2c |

Implementation/source/installation/A4 remain future work; joined/runtime acceptance remains AUTHORED_NOT_RUN. This investigation does not close FC14 or future EX50.
