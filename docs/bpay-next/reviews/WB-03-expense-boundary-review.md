# WB-03 expense boundary source review

Independent read-only review, 10 October 2026. Read the full WB-03 card (`02_WORKBENCH.md:153–184`), R2 KEEP disposition, all five child rows, `design/WB-03-investigation.md`, registry finals and the actual expense/correction/Magnit branches. **PASS for the current card's reuse/check scope; no concrete gap requiring new SQL, route, Worker or screen code was found.** No SQL, business/application execution, tests, fixtures, hosted reads or product edits. Joined checks remain NOT_RUN.

## Exact reused boundary

- Final route is `03102026_1450_banking_pay_next_producer_route_v1.sql`. It reads the real current, nonarchived, nonrevoked Timesheet/Contract (`33–45`); separate EXPENSES/MILEAGE returns EXPENSE_CARRIER (`47–48`) independently of the HOURS source-family branch. HOURS correction markers require the exact native import classification, adjustment/parent/correction identity and authoritative flag (`55–75`) before FACTUAL_IMPORT_CORRECTION. Malformed marked children refuse rather than fall into ordinary pay. Self-bill and genuine resolved imported families, including Magnit, return SOURCE_HOURS (`83–90`). No new Magnit expense route is appropriate.
- The final ordinary publisher is **1009**, preserving the full F05 2018 predecessor rather than reasserting RESET 1530. It takes this carrier's financial ID, requires its own current authorised TF and physical Timesheet/version/Candidate/Client (`77–97`), admits only ordinary/expense routes, and admits EXPENSES_ONLY only for an expense carrier (`139–142`). The carrier guard rejects hours/base/Additional contamination and reconciles its actual TF total to expense plus mileage (`152–159`). The work and revision are independently EXPENSE (`246–270`), keyed by the carrier's real booking/Timesheet. Amounts come from this TF's actual named/generic expense and mileage fields (`163–179`, `652–681`), not the hours Timesheet, a new calculator, or payment selection. Exact component count/total is checked before the unchanged atomic staged publication (`683–687`).
- Original 1530 authorisation trigger remains the final owner. Only a genuine new authorised financial transition under NEXT calls the publisher, and only for ORDINARY_HOURS/EXPENSE_CARRIER (`623–640`). SOURCE_HOURS and FACTUAL_IMPORT_CORRECTION cannot accidentally use this ordinary/expense branch. Unauthorisation changes future work approval/pointer state while retaining paid/frozen revisions (`643–679`); it does not approve/reject a separate claim from Banking selection.
- Current Source stage 1550 accepts **only SOURCE_HOURS** (`108–109`), requires its genuine authorised initial TF/root authorisation or exact committed current HEAD (`116–201`), and checks the complete approved inventory/basis. It explicitly accepts SOURCE_FIXED_EXPENSE (`302–310`). Each actual immutable component ID becomes one unique `SOURCE:<UUID>` line, consumes the real pay amount/exclusion flag, and maps the fixed expense to EXPENSE (`406–452`). Exact event replay precedes rediscovery (`87–105`); core's unique source event and `(revision_id,component_key)` prevent a second accepted event/duplicate component from silently adding pay. The stage contains no write to the Source component's Client charge.
- Original Magnit occurrence reader 2314 is evidence of the existing mapping, not a new NEXT runtime dependency. Its fixed-expense CTE separately sums actual pay and charge (`489–510`), anchors one EXPENSE_CODE EXPENSES at the first real fixed-expense ordinal, emits both values in the original mapping witness (`576–587`), and explicitly limits SOURCE_FIXED_EXPENSE emission to that anchor (`608–609`). No extra EXPENSE_CARRIER is needed; it would duplicate the Source ownership. The corresponding ordinary expense representation also retains distinct TF pay/charge (`441–470`).

## Approval-write check

Repeated the requested source-only case-insensitive `git grep` over saved `*banking_pay_next*.sql` repeatables for physical Timesheet/TF UPDATE/INSERT and assignments to `authorised_at_server`/`authorised_at_utc`; it returned no match. A case-insensitive `rg` cross-check includes untracked saved files and also returned no physical UPDATE or approval assignment. Named publisher/trigger/stage inspection confirms genuine approval is read while writes are to NEXT work/revision/line/position state. This finite current-source evidence does not certify future WB-25 selection code; that owner must retain the same boundary. No current NEXT claim-approval rewrite is justified.

## DR-38 and later owners remain explicit

WB-03's D04 is a **handoff note**, as the exact card requires: current 1009 Additional/EXPENSE/MILEAGE inserts still omit `tax_treatment`, while WORK is TAXABLE. Source 1550 assigns TAXABLE only for WORKED_TIME (`436–442`). DR-38 must apply existing OD-03 vocabulary—Additional TAXABLE, expense/mileage NON_TAXABLE—and retain fixed reimbursement amounts without channel conversion. This review does not claim that future assignment has been implemented.

The card names no Worker change. Expenses display belongs to WB-15/WB-16. Current route/publisher reuse does not close the later genuine contractless Daily seam or Workbench projection/refresh obligations (WB-36/WB-08/WB-44), and does not revive parked F25 producer/protocol/resolver work. The four future app checks—separate £45 carrier, actual Magnit pay/charge exactly once, untick preserving claim approval, correction pairs producing no payable child—remain NOT_RUN. The independent current source check supports a truthful no-new-code WB-03 completion receipt, not an installed/app observation.

## Fresh source hashes

| File | Raw SHA-256 |
|---|---|
| Route 1450 | `a8f9dc118ad75b62b3863820e38bd29cbc4c05aa47facf12c11e45b9013bfc74` |
| Final ordinary publisher 1009 | `34227084b44b53abfb441b2b2826d9ce437fdb20e636e7d3d1d3f54bf324c7d7` |
| Original trigger/source 1530 | `df943acaa6ddc4fa5bea877621762e96c93360d4030383f29b5bffca2d3aa301` |
| Source stage 1550 | `5388ce61af1cbd27acfdd4eeb69cc9654247d818e18745200bdd9557e83584b2` |
| Magnit original occurrence reader 2314 | `ad9af3abdac58406d11cddcef287c58a072151b7c4a231f8f363ec134c32216c` |
| Final-definer registry | `2dc51e7f79fb45b97905af6bcdc39bef57d172c57e53c111af2ce2ab7b0fd8e4` |

The reused route, original trigger, Source stage and Magnit evidence reader have no working-tree diff at this checkpoint.

## Final completion-note review

Read the fresh saved `WB-03-COMPLETION.md` (raw SHA-256 `7ca11e77488d963def409257d9a80524c8a3fe53f3ec1935c288233f01c8a89d`). **PASS**: the route, own-authorisation/expense-shape, complete component totals, imported-child refusal, separate Magnit pay/charge and single real-anchor line references agree with the inspected current source. D04 explicitly closes the required documentation handoff while actual DR-38 tax assignment remains unbuilt; it does not claim NULL tax is non-taxable storage. The note keeps WB-01 maintained projections open, later Daily/display owners separate, and actual app/installation/business checks NOT_RUN. No additional code delta or source correction is required for this note.

Fresh reread after the completion receipt added its local-check/commit/status lines: raw SHA-256 `0f563d1bebad8d368b639017e13b2bf4a4842226f345cb0214e02aee91b1dc71`. The source-line and D04 handoff verdict remains **PASS**. The note still expressly identifies actual DR-38 tax code as unbuilt and actual app/business checks as NOT_RUN. This independent review read the source and receipt; it did not rerun or independently attest the root's reported local checks.
