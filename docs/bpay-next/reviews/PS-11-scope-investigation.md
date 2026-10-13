# PS-11 source scope investigation

Read-only investigation, 10 October 2026. No SQL, tests, runtime, business data, network or fixture execution. No product files changed. This records source requirements, not installed or connected acceptance.

## Exact owner scope

Card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1549–1573`, sequence33, dependencies F-06/F-16. PS-C12/RQ-288 and the RQ-274 Summary clause require one recorded Candidate transfer NET, complete frozen workings, genuine zero distinct from NULL, and no Timesheet/Client NET or cash allocation. £100 earning less £30 recovery is a £70 earnings basis before payroll deductions; it is not an invented PAYE payment. The local checklist has five required objects:

| Row | Current object | Minimum completion |
| --- | --- | --- |
| D01 | `public.bpay_next_payment_history_page_v1(jsonb)`, original1206, current controlling0614 PS-07 closure | Re-read/reuse the existing immutable projection/request/transfer graph and record the precise PS-C12 authority in the scoped controlling successor. No monetary owner rewrite is needed. |
| D02 | BUILD-FE `js/bpay-next-payment-history.js` | Add precise missing Candidate NET versus intrinsically unallocated line presentation to the existing history detail. Preserve exact money strings, pagination and existing approved modal. |
| D03 | Existing history source/unit checks | Add finite positive/negative controls for NET65 once, zero, NULL, line unallocated and forbidden allocation. Reuse existing files; no new programme/framework. |
| D04 | Future joined in-app TEST checklist | Record the exact named journey as NOT_RUN; no fixture or runtime proof is authorised here. |
| D05 | BUILD-BE `broker/src/bpay-next-history.js` | Add/reuse a finite typed NET classification/decoder at the existing history boundary; never calculate a Timesheet/Client share. |

No new route, Screen policy/layout, financial owner, PAYE entry producer, monetary schema, Source producer, resolver or accepted-input protocol is required. Rule-B/F25 stays closed. AUTH-PS controls any future Summary layout/filter policy; this card can finish the accepted existing history labels/data semantics without constructing that Summary.

## Correct current source to preserve

* Original1206:40–46 explicitly preserves string-or-null scalars, separate SOURCE/target/whole-worker NET/whole-transfer cash, and member contribution rather than Timesheet cash allocation. The closed52-section registry exposes immutable `PROJECTIONS.entered_paye_net`, `NET_REQUEST.entered_paye_net`, `TRANSFERS.projection_id`, and the exact captured RUN_WORKS/RUN_LINES/approved revision workings. RUN_LINES has frozen earnings/source fields and **no recorded line NET field**. There is no SQL proportional allocation, current-pricing reconstruction, or sum of transfer cash by Timesheet.
* Original core1324:629–686 binds each original transfer to `(projection_id,run_worker_id)` by an actual composite FK. Re-entry appends a new projection; original transfers retain their accepted projection. `PROJECTIONS` includes retired projections deliberately. Choose the transfer's exact `projection_id`, never the latest projection, the current worker's `entered_paye_net`, or the first projection page. `NET_REQUEST` is the actual immutable accepted request; the graph preserves its `request_command_id`.
* Core projection `input_kind='UMBRELLA'` means `entered_paye_net IS NULL` is intentional, not missing PAYE entry. Actual PAYE request NET is numeric(18,2), nonnegative and NOT NULL. Worker `entered_paye_net` is a labelled **current worker** value, not historical transfer authority. Reissue has no fresh projection and must retain its original transfer/return-cash lineage rather than invent a second payroll NET.
* FE history `createClient` already retains exact transfer/projection records and checks their worker identities. `childLinks` opens recorded projections from a transfer and exact accepted request from a projection. PS03 `memberDescription` uses those retained references; the signed NET_ADJUSTMENT member is a constituent adjustment, not entered NET itself. Complete frozen child paging remains required; do not replace it with aggregate or truncated workings.
* Existing Worker validates closed request/response fields, parent/cursor/scope and string-or-null data; it forwards the accepted page unchanged and introduces no financial arithmetic. Existing history remains authenticated admin informational evidence independent of financial activation/worker READY. D05 must preserve that contract and PF13 environment check.
* There is no history CSV export, print calculator, Client allocation or Summary filter implementation in this card's current module. Existing issued-payment CSV is a whole-transfer receipt, not a permission to distribute NET. Preserve that boundary; do not build a new report/export feature to create a test target.

## Concrete gaps and minimum implementation seams

1. FE `displayScalar` at326–332 returns generic **“Not recorded”** for every NULL, including `entered_paye_net`. The card explicitly requires **“NET not entered”** for genuinely missing Candidate PAYE NET. A blanket `field==='entered_paye_net' && value===null` rename would incorrectly mark an Umbrella projection as missing: retain section plus recorded `input_kind`/`target_pay_channel` context.
2. FE `amountNotice` at349–353 labels whole-worker versus whole-transfer cash, but RUN_LINES has only the generic notice. Add explicit **“NET unallocated per Timesheet/Client”** semantics on the line workings. There is no line NET scalar to decode; do not add a fabricated zero/NULL monetary field or use its absent field as evidence that Candidate NET is missing. A known65 and unknown Candidate NET both leave line NET unallocated.
3. Worker string-or-null response validation currently has no NET-specific finite semantic decoder. Use a small pure helper over the existing closed section and retained row, with a finite result such as entered/not-entered/not-applicable/unallocated and the unchanged money string when genuinely present. The function must validate relevant discriminator/decimal shape and preserve `0.00`; it must not infer money from earnings, recovery, member sums or current policy. FE may mirror the same finite rules through the existing module, without a new network contract.

Minimum classification authority:

| Existing source | Honest meaning |
| --- | --- |
| WORKER/COMMAND_WORKER PAYE + NULL | Candidate NET not entered; explicitly current worker state. |
| WORKER/COMMAND_WORKER UMBRELLA + NULL | PAYE NET not applicable, not “not entered”. |
| PROJECTIONS PAYE_MANUAL/PAYE_IMPORT + exact nonnegative decimal | Recorded accepted Candidate NET; zero is real zero. For a transfer, bind exact transfer.projection_id first. |
| PROJECTIONS UMBRELLA + NULL | PAYE NET not applicable. |
| NET_REQUEST + exact nonnegative decimal | Immutable accepted NET input; no NULL-to-zero conversion. |
| RUN_LINES frozen workings | NET unallocated to the Timesheet/Client, regardless of whether Candidate NET is known. |
| TRANSFERS without the exact accepted projection loaded | Show recorded transfer cash and reference; request/open exact projection. Do not say “NET not entered” from missing client cache. |
| CASH_REISSUE | Actual new cash leg and retained original/return lineage; no new PAYE NET entry manufactured. |

“Read NET once” means render the exact original transfer's recorded accepted projection once for its cash row/detail context, without duplicating it onto each WORK/Timesheet or summing repeated references. Current existing graph already returns the necessary immutable rows; no new scalar cash calculation or new table is justified. If a small SQL contract metadata change is selected, mirror it exactly in both existing52-section allowlists and existing source checks, retaining signature/config/grants/cursors. Do not widen the financial transport or return current worker NET as historical transfer NET.

## Shared final ownership and finite source checks

The authoritative shared history chain in `01_BUILDER_RULES_AND_FOUNDATION.md:480` and `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:71` is **PS-07 → EX-41 → CM-18**. PS-11 has no independently assigned numbered chain slot. Current registry finalRoot is `10102026_0614_banking_pay_next_ps_c08_history_authority_v1.sql`, including original1206 unchanged. PS-11 must merge onto that current controlling owner, retain the captured/state-independent PS-07 authority, and make the registry designate one final closure which EX-41 then CM-18 re-read. Do not overwrite historical1206/0614 or create competing final definitions. If existing D01 behavior is reused, record the source proof and the narrow closure comment honestly, as PS-07 did.

Reuse BE `tests/bpay-next-payment-history-caller.test.js` and FE `tests/unit/bpay-next-payment-history.test.cjs`; extend the normal existing history cases rather than creating business fixtures. Finite meaningful controls: PAYE65 over two captured line records yields one unchanged65 authority and two unallocated line notices; `0.00` is entered; actual PAYE NULL differs from Umbrella NULL; cache absence differs from not-entered; retired exact transfer projection wins over newer worker/projection data; NET adjustment is not substituted for entered NET. Negative source controls should fail an inserted allocation/division or line-NET scalar, a truthiness/default-zero conversion, and a latest/current projection replacement. Preserve current generic typing, actor/parent/cursor/containment/security negatives; do not weaken them to accommodate this card.

D04 remains NOT_RUN until the owner-authorised later connected TEST journey proves the named two-Timesheet65, zero/NULL and earnings-basis clauses. Source/catalogue/compile evidence is not that journey.

## Raw source hashes at investigation

| File | SHA-256 |
| --- | --- |
| original1206 history SQL | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| current0614 PS-07 closure | `abecabcaa00883d1ebd2735c4f28df313a8a6161f4dce71272a57b4bd590d340` |
| BE `broker/src/bpay-next-history.js` | `b5bb109e546787fc65a83df1916dfd29c1b21d32ae6372fd3fd04ae97baa0d50` |
| FE `js/bpay-next-payment-history.js` | `66dbe8750a28e051d5c4f29a9e1c6742b6186af830255a510ea3ef1219f3c750` |
| BE existing history caller tests | `ffe2265114027202e988efa2aecabdc14d5f2fe0b3a424f67281bdcff93c7378` |
| FE existing history tests | `3e9a279f30363cf830863298b66844969a8a2da0fd7a672a86d1a9f8a174a502` |
