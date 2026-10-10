# PS-03 typed member-origin investigation

10 October 2026; read-only source. Full card 06:1375–1393 and exact checklist D01/D02/D05/D06/D07 plus shared D03/D04 reread. Dependency F-05; PS-03 has no 0130 editor-chain position. No product edit, SQL, execution, business function, test, fixture, hosted call or data observation.

**Reuse the existing disjoint schema and actual builders. The evidenced gaps are ordinary-member insert enforcement and explicit typed member-origin read/display. Do not create literal run-line/Case FKs for NET or repayment, or jump the 0130 chain.**

## Real origins and existing enforcement

| Kind | Genuine frozen origin | Existing enforcement and owner |
| --- | --- | --- |
| WORK | member.run_line_id → exact run_worker frozen run line, its captured revision/approved line and frozen contribution | Core1324 FK `(run_line_id,run_worker_id)` and transfer/worker FK. 0400 shape requires this one direct origin and forbids all Case IDs. Current ordinary BUILD is PS-01 final0533, preserved F05 body plus bridge capture. It inserts the exact frozen_inc_vat; ordinary Umbrella qualification retains same-channel taxable WORK and refuses Case/conversion. Case0410 and destination1827 validate their own retained WORK basis. |
| CASE | member.case_instruction_id + case_allocation_result_id; nonzero result also exact Case hold | 0400 adds real instruction/result FKs, disjoint shape and one-per-transfer instruction index. Final0410 guard requires a genuine Case/destination build, correct worker/preparation/selection, original DRAFT or NET allocation state according to payroll stage, exact signed allocated TARGET INC and exact ACTIVE hold/result tuple for nonzero allocation. A real zero allocation carries its instruction/result and **no hold**; requiring all Case FKs nonnull would reject valid provenance. Direction decides the existing stored signed contribution; no new valuer is needed. |
| NET_ADJUSTMENT | member.transfer_id → transfer.projection_id → accepted projection.request_command_id → exact accepted PAYE/Case NET request | All literal run_line/Case origin fields are deliberately NULL. Ordinary0110 stores the accepted request command/worker and payroll NET in the immutable projection. Simple0533 retains the existing one adjustment `projection.cash_amount − worker.gross_inc_vat`; Case0410 uses its existing final-stage `coalesce(projection.entered_paye_net,0) − projection.gross_inc_vat`, since separate CASE members already carry their changes. Destination1827 routes NET only to OWN and records its unique group subject. Never apply the simple adjustment formula to Case/destination owners. |
| CASH_REISSUE | member.transfer_id → actual transfer.original_transfer_id + return_cash_id; actual CASH_REISSUE command/request and return obligation | Literal run-line/Case fields are NULL. Core transfer FK `(return_cash_id,original_transfer_id,candidate_id)` binds the actual obligation; original transfer/Candidate/worker lineage is retained. Build0240 inserts one CASH_REISSUE member for the exact constructed leg cash and reserves that same obligation. Final receive is0316. Later outcome posting verifies amount and cash hold, never reopens payroll or fabricates another earning origin. |

Core1324:727–740 is the historical shape, not the final one. Migration0400:69–78 extends it correctly with instruction/result identities and the legitimate zero-result exception. Current0145 member guard denies UPDATE/DELETE and all insertions outside BUILDING. Projection0390 freezes the full projection; PS-01 final0533 freezes captured transfer bridge values and original transfer identity. Original member sums remain server-owned: builders accumulate the bounded actual contributions, core MEMBERS_READY requires stored sum=cash, and the UI must not total a partial history page.

## Evidenced enforcement gap

Final0410:148–159 returns NEW for non-CASE members when there is no Case/destination build. In that branch, current0145 checks only append-only/BUILDING and the schema checks origin shape/same-worker FK. Consequently an ordinary WORK insertion with a genuine same-worker run-line but a different signed contribution is not rejected by the insert guard. An ordinary NET_ADJUSTMENT or CASH_REISSUE can similarly carry the correct all-NULL literal shape without insert-time validation of its actual projection/request or parent-cash relation/value. Actual builders write correct values and later posting catches some conflicts; that is not the requested DB refusal of a malformed member insertion.

The narrow support is an INSERT-only member-origin guard over the existing table, or the corresponding finite successor of the actual member guard. Reuse existing UPDATE/DELETE protection. Validate WORK against that exact frozen row; validate ordinary NET against the transfer's retained accepted projection/request/worker and the existing simple-owner difference; validate CASH against the real reissue request, parent transfer and return-cash tuple, with contribution equal to that leg cash. Reject NET on Umbrella/no accepted NET origin, CASH on an original transfer, and CASE without the existing Case/destination binding. Let the existing0410/1827 owners validate their supported Case/destination branches rather than duplicating or changing their calculations.

Also inspect the absent one-per-transfer NET/CASH uniqueness: there is a real WORK uniqueness index and Case-instruction index, and destination-group subject uniqueness, but core/0400 have no NET/CASH per-transfer uniqueness. Ordinary owners emit each once. If the new malformed/multiple-origin refusal relies on an existence test, ensure concurrent inserts cannot admit duplicate NET or CASH origins; a finite partial unique index for those singleton kinds is preferable to a new historical aggregate or counter. No new member-origin columns or fabricated run-line/hold FK are necessary.

CLAIM stays0130 and BUILD stays final0533. A supporting member guard/constraint is not an early DY-09/DR-38/DR-22/CM-13a BUILD rewrite. Umbrella comparison/Case producers remain their actual later owners; do not label them available merely because the CASE schema exists.

## Current 52-section read and renderer

Current `public.bpay_next_payment_history_page_v1(jsonb)` is final `06102026_1206`. Its fixed MEMBERS projection is:

`transfer_id, member_no, run_worker_id, subject_kind, run_line_id, case_hold_id, case_instruction_id, case_allocation_result_id, signed_cash_contribution`.

Broker `bpay-next-history.js:31` and FE `bpay-next-payment-history.js:30` exactly mirror that allowlist. Parent is TRANSFER; member_no is the native key; amount scope explicitly says contribution, not Timesheet allocation. Reader checks Candidate/Timesheet ancestry before bounded keyset paging, limits one section to100 rows, keeps exact scalar STRING_OR_NULL and carries live-keyset/refresh consistency. Preserve all of that.

WORK/CASE direct IDs and contribution already reach the UI intact. NET/CASH origin is presently only **implicit through the parent transfer**, whose existing TRANSFERS fields carry projection_id/build_command_id/original_transfer_id/return_cash_id. PROJECTIONS supplies accepted request_command_id; COMMAND→NET_REQUEST and REISSUE_REQUEST/RETURN_CASH preserve actual lineage. This is genuine stored provenance, not an absent origin, but the MEMBERS row does not expose those parent-origin IDs explicitly.

FE currently renders a generic `<dl>` of all row fields and exact raw money strings. `childLinks` has no MEMBERS branch and no typed member-origin presentation; the NULL direct-origin fields of NET/CASH become generic “Not recorded”. Do not certify this as an implemented typed origin renderer merely because the IDs can be found elsewhere in the graph.

Minimum D06/D07 choices:

- Prefer retaining the same52 sections/endpoint/cursors and extending only the MEMBERS read projection with its genuine conditional parent references if explicit per-row origin IDs are required: NET projection/request IDs; CASH original-transfer/return-cash IDs. A bounded fixed parent join is supporting read-only SQL, not a new producer, calculator or all-history scan. Mirror only that exact field change through broker and FE allowlists; do not add unrelated PS-01/02 columns or require every kind's origin columns nonnull. These IDs should be NULL for other kinds.
- Alternatively reuse the existing validated parent TRANSFERS/PROJECTIONS/command pages as explicitly retained display context, provided the implementation binds the exact member.transfer_id/worker to that context and never selects “latest” or silently guesses a missing parent. This preserves the current wire fields but must make the implicit lineage visibly typed; generic row printing alone is insufficient.
- A small FE typed origin renderer should name WORK frozen line, CASE frozen instruction/result (and optional hold), accepted NET projection/request, or reissue parent cash. Display the server's signed_cash_contribution exactly, distinguish NULL from zero, keep the whole-leg server member_cash_sum separate, and retain the no-Timesheet/Client-allocation notice. Do not parse/round/sum money or infer origins from amounts/current mutable worker settings.

D05 remains a finite future joined checklist: genuine typed origins for all four kinds, wrong/foreign/multiple identities and contribution refusals, valid zero CASE with no hold, actual accepted NET and returned-cash links, ordinary Umbrella supported WORK, exact replay and one-leg server sum. All actual Worker/queue/minute/app/installed/business checks remain NOT_RUN. PaymentSummary layout remains AUTHPS-unapproved.

## SHA-256 at investigation

Selected-plan viability, 10 October: a finite0145 guard successor and singleton partial index are appropriate supporting pieces; keep the append-only/BUILDING check before any exact Case/destination exemption. The CASH request must be joined by retained build command/cash/worker/Candidate while allowing its transfer_id NULL at0240's member-insert point, before cash reservation and attachment. Ordinary NET must distinguish the original simple request from a genuine Case NET request; preserve0410's distinct formula and separate CASE members. No CLAIM/BUILD chain jump is needed. Final BD033 selected the alternative validated existing-page graph approach for D06/D07, so the earlier preferred SQL MEMBERS extension is not a requirement: the52-section SQL reader stays unchanged, exact parent context must be bound by native IDs and absent context honestly labelled. Worker/Office implementation is reviewed separately.

| Source | SHA-256 |
| --- | --- |
| core1324 | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| shape0400 | `1a7ca4f86eca7e96775ad4cfc47a8165768d1c41ad87f6f81752555d2648c987` |
| current0145 member guard | `2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c` |
| PS-01 final0533 BUILD/transfer guard | `a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314` |
| Case0410 | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |
| destination1827 | `33891038c2865684c091f7bd3df4ce188bb42e026129163216e3bc341feb198c` |
| simple projection0110 | `d74d8a9629ef245eb8940c4026e0ebd2b99d9d81a72b96708c72673164669c1e` |
| reissue0240 | `5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714` |
| SQL history1206 | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| broker history | `81d4ca2fb90b1bbec7dea3a41698933f70b306833079484d45a89e4e77738451` |
| FE history | `e6efd41005e6c80808570ea1d92c25f3e16adcf1eb8afb5cf1835444cf7323c5` |
| registry | `83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc` |
