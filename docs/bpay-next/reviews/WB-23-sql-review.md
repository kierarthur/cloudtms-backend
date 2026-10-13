# WB-23 SQL source review

Verdict: SOURCE PASS for the finite WB-23 SQL scope; F1 is closed by the new 2240 successor. No whole WB-23 UI/runtime acceptance is claimed. Installed 2232 files remain immutable.

Reviewed full 2232 migration and all eleven routine definitions, current registry, corrected BD076 design, exact 1832 accepted CREATE/ACK/seal/intake bodies and full 2026 preparation reader. Static source/hash/diff review only; no SQL, tests, business operations or runtime executed by this reviewer.

## F1 — terminal page cleanup blocked (closed by 2240; original finding retained)

2232 repeatable lines143–159 adds unconditional UPDATE/DELETE/TRUNCATE rejection on `private.bpay_next_selection_page`. Current PF-04 owner `10102026_0406_banking_pay_next_terminal_cleanup_v1.sql:39–45` locks the actual run, permits only COMPLETE/CANCELLED, and deletes precisely these terminal temporary pages. Every such DELETE now raises `23514/BPAY_NEXT_SELECTION_PAGE_CONFLICT`; its entire cleanup call, cursor progress and lane switch roll back. An eligible retained first page can repeatedly prevent both cleanup lanes progressing. This also affects ordinary pages, not only new matching receipts.

Small correction: new successor of the receipt guard preserves UPDATE/TRUNCATE rejection and all nonterminal DELETE rejection, but permits DELETE for the actual OLD.run_id root in COMPLETE/CANCELLED and returns OLD. This preserves the already-approved PF-04 distinction between disposable closed transport pages and permanently retained choices, snapshots, commands and financial facts. Reuse the existing root-first cleanup owner; no new cleanup/economic command or broader money-owner change is needed. Terminal late page retries retain the existing unavailable/refusal semantics.

## Reviewed source closure

| Boundary | Source finding |
|---|---|
| Accepted cut / SUBSET prefix | Original 1832 CREATE is unchanged. Its WORK row_number sorts SUBSET before ALL, then native Work UUID. ACKv2 retains exact contiguous attempted-member and eligible-selection counters; no out-of-order relaxation or member update was introduced. |
| Exclusions | Combined ≤100 IDs, exact actual captured page membership, UUID-native comparison including duplicate rejection through set/count equality. Exact immutable request/result replay precedes closure checks. First matching closes fresh exclusions; an empty exclusion set creates one bounded receipt and seal requires that receipt. |
| Matching | Only original ALL_MATCHING accepted descriptor; all SUBSET prefix members must be acknowledged and eligible SUBSET choices segment-sealed. Original uploaded count is the cursor; native member ordinal index bounds each page at100. Existing exclusions and held prefix cannot produce duplicate choice append. Captured skips are retained, never requalified against later live revision/query state. |
| Atomic replies | ACKv2 writes immutable request, attempted IDs, skips and captured result in the same INSERT/counter transaction. Empty matching pages are legal only with a matching request. Matching complete/next_after_member_no are derived from the actual remaining captured member set; replay returns the captured outcome. Legacy pre-2232 ACK fallback remains exactly preserved. |
| Seal | Full 1832 seal preserved except exclusion receipt count becomes max(1,ceil(n/100)); actual choice/component/segment/query-snapshot and Case selected-item checks remain. No new freeze/pin/agency-clock authority. |
| Input readback | Existing WORK_CHOICES kind has a closed accepted_inputs=true variant, after_member_no nullable bigint, limit1–100. Same run actor/module epoch/accepted command is checked. Six actual immutable member fields plus four native run counters distinguish captured skipped attempts without fabricated choice state. Native ordinal uniqueness/contiguity supports its end predicate. |
| 2026 preservation | Public preparationPage diff contains only WORK_CHOICES alternate keys and its early retained-input return. Every existing detail branch, Component/Shift/Break/Rate fact, current-revision check, byte packing and normal post-seal WORK_CHOICES shape is unchanged. |
| Authority / schema | Existing service/admin/NEXT wrapper admission remains; private helpers are owner-only with all browser/service direct EXECUTE revoked. Both public signatures remain service-only. Migration changes only private transport receipt metadata/constraints/index; no public trigger, financial command kind, producer, economic snapshot or parked F25 protocol. |
| Registry / scope | All eleven actual definitions have current finalRoot/finalPhysical2232; predecessor chains retain1832/2026. This is the narrow D06 selection-enumeration support; context rows are reused, not duplicated economic deliverables. Actual Worker/UI phase joins and remaining WB26/DR follow-up ownership are separate source work, not certified here. |

## Source snapshot SHA-256

- `supabase/migrations/10102026_2232_banking_pay_next_selection_page_receipts.sql`: `6ece229f994bfc8229a66bd4b9102e6d3353c7e9bdd2b10a4df7f0f3f4b02125`
- `supabase/repeatable/10102026_2232_banking_pay_next_selection_pages_v1.sql`: `cc1491f55ff1f4afe48f82e8892015aa2f91500f42d0d41d692f0e6b78dd3e48`
- `supabase/repeatable/10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql`: `b629fcc003b76ae61ec58c0d35ad2256a4cf9f736e307f5dc80e0a8fb0e20438`
- `supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql`: `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a`
- `supabase/repeatable/10102026_0406_banking_pay_next_terminal_cleanup_v1.sql`: `434704fd74342fc461eb30ce09d812ff51eea2a05e83be42af9a14a2203fe4c8`
- `docs/bpay-next/final-definer-closures.json`: `e07a0c0ed33ca2ca0576402710d591c3cf431d74b3588e49dcfe6422fc085c47`
- `docs/bpay-next/design/WB-23-selection-pages.md`: `659de41afbf37a9ce634ad83262b91d422f2f8de1c67d7082705e3e87049c668`

## Fresh 2240 F1 closure

Read the entire current `supabase/repeatable/10102026_2240_banking_pay_next_selection_receipt_cleanup_guard_v1.sql` and its exact registry chain. DELETE checks the actual OLD.run_id root with `FOR UPDATE` and allows only COMPLETE/CANCELLED, returning OLD. This is reentrant with PF-04's existing parent-before-child root lock; that owner and all permanent financial/choice facts are unchanged. UPDATE and TRUNCATE always raise the original23514 conflict; nonterminal or missing-root DELETE also raises. Existing2232 trigger bindings reuse this exact guard signature. Owner/revocations remain private, no public exposure.

Only this guard chain now points to2240, with2232 retained as its predecessor; the other ten reviewed definitions remain at2232. The original2232 raw hash remains `cc1491f55ff1f4afe48f82e8892015aa2f91500f42d0d41d692f0e6b78dd3e48`. No new migration or change to the cleanup/economic owner is required. Source review closes F1 and finds no remaining finite SQL defect. Root's compilation receipt is separate; this reviewer did not run SQL/tests/runtime.

- 2240 raw SHA-256: `36c7a3aac98fc13431e03f4534ba3ca1242cef2fc735775ebe890e412bb1fb45`.
- Current registry SHA-256: `243b5a5d765b4cdca268fc0e6e75b4c26c72e1e51d3f3486d242e7a06446cb8c`.

## SQ01 2245 retained-input resume context — SOURCE PASS

Read full current2245 and exact function diff against2232 plus SQ01 in `WB-23-sequencer-review.md:7–11`. The original component owner permits short/reordered legitimate pages, so receipt page number alone does not imply100 consumed components. Exposing actual stored choice progress is a necessary narrow resume aid, not a new financial owner.

`private.bpay_next_choice_inputs_page_v1(uuid,bigint,integer)` adds only three nullable genuine Work-choice fields: `choice_state` from selection_state, `component_page_count` from page_count, `component_count` from component_count, by LEFT JOIN on exact run_id/work_id. Native work_choice has that unique key; captured skipped members have no choice and therefore return JSON null, without invented OPEN/SEALED/count zero. Original six accepted member fields, native member ordinal cursor, four counters and end predicate are unchanged. Inputs remain original run/after/limit; no new kind, route or command.

The actual root is now `FOR SHARE`, so the standard parent-first upload mutation owners cannot advance native progress between root counters and this bounded read. No Candidate/business mutation, new clock or money lock is added. ≤100 member rows and a unique-key factual join remain; fixed additional enum/bigint fields stay below existing byte envelope. Public2232 accepted-input actor/epoch authority still owns dispatch; helper owner/revocations stay private. Immutable2232 and cleanup guard2240 are unchanged.

Registry only this helper chain2232→2245 was reviewed. SQL scope remains SOURCE PASS. This certifies factual resume context only; sequencer handling of missing native components and its own SQ01/SQ02 closure is the separate UI source review, not inferred from these fields. No SQL/runtime/tests executed by this reviewer.

- 2245 raw SHA-256: `e1c9936055a24b271b887ee431a8f8dd4e2a224d1b8bb184f3e709534e1fabb4`.
- Current registry SHA-256: `922914abd78e312ea8b253a17d62bcd35bed967b96ef28490e8a781d5dd6da81`.
