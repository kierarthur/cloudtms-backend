# PS-15 exact scope / BD039

Sequence35; dependency F-06 built/reviewed. Full card06:1636–1660, all checklist rows and controlling R2§7 re-read before product construction.

| Required row | Exact named object / work |
| --- | --- |
| D01 | Existing03102026_1324_banking_pay_next_core.sql named schemas: module_control; valuation_policy/window/control; work/revision/approved_line/shift_detail/rate_detail/rate_schedule/break_detail/source_chosen_detail/position; finance_case/case_event/financial_effect; pay_run/selection_page/run_selection/selection_candidate/run_worker/run_work/run_position/run_line/hold/case_hold; net_projection/transfer/return_cash/transfer_member/transfer_outcome/csv_instruction/worker_control; command_clock/command/run_command/command_member/paye_net_request/outcome_request/return_request/reissue_request/cancel_request/publication/enrollment_clock/job, each with the private.bpay_next_ prefix. Inventory exact point/keyset support; add only an evidenced missing index via a new migration. |
| D02 | docs/bpay-next/PSC_CLOSURE.md exact list/detail/export contract gaps and indexed/security inventory. |
| D03 | public.bpay_next_payment_history_page_v1(jsonb) scoped controlling successor onto current0623→0614→original1206. |
| D04 | broker/src/bpay-next-history.js actual authenticated bounded history adapter; R2§7 narrows older proposed Summary adapters to existing reader compatibility. |
| D06 | PS-15 future joined in-app checklist, NOT_RUN. |
| D05/D07 | Baseline receipt and Summary-policy reference are CONTEXT, not additional product tasks. |

BD039: retain the correct52-section history/frozen-child contract and its existing exact core/Case/F-06 indexes. No missing index is evidenced, so no redundant migration, schema/ACL/policy change or stored totals rebuild. New controlling0653 full successor composes the original1206 reader after0623/0614, adding exactly the six already captured revision display fields.0653 is the one final physical definer. Its existing authenticated Worker adapter mirrors that minimal field delta. Existing history SHIFTS/BREAKS/RATES/SCHEDULE provide state-independent immutable itemised detail; operational Draft-review /frozen-detail guards remain with their existing owner. Do not call that guarded endpoint state-independent. No new Source/Pay Query/MyTMS/Manager/invoice/money owner, trigger, ledger, protocol or pricing lookup.

Owner start-sheet AUTH-PS approval is already binding; S1 will be built at sequence287 with its approved policy/design/recommended choices. R2§7 and file07 assign its new list/filter/export signatures to that later screen step. PS-15 records those accepted requirements and precise ownership now; it does not invent an earlier competing API or wait for another approval. Existing live keyset history is not a consistent report-export snapshot. Unsupported export/filter payloads must explicitly refuse before RPC, never become partial totals.

BD039 refinement after source controls: captured revision display fields exist in core1324 but were missing from historical REVISIONS transport. New0653 now defines the scoped original1206 body with exactly those six fields, preserving all other reader behavior/signature/grants and PS-07/PS-11 semantics. Existing Worker and Office decoder schemas mirror that minimal change. Current finalRoot/finalPhysical0653 is singular; no index/capture/schema change.
