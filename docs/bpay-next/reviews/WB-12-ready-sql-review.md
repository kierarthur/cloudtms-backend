# WB-12 Ready SQL review — 10 October 2026

SOURCE REVIEW: PASS — corrected current draft common/public reader and two supporting indexes; F1 closed by fresh source re-read. Read-only source review, no product edits, SQL/DB/runtime/tests/fixtures or hosted calls. Root alone writes product.

## Scope

Read approved full WB-12 card (`02_WORKBENCH.md:499–539`), all eight current checklist rows, BD068 and existing WB-12 investigation. D06 is the principal Ready screen deliverable; the common/public reader successor and two indexes are the finite supporting scope needed for exact server counts and visible Status sorting. No publisher, producer, financial command, Rule-B protocol or new table is needed.

## F1 — P1: malformed public-body source assembly

At the inspected raw SHA below, `10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql:561` truncates the UUID validation expression after `{12}` and embeds the original public-function tail into the regex/source. That inserted tail reaches `$function$;)` at line647; the original ordinary tail then appears again from651. The intended validation block is not valid PL/pgSQL and this draft cannot be treated as installable.

Minimal correction: regenerate the optional count_selection validation hunk from clean current1634; retain the exact lowercase UUID anchored regex and separate whole-array uniqueness predicate, then one original public tail with the Status key-count/type additions. Re-read fresh corrected bytes before source PASS or locking. No business/body expansion is required.

Inspected draft SHA-256:

- `supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql`: `83cdc4d1a58a8b0f9c835097fdae09363256fa5749b5998621670d8883dd41aa`.
- `supabase/migrations/10102026_1743_banking_pay_next_ready_candidate_read_indexes_v1.sql`: `3b471ee4cbe26087f2c8a02040aa190a536288627d213de1e874b42f3ae834fc`.
- `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql`: `391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d`.

## Corrected current draft — F1 CLOSED; SOURCE PASS

Re-read the regenerated saved1743 source and full diff against current1634. It now contains exactly two CREATE OR REPLACE definitions, each one complete dollar-quoted body, one public tail and one owner/ACL/notify/commit tail. The UUID guard is fully anchored and explicitly parenthesised. The malformed raw snapshot above is historical provenance only.

| Requirement | Fresh source evidence / conclusion |
|---|---|
| Exact selectable Candidate count | Common reader lines35–45 counts only native READY Candidate summaries with payable_count>0, exact channel/client natural scope and optional Candidate. It does not test amount>0, sum a visible page, count inert retained summaries or include deductions-only Candidates in Create counts. |
| Deductions-only visible | CANDIDATES lines53–54 retains payable_count>0 OR deduction_item_count>0. Actual native1332 contribution lines472/478 derives these respectively from selectable PAYMENT rows and unique selectable DEDUCTION members. The row remains factual/readable; its disabled tick is a caller requirement, not a fabricated DB exclusion. |
| Native accepted selection preserved | Both original HEADER projections retain selected_count, selected_amount and unknown_selected_amount_count unchanged. Original Candidate fields also remain unchanged. Local parent intent uses separately named fields, not the accepted counters. |
| Exact parent intent counts | ALL XOR presence in the typed UUID exception array counts current eligible Candidates; NONE reverses that interpretation into explicit inclusion. selected_member_count sums each selected eligible Candidate's native payable_count+deduction_item_count; selectable_member_count is the same whole eligible denominator. This is current parent-intent membership, not an estimate of payroll/net/cash or a claim about later child overrides. Unknown/out-of-current-scope IDs do not manufacture contributions. |
| HEADER READY only | Public lines551–565 admits non-null count_selection only for HEADER with section READY, exactly mode/candidate_ids, ALL or NONE, array of anchored lowercase UUID strings, no duplicates. Missing optional count_selection is normalised to JSON null; existing callers can omit it. Whole request still has the32KiB octet cap. Oversize requests refuse rather than truncate. |
| Typed nulls | selectable_member_count is a nonnegative integer text for READY and null for other sections; selected_candidate_count/selected_member_count are integer text only for an explicit count request, otherwise null. Empty eligible sets produce genuine zero counts. Current Worker HEADER schema/type checks mirror this finite shape; caller review is separate. |
| Status five-field identity | Common lines93–96/429 and public585/597 use (payable_count=0, bank_warning_code IS NULL, coalesced warning COLLATE C, Candidate name COLLATE C, Candidate UUID). Seek, emitted cursor and explicit scalar ORDER BY use the same tuple, all ascending or all descending. UUID is the unique final tie-break. Public admits Status only for CANDIDATES, so retained pickers do not acquire another sort contract. |
| Exact supporting indexes | Migration line4 matches the Status equality prefix section/pay_channel/client_scope and all five ordering expressions including both C collations. Line5 is a covering partial eligible index with the same prefix/Candidate and payable_count>0 predicate, including both member counters. Columns and nullability were verified in actual1252 summary schema; no table, producer or money owner is added. |
| Scope/channel/amount semantics | Original natural ALL/channel and ALL/exact-client lookup is unchanged. Both scope reads its single ALL summary amount, not duplicated channel rows. Original stored SOURCE, NET addition, deduction balance and unknown-money distinctions remain. No pay-date valuation or browser financial calculation is introduced. |
| Existing reads preserved | Full source diff changes only the named count and Status hunks plus optional input/cursor validation. All eight issue provenance fields, existing group/line/issue/export projections, ten public dispatch kinds, stale membership check, limit+1 seed, byte bounds and exception envelope remain current1634 behavior. |
| Authority/time bounds | Common stays STABLE SECURITY INVOKER, private owner-only ACL; public stays STABLE SECURITY DEFINER with original service-role and active-admin predicates, NEXT/current adoption checks,5s settings,120000-byte envelope/110000-byte row accumulation. Public EXECUTE only service_role; original wrappers remain defined by1634 and keep their own5s/ACL. Notify schema reload remains at the tail. This source review does not prove hosted hoisted statement_timeout or a schema-cache/runtime installation. |

The successor deliberately defines only common/public; the ten unchanged1634 private wrappers continue to dispatch into the common signature. The final-definer registry observed during this pre-lock draft review still names1634 for those two physical definitions: root must advance exactly common/public when binding the successor, retaining wrapper finalPhysical1634. This is a draft release bookkeeping handoff, not a reason to duplicate unchanged bodies or broaden scope.

No concrete remaining source defect was found after F1 correction. Scope is the required finite support for WB12 D06; it does not close the screen implementation, app counts/walks, later estimate/Create/child intent owners or whole ITEM. Compilation, migration locking, source gates and runtime/app acceptance are NOT_RUN by this reviewer. No product code, tests, SQL or data was executed.

## Final raw hashes

- `supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql`: `727d80356ecfd7ef7e8c822c3181403602910ab4cef5bb50f4677dd32df2b760`.
- `supabase/migrations/10102026_1743_banking_pay_next_ready_candidate_read_indexes_v1.sql`: `3b471ee4cbe26087f2c8a02040aa190a536288627d213de1e874b42f3ae834fc`.
- `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql`: `391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d`.
- `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql`: `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391`.
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql`: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`.
