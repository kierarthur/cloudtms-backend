# WB-14 issue SQL source review

**SOURCE PASS — no open defect found in the reviewed 1806 SQL/index/registry delta.** Read-only static review; no DB, SQL, runtime, app or tests executed. This does not certify installation, timing or the specifically BLOCKED three-Candidate Umbrella journey.

Reviewed the complete 1743→1806 text diff, current changed branches with their existing full request/security/tail context, native column constraints, both migration statements and the complete two-chain registry diff. The new repeatable defines exactly the original common helper and public reader, with one original ACL/notify tail. No producer, money, classifier, group identity, parked F25 protocol or other read-kind behavior is added.

| Check | Source conclusion |
|---|---|
| Reason order/seek/key | `reason_priority` smallint, `reason_code COLLATE "C"`, native non-null item UUID. Literal scalar ORDER BY matches seek tuple and JSON key in both directions; no composite ORDER BY or browser ordering. Native priority is non-null 1–200 and code is non-null. |
| Public validation | Only Action Required, Blocked and Issue Detail accept REASON_ASC/DESC. Cursor has three elements, numeric nonnegative integer priority bounded to smallint, string reason, lowercase UUID; exact original context and cursor membership checks remain. Invalid malformed/cast inputs retain the existing typed request refusal mapping. |
| Whole affected count | `count(distinct i.candidate_id)` over the exact original group/reason/optional item/Candidate/client/channel/section scope occurs **before** the seek predicate is added. It returns a bigint decimal string on each page and remains null for other kinds. It cannot count only the current page or manufacture shared scope. |
| Indexes | Exactly two additive private indexes. `(section,reason_priority,reason_code COLLATE "C",id)` mirrors the Action/Blocked reason sort. `(group_key,section,reason_code,reason_priority,id)` supports exact grouped reason pages with fixed section/reason. Existing group/all-section indexes and native constraints remain. No tables/trigger/money authority changed. |
| Preserved contracts | Full 1743 predecessor comparison limits changes to REASON keyset/ORDER/enum/cursor validation and affected count type/query/output. WB12 eligible/local-intent HEADER counts, Status sort, eight issue amount fields, all ten kinds, 100-row limit+1, 32KiB request, 110000-byte rows, 120000-byte envelope, 5s statement limit, exact context and stale-cursor checks remain byte-identical outside these hunks. |
| Security/registry | Public remains STABLE SECURITY DEFINER with original service-role/active-admin/NEXT checks, owner postgres, execute only service_role. Common remains STABLE SECURITY INVOKER, owner-only EXECUTE. `notify pgrst,'reload schema'` remains. Registry updates only the common/public chains to 1806, appends 1743 predecessors/1806 allowed physical entries and preserves wrappers on 1634. |

Caller boundary: `item_id` retains its original exact-row filter. The generic whole-group detail should request `item_id=null` after validating the actual opening anchor in its trusted current caller, rather than describe a single-item filtered reply as the whole group. The SQL count is correct for either explicit request scope. Current native WORK/CASE group belongs one Candidate; the genuine result is zero/one. There is no shared issue-change evidence that authorises reason/target-Umbrella fanout. The three-Candidate Done journey remains BLOCKED as already documented, while the exact grouped detail/source work proceeds.

The first two repeatable header comments still describe WB12; this is harmless retained predecessor prose and does not change the reviewed executable scope. No product change was made by this reviewer.

## Current source SHA256

- `supabase/repeatable/10102026_1743_banking_pay_next_ready_candidate_counts_v1.sql` — `727d80356ecfd7ef7e8c822c3181403602910ab4cef5bb50f4677dd32df2b760`
- `supabase/repeatable/10102026_1806_banking_pay_next_issue_reason_reader_v1.sql` — `bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329`
- `supabase/migrations/10102026_1806_banking_pay_next_issue_reason_read_indexes_v1.sql` — `99e877d811dd5b530c980930fc9954f807a241ea831f65d28a1bdb1c60c16b65`
- `docs/bpay-next/final-definer-closures.json` — `a66971381cbb3dcf46d34a032281fd85d3eefba0e354230e48435a8ee13322db`

## Source-gate closure supplement

**SOURCE PASS — the one new closed Workbench EXECUTE shape is sound; no open finding.** Fresh diff of `scripts/bpay-next-source-gates.mjs` adds only an anchored literal `execute 'select count(distinct i.candidate_id)'||v_sql||v_where into v_affected using` plus the exact original eleven bind operands. It permits neither another aggregate/alias/relation/INTO target nor a changed bind list, trailing SQL, caller SQL interpolation or another execution shape.

The exception is consumed only when the existing `closedWorkbenchTemplates` predicate is true for the exact common-helper signature, declared fixed SQL variables/allowed fragment producers and checked literal fragments, with exactly one EXECUTE token in the inspected statement. The existing protections against concatenating p_scope/p_after/p_limit/search/client/Candidate/group/item/reason into SQL remain unchanged. Original v_sql page loop and exists/member shapes remain unchanged. The gate extension is a finite recognition of the already reviewed count statement, not a whole-file waiver or a new test/tool framework.

Fresh SQL, migration and registry bytes still equal the hashes above. Current gate SHA256: `e34a1c6c9a33cc5aa84b1fdf94c900be911a3ab4937b5e596f75277819937e3c`.

The builder separately reports local SQL compilation/seal/coupling and source9/9 PASS. Those are builder receipts; this reviewer performed no execution and does not convert those reports into app/timing/three-Candidate acceptance.
