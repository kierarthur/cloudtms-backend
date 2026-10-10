# WB-17 navigation SQL review — 10 October 2026

**SOURCE PASS for the current 2105 common-reader definition and exact closed Worker/FE identity mirrors. No concrete code defect found in this reviewed subset.** This is not whole WB17 completion or an app/runtime receipt. Root is still implementing the actual child navigation UI; that part is outside this review.

## Scope and comparison

Read full WB17 card/checklist, root design `WB-17-exact-work-navigation.md` (BD073), current 2105, current 2026 predecessor, native Work/revision schema1324, native Source schema1534, genuine Source/protected stage1236, and current transport mirrors. Function-level comparison of `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` isolates exactly four changed regions: the CANDIDATE_GROUPS identity suffix, ISSUE_DETAIL identity suffix, existing Candidate-group PK enrichment, and new bounded ISSUE_DETAIL PK enrichment. The separate functions physically contained in 2026 are not redefined by2105; their omission from this new file is intentional, not deletion.

## Confirmed source behavior

- Exact Work lookup is Candidate-scoped; revision is scoped by both `r.work_id=w.id` and the single native pointer `CASE WHEN approval_state='WITHDRAWN' THEN applied_revision_id ELSE current_revision_id END` (2105:508–518). It never selects booking-original, latest-by-order, or arbitrary previous usable revision. Existing maintained `current_revision_id` still stays NULL for withdrawn groups; the added physical_revision_id explicitly identifies the applied evidence. Replacing the older cached physical column in just these navigation projections with the explicit native revision is within the card's current-identity instruction; no maintained writer or money classification changes.
- Candidate-group seed remains the original sorted <=11 presentation identities for limit10, before factual joins. Work/revision/head joins are PK/unique identities and cannot fan out the page. Native presentation parent, Case ROW FK, scope, sort tuples, anchor, current page cursor and limit behavior are unchanged.
- ISSUE_DETAIL uses MATERIALIZED <=p_limit+1 indexed item identities and original row_key first, then its own item PK and exact native Work/revision/head joins. Its count(distinct Candidate) and full scoped predicate remain unchanged. Owning rows remain present when optional Work/Source identity is absent because enrichment uses LEFT JOINs. Outer order remains the native typed row order.
- Added physical version and root version are positive integer strings, matching the existing transport `positive?` convention. UUIDs and DATE remain their original JSON scalar representations. Source kind uses exactly the five existing revision values. New fields are finite, named facts; there is no arbitrary row/composite expansion.
- A head-backed SOURCE/PROTECTED row joins the exact retained head by UUID and compares Candidate, Contract and revision week. Root/version are that exact head's retained fields. No current-state/current-head/latest-head predicate substitutes evidence. Head and immutable revision original identity were already validated by the native source owner; this review does not require redundant producer/event joins.
- An actual sealed SOURCE/PROTECTED revision with head NULL uses its immutable physical ID/version as Source root. This is genuine initial Source/protected authority already established by1236, not unsupported fallback. Null head alone is not enough: the branch requires the genuine Source kind and sealed revision. Ordinary/Expense/Adjustment rows have null Source root/week; no original-Timesheet or current-head COALESCE occurs.
- Case and standalone Case-component rows have no Work revision, so all added physical/source fields are NULL. Original Case identity, amount, tax, rule, continuation and display fields remain unchanged. Unknown/missing optional Source head can leave root/version unavailable without dropping an owning issue or making up a physical root.
- WB16 GROUP_LINES body, approved segment/break/parent evidence, component amounts and all other eight read kinds are byte-for-byte unchanged at function-body comparison. Five-second statement bound, p_limit+1 behavior, unchanged110000-byte envelope, original validation and continuation semantics remain intact.
- 2105 retains the exact function signature, STABLE/security-invoker/search_path/statement_timeout configuration, logical owner and private-only ACL. It introduces no public RPC, route, table/index, trigger, producer, money effect or command.

## Closed transport mirrors and preserved frozen scope

`broker/src/bpay-next-workbench.js` and `js/bpay-next-workbench.js` ROWS definitions are exactly equal after the existing export keyword difference; their new relational checks are exactly equal too. Only CANDIDATE_GROUPS and ISSUE_DETAIL receive the seven added fields. Both enforce finite Source-kind enum, physical ID/version/revision all-null-or-all-present, Case no-Work nullability, root ID/version pair, non-Source null Source fields, and actual Source week presence. Existing actor/context validation, limits, ordering, byte bounds, scope and money fields remain intact.

The D02 frozen service0250 and D06/D07 fixed transport/handler are unchanged and retain exact run-line/run-work authority and four frozen kinds. They do not use these current-reader fields as captured frozen evidence. No public preparation successor or captured comparison-body rewrite has been added by this batch. Root BD073 explicitly preserves the one actual Open Draft/frozen destination and records later WB23/WB24/WB25 and DR02/DR06 joins rather than building another frozen viewer. This source PASS does not certify those future screen joins.

## Closing metadata — verified current closure

The earlier source read raced root's registry save. A fresh single-read registry inspection now confirms the private common-reader row has finalRoot/finalPhysical/finalClosures2105, includes2105 in allowedPhysical, retains2026 in predecessors, and labels the owner WB17. That closing requirement is **closed; no open metadata finding remains**. The public preparation dispatcher and current Case component reader correctly remain2026; the frozen detail service correctly remains0250. No unrelated signature was moved to2105.

Current raw registry SHA256 is `5de9f5546a7d9657254cef2a7d2ebd9407a116d37e5b3082af421df3f58559be`; current2105 remains the reviewed `dad87c105db7daa58b4b62433812d13bcab89aa86481a158e012791777d2c12e`. The parent separately reports local compilation0migrations/4repeatableclosures, db328/855 and source9PASS, with seal/coupling finishing. Those are parent-owned execution receipts, not tests executed by this reviewer; neither metadata verification nor those reports imply app acceptance.

No SQL, DB/runtime/business call, tests, app or browser was executed. Root's compilation/source/check receipts are separate; this review makes no hosted or app claim.

## Reviewed raw SHA-256

| Source | Raw SHA-256 |
|---|---|
| BUILD-BE/supabase/repeatable/10102026_2105_banking_pay_next_exact_work_navigation_v1.sql | `dad87c105db7daa58b4b62433812d13bcab89aa86481a158e012791777d2c12e` |
| BUILD-BE/supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql | `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a` |
| BUILD-BE/broker/src/bpay-next-workbench.js | `bc063d8205be961e18703dfa6d85dbc431a176f4bde7b37bcb484b4d49992232` |
| BUILD-FE/js/bpay-next-workbench.js | `a7ca68cd8ccd4669d34af32349bab64ca46c3ea57ffd2b262b98474f3c9d2835` |
| BUILD-BE/docs/bpay-next/final-definer-closures.json | `5de9f5546a7d9657254cef2a7d2ebd9407a116d37e5b3082af421df3f58559be` |
| BUILD-BE/broker/src/bpay-next-frozen-rpc.js | `6ddbc1ecb64873714e9a9c9a05bc4fcb2dae5d7fca95236a4f75976594a4e83c` |
| BUILD-BE/broker/src/bpay-next-frozen-read.js | `3c7d75b485db6824a38000d580eb96c37864c01a61fdaf114d5551637e10c3b2` |
| BUILD-BE/supabase/repeatable/04102026_0250_banking_pay_next_frozen_detail_service_v1.sql | `ca6e50fcb6324e9e3a39e840a664ddfa53a7a7d96d9d0fb9859d65743614e091` |
