# WB-09 SQL source review

10 October 2026, independent source-only review. Read full card/all checklist rows, R2 WB09/10/12–16, W13, the field-map investigation and reader contract; read all1520 SQL. No SQL, tests, application/data, runtime, timing or product edits. Initial raw SQL SHA256 `b1a0aaf8f7deced2903eeef153dc68c9d5a80a000098130489cf691ac0e3f6db`.

Initial verdict: **changes required** for the current read contract. No whole WB09 or runtime acceptance claim.

## RD01 — complete group interface lacks required facts and ordering

1520:89–121 returns group identity/count/progress only. It omits group_amount, has_deduction, client display name, selectable and section/reason facts required by the actual WB15 consumer contract (02:626,635–636). It orders groups by group_key, rather than the declared week/group-kind/native-ID tuple, and accepts READY groups without checking `g.complete`, `g.dirty` or `g.progress_state` before offering the parent. The global pending counter is useful but is not a replacement for the exact complete/current parent qualifier.

Use a bounded seed of <=10 exact parent groups with explicit finite/null-safe `(week_ending_date,group_kind,native owner UUID)` order, then named scalar current-member aggregates for those parents or a narrowly maintained typed group subtotal. Do not substitute factual approved whole gross or sum a child page. Qualify READY by genuine selectable READY membership and complete/current parent. Carry source gross, NET additions and deduction balance independently, plus the required controls and actual retained Client display. Case identity and null-week ordering stay explicit. No history or headroom calculation is needed.

## RD02 — issue NAME sort is actually random UUID order; declared issue sorts and Timesheet search missing

1520:459 admits only NAME_ASC for ACTION_REQUIRED/BLOCKED/ISSUE_DETAIL, but1520:352–353 uses `i.id` as the entire order/seek tuple. This is uniquely pageable but does not implement Candidate A-Z promised by NAME_ASC and WB13 (02:550,569). Payment/Amount sorts required by that same actual service consumer cannot be requested.1520:131 searches only Candidate display/reference; it omits `timesheet_reference`, explicitly required by02:568.

Use fixed declared Candidate/Payment/Amount query shapes, with explicit null treatment and a unique final item UUID/typed owner tie. Preserve exact section/native owner/scope and include literal escaped Timesheet-reference prefix search. Do not label an ID sort NAME_ASC. WB10 owns the named follow-on sort/prefix indexes and timing evidence; the service must expose the correct order rather than defer its semantics.

## RD03 — Candidate name order omits the declared TMS-ref tie

1520:76–77 uses `(candidate_sort_name,candidate_id)` while the actual WB12 default key is `(candidate_sort_name,tms_ref,candidate_id)` (02:516). Two same-name Candidates with different references therefore have a different order than the declared interface. Add explicit non-null discriminator/ref ordering to the finite query/seek/cursor key and update its exact key validator. The final Candidate UUID remains the unique tie. Both still uses the channel-ALL natural summary correctly.

## RD04 — ISSUE_DETAIL omits the exact affected-Candidate count

The ISSUE_DETAIL projection is the issue row shape only (1520:206–243). The response contains rows and page receipt but no affected_candidate_count required by02:603. It cannot be reconstructed from a visible page. Current group-specific work/Case selector has exactly one native Candidate when present, so the smallest truthful count can be obtained from that exact group/scope/reason membership; missing scope yields zero, not a broader all-reason enumeration. Any future multi-Candidate Umbrella grouping needs an actual retained grouping authority. Do not invent an item umbrella_id or new issue UUID.

## Positive source findings and reviewed limits

- Real columns checked: public.clients.name exists (baseline1500:2033–2036); private.cloudtms_database_identity.installation_id is uuid and singleton boolean (1507:6–14). Cursor agency uses this real installation identity, not a manufactured Source agency UUID.
- Public wrapper and private readers are STABLE and perform no row locks/volatile authority/clock/write helpers. Service JWT and actual active-admin predicates precede data dispatch. Snapshot revision/adoption/owner/date/scope and rows remain coherent.
- Global adoption/current/dirty/pending checks fail typed for every kind. Unknown Candidate/header money uses unknown counters, selected unknown is independently qualified, and stored zero is not displayed as a complete unknown total. CANDIDATES includes deduction-only Ready membership.
- Caller values are `$n` USING parameters; assembled SQL fragments/projections/sort choices are fixed code. Membership EXISTS runs the same scope/filter shape and seek tuple before continuation. PICK_CLIENTS seeds actual maintained real Client keys and uses ID order, avoiding live-name ordering drift.
- All financial outputs are named decimal text; explicit composite row fields only. GROUP_LINES splits its named JSON constructors below the PostgreSQL argument cap. Native portion/member identities and accepted provenance remain distinct. No rates/shift/break/cash/headroom or parked F25 rewrite.
- Limit+1 lookahead supplies true completion, <=100 returned rows /<=10 outer groups, whole-page byte refusal and final120000-byte envelope cap. No byte-break partial page or all-run JSON. Empty final page retains page receipt context.
- Private executor and ten wrappers are owner-only/revoked from ordinary/service roles; public wrapper service-only with pinned search path/owner and exact NOTIFY. Existing tables retain FORCE RLS.
- Most declared future sort/prefix indexes are not present on the existing projection. Source review does not prove an indexed execution plan or legal-limit page walks; WB10 and joined runtime acceptance remain required. The original CURRENT_WORK factual reader remains unchanged and is not a payable summary.

The requested SQL/documentation and the referenced source/schema evidence were read; only this review note was written. Root alone corrects product. Worker/FE integration is not covered by this initial SQL verdict.

## Fresh correction review — RD01–04 closed; SQL subset PASS

Re-read current full1520 source after the root corrections. Final reviewed raw SHA256 `eebe1a463cac604165a8b9e460ba2a5e82c95d45edd118806d96ae3d03736409`; reader-contract SHA256 `d6749ede9676fcf32c2eedad5cb35a45830ed6db8f7d6eea1e293eeb74d2f424`.

- RD01 closed. READY exact groups now require complete/not-dirty/CURRENT and actual selectable child. Typed date-null/date/kind/native UUID keyset is used. The <=10 emitted parent seed plus one lookahead is established before LATERAL enrichment (1520:406–419); the aggregate reads only those exact indexed current-member groups. Named current source gross, NET-add and gross/net debt dimensions remain distinct; unknown money is NULL. Client display derives actual current retained child display, warning derives cached Candidate metadata, and section/reason/selectability are explicit. No child-page subtotal, history or headroom fallback.
- RD02 closed. ACTION/BLOCKED expose the six actual NAME/PAYMENT/AMOUNT ASC/DESC shapes. NAME reads current Candidate display and Candidate UUID, Payment actual Timesheet reference, and Amount genuine nominal/unknown-discriminator values; every tuple ends in native item UUID and has matching seek/key construction. All three literal-prefix fields, including Timesheet reference, are present. Source scope/section and native ownership remain fixed. Unique ties satisfy R2's Case/non-null item correction rather than the old nullable work-only key.
- RD03 closed. Candidate name uses name/ref-null/ref/Candidate UUID; deduction also preserves that ref tie. Candidate amount separates unknown from known values in the internal sort key while wire unknown totals remain NULL. Context and exact key length/type validation are updated consistently.
- RD04 closed. ISSUE_DETAIL exact-group membership EXISTS runs before cursor seek and returns page-independent affected_candidate_count `0`/`1`; public reply carries this named value. The current group FK confines membership to one native Candidate; no invented multi-Candidate grouping.

RD05 review concern was comma splitting when new COALESCE expressions are used for key tuples. The latest saved source **already uses** explicit fixed scalar ORDER BY clauses (1520:380–392), replacing only the fixed `@` direction marker. My warning referred to the earlier derivation during concurrent draft review; it is not a defect in this final hash. Function-expression commas remain intact, seek tuples and scalar ordering use identical expressions/collation/direction, and no SQL parser or caller interpolation was introduced.

No remaining concrete source defect found in this scoped SQL re-read. Public/private STABLE snapshot behavior, current barrier, native agency/actor/epoch/date/scope/revision cursor binding and membership checks, explicit decimal rows, limit+1 completion, full-response refusal and ACL/NOTIFY retain the positive findings above. Source PASS is not execution/planner/page-walk/auth/adoption/latency acceptance. WB10's exact follow-on indexes/performance and Worker/Office joined contracts remain their actual required boundaries; parked F25/Rule B and monetary owners are untouched.
