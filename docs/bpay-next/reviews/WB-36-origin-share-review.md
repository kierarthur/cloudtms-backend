# WB-36 origin-share subset review

**PASS — the positive origin-share subset, now including its finite INSERT/immutability guard, 1620 hold successor, and 1519 capture-counter successor.** No concrete defect found in the scoped definitions reviewed below. Negative reservation/recovery/forgiveness consumption and the whole WB-36 closure remain pending; their absence is not waived. The first review below records the earlier three-helper checkpoint; the fresh supplement records the completed positive guards/call seam.

2026-10-10. Read-only full source read of the three definitions and their private ACL/attachment tail, against current1100 schema, current2018 hold/outcome owners, immutable1342 guards, and final1157 shared valuer. No SQL/runtime/tests/business/data/network execution or product writes.

## Reviewed current source

- `supabase/repeatable/10102026_1100_banking_pay_next_segment_authority_v1.sql`: gross balance2650–2662; recordshares2664–2720; payroll attachment2724–2763.
- `supabase/migrations/10102026_1100_banking_pay_next_segment_exclusion_v1.sql`: parent attribution90–113; origin_share114–129; effect_share130–137; exact effect_origin138–145; private RLS/revokes/owner policies164–195.
- Native owners remain `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`: hold1358–1396; common BANK/INTERNAL_ZERO post_simple_outcome2411–2491. Final private valuer remains `06102026_1157_banking_pay_shared_selected_quote_v1.sql:932–991`.

## Finite checks and result

| Check | Finding |
|---|---|
| Actual fields/types | `segment_member_count`, `segment_attribution_complete`, `segment_included_positive_ex_vat`, `included_positive_source_ex_vat`, `target_umbrella_vat_chargeable`, line valuation_policy_id/run pay_date, and all hold/share columns match saved migrations/current owner fields. SUM returns numeric; COUNT's unnamed record field is `count`, as accessed by v_total.count. Composite `IS DISTINCT FROM` checks remain NULL-safe. |
| Exact captured operand | recordshares joins the exact run_position by `(run_work_id,component_key)` from the line, requires the hold tuple/ACTIVE status to match the line, checks attribution complete and included source total equals parent reservation. It only emits positive included children of that actual captured parent. Removed/snoozed positive children cannot enter this query merely through shared date/ordinal. |
| Cumulative penny conservation | Ordered cumulative source is passed to the unchanged shared valuer. Its actual result columns are policy_window_id/target_ex_vat/target_vat/target_inc_vat. Differences of successive cumulative target ex/VAT telescope to the parent full-source quote; child inc=child ex+VAT. Final aggregate checks count/source/ex/VAT/inc against the exact hold. Empty/short/mismatched child output refuses through tuple inequality instead of fabricating zero. |
| Valuation authority |1157 validates penny source, actual channels, policy/pay_date and unique applicable policy window, then uses existing pay_finance_selected_quote_v1 in SUGGESTED_EQUIVALENT_BASIS mode. No new manual rate, calculator, NET projection, cash ratio or bank amount enters these helpers. Original policy/window immutability is retained (1342:201–219); prefixes use the same captured policy and pay date as the parent. Prefix calls allocate already-valued gross attribution; they do not create multiple financial holds/effects or consume source twice. |
| Derived gross reader |Realised source is the signed sum of retained effect_share source dispositions at exact `(work_id,component_key,segment_key)`. Held source joins actual hold status ACTIVE. RELEASED and REALISED holds stop contributing to held; realised effect history survives bank return/reissue. No Candidate/agency history scan or mutable standalone segment balance is introduced. The new exact-work/component/key indexes match this reader. Correctness depends on required immutable same-scope insertion guards admitting only genuine effect/share membership. |
| Actual settlement seam |The AFTER INSERT attachment ignores every effect_kind except PAYROLL_SETTLED and uses operation_item_id as the real hold ID, exactly matching2018:2461. It copies the exact immutable hold-share gross into effect_share and links each source origin in effect_origin, then reconciles all four parent effect gross totals. Native BANK and INTERNAL_ZERO both use this same post_simple_outcome owner.0490 does not insert a second WORK effect. |
| Lifecycle order |At the attachment invocation the native hold is still ACTIVE:2018 inserts the effect2456–2468, updates the parent2472–2487, then sets hold REALISED2488–2489. Therefore a pending same-scope INSERT guard must validate the actual active insertion owner, not require REALISED immediately. A deferred end-of-transaction check may require the genuine terminal realisation. Do not move the native status update earlier or weaken it to fit the new attachment. |
| Feature boundary |Exactly one new private feature attachment on private.bpay_next_financial_effect. It does not replace frozen1342 callback/binding, attach to public Source tables, rewrite the later full settlement owner or act on return/reissue/negative recovery kinds. Nonsegmented native hold/line shapes remain unmodified by its early return. |
| ACL |All three functions are private SECURITY INVOKER; explicit logical postgres owner and revokes from PUBLIC/anon/authenticated/service_role; no public RPC or browser table grant. New tables have owner mapping/RLS/private revokes. Existing definer owners supply controlled invoker authority. |
| Replay |Native outcome paging validates job/lease/cursor and returns genuine DONE/cursor replay before a second effect insertion. New effect/key uniqueness and origin composite PK reject duplicate contradictory membership. The two-arg recordshares itself is an append helper, not an independently idempotent public command; the actual hold caller must call it only in the original fresh hold transaction and preserve existing cursor/exact retry semantics. |

## Explicit pending closure obligations

These are the builder's already acknowledged remaining WB36 work, not newly invented scope or a whole-owner PASS:

1. Immutable INSERT qualification for origin_share/effect_share/effect_origin must validate exact run_line/hold/run_work_segment/parent/work/component/key scope. The schema's individual FKs alone do not establish every cross-column identity. effect_share must have a real native payroll operation/member/transfer/owner; totals alone are not financial admission.
2. The real hold transaction must obtain its inserted hold ID and call the exact two-argument helper before success/cursor publication. Exact same-parent child source sum and captured member completeness must be proved by capture guards. Otherwise a metadata-only segment_member_count=0 branch must not become a bypass for genuinely segmented work.
3. Negative effect membership/immutable source consumption must be implemented with current0316, and actual1647 forgiveness retained separately. The reader summing signed effects is ready for that path; its existence does not prove the negative writer is closed.
4. Whole-day, moved-day and Source-parent conservation still require the exact capture/hold/recovery implementation described in `design/WB-36-money-integration.md`. These helpers do not fix missing old parents or mixed parent negative credit by themselves.

## Snapshot hashes

Raw SHA256 source snapshot (not installed definitions):

- `supabase/repeatable/10102026_1100_banking_pay_next_segment_authority_v1.sql` — `e6555da7c7695b521a0e01c8d43e06727d088561838625c16fe001e61132cba3`
- `supabase/migrations/10102026_1100_banking_pay_next_segment_exclusion_v1.sql` — `7cb60e23c875f83632ea33e549664b85f3821ad6f8de5672dcc18abb9381160f`
- `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` — `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`
- `supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql` — `ad1c7a92754f2937edb642dd5ee387f62761ef241cef77d3b792185b2eb22f3c`
- `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql` — `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af`

Exact definition-fragment hashes (UTF-8 current CREATE through matching $function$;), allowing later unrelated draft additions to remain distinguishable:

- `private.bpay_next_work_segment_gross_balance_v1` — `96e70b650ed344ef419112ffcdebcf167baedca5ffc035ee79066fe1ad5d6045`
- `private.bpay_next_record_work_segment_origin_shares_v1` — `4370bad76a5f4ced02e995f71df777778362ba589fd4cce10533e59e9e91f7de`
- `private.bpay_next_segment_payroll_effect_membership_v1` — `af20c45a23e62dcafc23b440d5c11e10fa777e6341f4d19f193d1898a5d3b043`


## Fresh positive guard / hold / capture supplement — 10 October 2026

**PASS for this requested source subset.** Full reads of the new segment_fact_guard, segment_prepare_owner, hold successor, capture guard successor, their actual schema fields/FKs and trigger/ACL tails. Static comparisons against current2018 hold and current1519 capture guard establish the preserved original branches. No SQL, tests, runtime or business invocation was performed. The following is not a negative-branch or full WB36 approval.

| Area | Fresh source finding |
|---|---|
| 1620 native owner preservation | Original module SHARE → run SHARE → Candidate control → job → worker locks, actual lease/module/owner epochs, exact cursor replay, financial-resolution/target checks and current position/source-book validation remain unchanged. Static full-definition diff is limited to the captured positive operand/zero eligibility, independent managed completion, fresh hold ID/share call and corresponding included-source increments. No new publication clock or transport command. |
| Positive and zero allocation | Holds use `segment_included_positive_ex_vat` after complete exact segment attribution, or the real unsegmented residual less captured snooze exclusion. Snoozed/unselected positive share is excluded from allocation. A zero line requires all native gross counters zero, no parent snooze, and an included captured segment when segmented; it creates no zero hold. The shared valuer still receives the actual captured policy/run pay date/channels and parent target setting. |
| Fresh caller / replay | The actual hold INSERT returns its UUID and immediately invokes `record_work_segment_origin_shares_v1(line,hold)` before updating native held counters and publishing cursor/worker totals. Existing cursor/FINANCE_ALLOCATE replay precedes this fresh arm. There is one native hold/line per parent; child prefix quotes attribute its gross rather than create shift economic units. |
| 1519 preservation | Original complete Case-period and run_case_instruction branches are byte-identical. The original managed-capture ALLOCATE transition and its actual origin/version/observed gross/lease/owner checks are unchanged. Added RESERVE arm permits only six subordinate attribution fields to vary; every other JSON field, including expected count, snooze identity, native source/target capture and managed transition, remains immutable. |
| Exact counter closure | Added counter values equal the actual immutable parent-child count and sums, each step advances at most one member, requires a current RESERVE PREPARE owner, and forbids further attribution after completion. Completion requires count=expected count and exact native approved/realised/held sums. Zero-member positions are marked complete on their original INSERT by the capture caller, so the later nonempty sum arm is not used to invent a zero history. |
| Selection / captured scope | Actual run/work/revision/component/key and selected disposition are checked against retained approved_segment and accepted snooze cut. Page guard validates its next page/member ordinal and string-only exact request array; child FK and page membership protect the real selected origin. Captured rows require current PREPARE/RESERVE, actual current segment or genuine retained old share for REMOVED. |
| Moved day / child authority | One `run_work_segment` exists per `(run_work_id,segment_key)`, with real component parents in `run_work_segment_parent`; composite FKs bind run/work/revision/key and native parent. Guard takes approved gross only from the exact current component; an old component with the same key gets actual retained gross credit, zero current approval and genuine prior-origin proof. Parent positive/recovery/held-overhang operands are independently recomputed from signed realised shares and ACTIVE holds. This guard does not redistribute credit across days. |
| Positive origin INSERT | Actual hold, line, run segment and parent are loaded by UUID; scope must match line work/component and segment key/run_work, original hold must be ACTIVE, parent attribution complete and segment INCLUDED. Amount equals the captured positive parent child; the hold's source and target tuple equals the actual frozen line. Schema exact hold/line/work/component and segment-parent FKs cover the linked fields. Aggregate target penny closure remains the unchanged reviewed recordshares helper's full-parent check. |
| Positive effect INSERT | Actual financial_effect must be PAYROLL_SETTLED, with work/component equal both share and actual hold; source/target amounts equal the original immutable hold share. Actual original transfer WORK member, outcome request, active CSV_SETTLEMENT/BANK SETTLED-or-RETURNED or INTERNAL_SETTLEMENT/INTERNAL_ZERO INTERNAL_PROCESSING job, native Candidate/module epochs, MEMBERS lease and command bind the real insertion owner. All referenced field names/types exist in1324/current owners. |
| Native order / effect origin | ACTIVE hold is correct at this INSERT seam: current2018 emits PAYROLL_SETTLED before parent counter movement and hold REALISED. Exact effect_origin links same work/component/key and original effect operation_item_id=hold UUID with equal source amount. A later return/reissue does not erase this original gross attribution. |
| Immutability / ACL | First operation check rejects UPDATE/DELETE/TRUNCATE before reading NEW. Exact seven feature relations receive row and TRUNCATE guards only; none of the eighteen frozen bindings is replaced. The helper/guard private owner and explicit PUBLIC/anon/authenticated/service_role revokes remain; schema uses private RLS/revokes/owner policies. Named composite `%rowtype`, numeric sums/count bigint and NULL-safe tuple comparisons match current declarations. |

The negative schema/reconcile/case_hold/negative-effect/write-off path is concurrently being authored and was not reviewed by this supplement. In particular mixed positive/negative-parent eligibility, recovery reservation attribution, exact original-paid consumption and forgiveness remain mandatory checks in the later negative review. No installed or runtime status is inferred from these source bytes.

### Fresh source identity

Raw repeatable SHA256: `5c7213777b64330e578a2041481329cdd454311745fbf14f0aac59863a0f74d9`; raw migration SHA256: `8b9ea680f83198eebc77d1d0851fdbe6230c0cc234f6c938021b929752e8d7c0`. These are draft-file snapshots and can change when the separate pending negative branch is appended. Exact reviewed definition fragments (UTF-8 CREATE through matching `$function$;`) remain the binding for this bounded verdict:

| Signature name | Current lines | Definition SHA256 |
|---|---:|---|
| `private.bpay_next_hold_fresh_position_page_v1` | 3337–3626 | `a825fad4257fe8c802c840de7dba54764a5c4337e6832e74133e3fe90399d65e` |
| `private.bpay_next_work_collection_capture_guard_v1` | 3632–3747 | `04f6284958431d5cb327fa7ce29133249ec0d7b98133d8c860327064b4c3bc2d` |
| `private.bpay_next_segment_prepare_owner_v1` | 3752–3763 | `22b976acb883f9ae4f8731266b084a2e63e9d6bc879c21b8aa2cb7bfbcdbea4a` |
| `private.bpay_next_segment_fact_guard_v1` | 3768–3910 | `e63b8ebbc8be863d4d3c556dca0cae8dfecafd588eaff9a32fe6759542373032` |
