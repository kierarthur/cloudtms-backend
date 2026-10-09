# F-25 accepted ordinary case preparation source review

9 October 2026. Independent source-only review of the complete 2308 successor against the saved 1958 owner, accepted CASE/WEEK schema and producers, and the existing monetary owners. No SQL, runtime, tests, fixtures, hosted reads or product edits were executed.

**PASS for this finite implemented 2308 consumer subset. No concrete open defect was found in the reviewed successor. Full F-25 remains incomplete; this is not an installation, business execution or application verification claim.**

| Source | Raw SHA-256 |
| --- | --- |
| `supabase/repeatable/09102026_2308_banking_pay_next_accepted_case_prepare_v1.sql` | `a6e84241e6622386b2645e9373cd714d64fc661ba244bcde5fdb248f094f82f2` |
| Original `09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql` | `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b` |
| Final capture `09102026_2303_banking_pay_next_accepted_case_terms_capture_v1.sql` | `db987eb3eba2ba945e48bbc7ac76b5e68f2ac6ffa8122d8360c67f80a1327f7b` |
| NO_CASE consumers `09102026_2306_banking_pay_next_accepted_no_case_consumers_v1.sql` | `f5d9bfb70e86ebf6cbd4ffae7fad377af27c972b0a54783bcd5d91d91e8813b7` |

The review read all 665 lines of 2308 and compared its changed input seams and unchanged monetary tail with 1958. The allocation/hold/counter/result/replay tail at 2308 lines 509–665 is line-for-line equal to 1958 lines 497–653. The existing funded-period exception at 2308 lines 373–407 is unchanged. These are source comparisons, not test execution.

## Retained input and replay closure

2308 lines 47–80 retain the existing module/run/Candidate/job/worker/state lock order, owner epoch, lease and phase authority. Existing terminal READY and REVIEW replay returns precede retained-input reads. Lines 94–106 retain exact checkpoint replay and ahead-cursor refusal without a second hold, cursor spend or financial-view bump.

Lines 82–87 resolve the exact append-only accepted parent by original job command, run, Candidate and module epoch and require pay-date/channel agreement. The parent schema binds that command to the accepted sequence and epoch; the reviewed acceptance/child guards require the same original PREPARE transaction. A positively retained NO_CASE parent dispatches to the reviewed 2306 owner at lines 108–110, so a later newly created case cannot redirect that original request.

Initial case preparation uses the retained sealed selection revision. Lines 146–149 require both the complete accepted CASE child count and selected child count to equal the real immutable selection header. Instruction paging, cursor and completion counts at lines 257–259 and 465–479 remain scoped to that exact parent/run/Candidate/revision. The allocation-stage NOT_AFFECTED proof also requires exact frozen instructions and selected accepted identities; missing identities do not become an empty exemption.

The initial and resumed Source comparison checks at lines 117–144 use actual selected accepted case identities and immutable collection/work lineage before stage writes. Instruction capture repeats the exact comparison refusal at lines 271–275. It keeps `BPAY_NEXT_COMPARISON_NOT_INSTALLED`; a collection whose current qualification reader returns no row cannot silently become ordinary input.

## Retained economic choices and genuine origins

Lines 155–160 consume retained floor state/value and pay week. Invalid or missing case-dependent floor remains the existing REVIEW path. A nonempty, complete selected OVERPAYMENT/RECOVERY/GROSS_DEDUCT identity proof retains the existing NOT_AFFECTED branch. Other scopes use only append-only accepted WEEK children with same Candidate/week, both origin sequences preceding acceptance, and canonical UTF-8 proof SHA. Bounded keyset paging at lines 220–253 accumulates their retained eligible amounts into the existing allocation state and never rereads mutable payment status, return eligibility or the current NET projection.

Lines 262–268 distinguish current monetary case/component rows from the exact immutable retained rule and original CASE_CREATE request. Retained rule schedule, threshold, floor override, funding identity and age remain authoritative; later current-rule pointers do not replace them. Retained unfunded eligibility keeps loan/advance recovery nominal zero. Existing immutable rule guards protect the referenced fields. The acceptance producer refuses unresolved earlier economic commands at 2303 lines 98–106, and the existing Candidate job barrier prevents a later economic job overtaking this unfinished PREPARE; the unchanged funded-period exception therefore is not repurposed to refresh accepted choices.

An ordinary automatic collection resolves the retained raw collection ID, exact case/component/Candidate and original policy (lines 277–297). It does not rerun the current qualification reader. Selected qualification and genuine origin were proved before acceptance. Ordinary manual cases use the original immutable CASE_CREATE receipt and original principal/component/policy tuple (lines 298–310). Their due date comes from that exact immutable request; automatic collection due date remains NULL (line 283). Case OPEN/due eligibility uses retained case status and the accepted pay date at lines 355–356.

## Existing money authority remains live

Lines 316–319 and 345–349 preserve current outstanding principal, realised recovery, write-off and paid-credit operands. Lines 363–417 preserve real period creation, period origin identity, the genuine funded-period exception and disjoint active-hold capacity subtraction. No accepted-input sidecar becomes a financial counter or capacity ledger.

The existing shared valuer is called once at lines 419–420 with the genuine original policy and accepted pay date. Its narrow missing/ambiguous-window handling and conservation checks remain; other errors rethrow. Frozen instructions retain the exact accepted rule and original policy alongside the real monetary revision/period/capacity operands. Destination capture occurs once per instruction.

The unchanged allocation tail preserves the existing allocator, result rows, holds, capacity uses, case/component/period counters, positive payout basis, real gross fallback contribution, completion counts and Worker/job terminal receipts. It introduces no new calculation, synthetic zero, second clock or financial owner.

## Scope and dependency limits

The function remains SECURITY INVOKER with a fixed search path and all direct browser/service-role execution revoked; the existing authorized service owner calls it. No new table, grant or external effect is introduced.

The separate 1519 collection trigger closure remains pending outside this consumer subset: its guard must accept exact retained ordinary collection identity/policy without requalifying later live inputs. This review does not claim that trigger closure or the full Source/book/query/settings card is finished. Root's installation, source gates and local SQL compilation remain separate evidence.

This review also found the stale nonexistent `case_due_date` assignment in the earlier 2303 source. WC05 and its corrected fresh hash are recorded in `F-25-week-capture-review.md`; the saved final capture removes that assignment and this review uses the corrected hash above.
