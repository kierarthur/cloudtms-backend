# WB-02 withdrawal classifier source review

Reviewed 10 October 2026, independently and by source inspection only. No SQL, business calls, tests, fixtures or runtime execution were performed by this reviewer.

**Verdict: PASS for the current 1332 schema and five-function classifier slice. No remaining concrete defect found in these saved bytes.** This verdict does not claim the later WB-06 final-APPLIED integration, WB-41 Case classification/allocation, WB-44 adoption/fanout, or application first use has been completed.

## Exact reviewed source

| File | SHA-256 |
|---|---|
| `supabase/migrations/10102026_1332_banking_pay_next_withdrawn_projection.sql` | `923b94d3a26e87772243f99cdf90b5c70eab0c123e04bdad9b889ca300a007f2` |
| `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql` | `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391` |
| `docs/bpay-next/final-definer-closures.json` | `34ffb37864de888a8ec11dc7e7ad169217e076b8cdd0e12aa7da6fc83a942759` |

Read with the complete WB-02 card and its nine checklist objects, R2 withdrawal/reauthorisation connections, the preceding WB-08 projection schema/classifier, current 1321 member classification, native position/application owners, original ordinary authorisation trigger and Source withdrawal guard. The registry designates exactly the five new final definitions in 1332: WAIT predicate, work refresh, ordinary trigger, Source withdrawal guard and group contribution.

## Findings closed in the fresh version

1. **Dirty-state lifecycle and revision tracking — closed.** Lines 42–53 preserve the stored `AWAITING_REAUTHORISATION` marker while a newly approved revision remains unapplied, advance the workbench revision, and increment `dirty_group_count` only on false-to-true. The complete refresh at lines 78–93 decrements once when previously dirty and clears the group marker. This preserves the prior WAIT transition for the later real final-APPLIED boundary.
2. **WAIT ownership and exact work identity — closed.** Lines 10–13 require NEXT, resolve one real work with `INTO STRICT`, and use the signed sum of existing realised source amounts. Refresh calls this predicate once at line 38 and reads the prior WAIT from persisted projection rows before replacement (lines 35–37). It does not compare two reads of the already changed current work.
3. **Mixed applied evidence and fallback progress — closed.** Lines 66–77 choose the last applied revision on withdrawal and refuse to present a coherent amount where any economically relevant position belongs to another applied revision. Entirely inert zero positions do not invalidate that evidence. A mixed/nonexistent capture produces nonselectable unknown evidence; lines 133 and 248–249 retain `PROCESSING` for a mixed retained revision rather than overwriting it as current or known zero.
4. **Retained hold count — closed.** Group contribution line 475 counts a `WITHDRAWN` member with genuine positive retained H as held. The member retains A/C/H exactly once; there is no duplicate RESERVED portion or duplicate amount aggregation. The complete contribution function is identical to the prior 1252 definition after removing this one predicate extension.
5. **Classification priority — closed.** Both native withdrawn members and unknown fallback use `AWAITING_REAUTHORISATION` rank 70 and `APPROVAL_WITHDRAWN` rank 20 (lines 124–126, 241–246), matching the agreed finite reason ordering.

## Preserved authority and monetary behavior

The migration adds only the finite `WITHDRAWN` projection portion and its shape constraint. Existing required amount/boolean fields supply the non-NULL guarantees used by that CHECK. A withdrawn portion must be informational, nonselectable, known, and ACTION/BLOCKED; nominal equals retained A, residual equals A−C−H, and payable gross, included/excluded, recovery, overhang and written-off amounts are zero. This is a retained approval display, not a newly approved zero revision or a recovery offer.

Refresh lines 99–130 read actual retained position values and immutable approved-line labels where available. They do not create revisions, positions, jobs, debt, Cases or new financial effects. Current revision remains NULL after withdrawal. Existing accepted Rule-B selections and frozen financial owners are not rewritten. The preceding ordinary member decomposition, native conservation and projection delta owner remain in use outside the withdrawn branch.

On a WAIT transition, lines 55–61 prehold only the exact linked collection Cases, in UUID order and under the already held Candidate/work scope, before metadata/singleton locks. Lines 277–281 then refresh those same Case projections. Actual Case WAIT classification and allocation remain WB-41 work; this slice does not silently implement that future owner.

The ordinary authorisation trigger CREATE body is byte-identical to its 1530 predecessor after removing the single added refresh call at line 360. The Source withdrawal guard CREATE body is byte-identical to its 1580 predecessor after removing the single added refresh call at line 427. In particular, the existing exact-work Source financial-dependency refusal remains at lines 414–417. Approval dispatch, work/Candidate identity checks, lock order and native financial-view increment otherwise remain intact.

All five definitions retain SECURITY DEFINER with explicit search paths, audited logical ownership and revocation from PUBLIC, anon, authenticated and service_role (lines 432–442 and 498–499). No new public routine, browser grant, trigger attachment, producer protocol, resolver or F25 parked owner is introduced.

## Completion boundary

This is a source PASS for the saved schema/classifier support. The existing staged publisher dirty handoff and the real final-APPLIED WB-06 refresh must still be accounted for by their named owners; projection refresh alone is not proof that every caller has been attached. Later WB-41 classification/allocation and WB-44 joined adoption remain separate required work. No actual application/data observation or joined scenario is claimed here.
