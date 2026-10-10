# WB-08 Case, bounded fanout and metadata source review

Read-only review on 10 October 2026 of `.codex-local/WB08-case-metadata-owners.sql`, all four drafted owners and the trigger attachment, against card02:320–386, WB-08-D08/D09/D11/D12, `design/WB-08-workbench-projection.md`, native0300 Case schema, native0330 creation, original0360 preparation, current Rule-B0151, snooze1100, and the drafted summary/schema dependencies. No SQL, tests, business calls or product edits were performed. Root is still authoring; this is a bounded source review, not whole-item acceptance.

## Remaining finding H01: payout due date uses the repayment schedule

`refresh_case_v1` assigns `v_due := coalesce(v_rule.next_due_monday,v_case.due_date)` to PAYMENT and DEDUCTION components alike. A Loan/Payment Advance has a PAYOUT component and a RECOVERY component under the same native rule. The rule's `next_due_monday` is the repayment schedule start. A currently payable advance whose repayments start later is therefore wrongly classified FUTURE_WEEK and made unselectable until that repayment date.

The original0360 owner applies the next-due schedule in the RECOVERY branch; its PAYMENT branch derives remaining payout from approved minus funded/paid credit/writeoff and does not postpone payout to repayment start. Native0330 records Case `due_date` separately from the repayment `input_start_monday` and creates both components. Minimal correction: derive PAYMENT eligibility from actual Case `due_date`; derive DEDUCTION eligibility from the actual repayment rule. Do not alter allocation/headroom or introduce another lifecycle owner.

## Closed boundedness finding H02

The first reviewed draft aggregated all linked collection Cases before enforcing the 100-owner cap. The fresh saved source now places `ORDER BY id LIMIT 101` on the distinct explicit/linked Case union before `array_agg`, then refuses total distinct WORK+CASE owners above100 before owner locks. Input arrays also have a16KiB cap and null/missing-owner refusals. This retains all-or-refuse behavior without an unbounded materialized identity array. Larger actual fanout remains WB44's indexed continuation requirement, not a truncated refresh success.

## Verified scoped behavior

- Native Case quantities match0300 component counters and0360: deduction capacity is funded principal for LOAN/ADVANCE and approved principal otherwise; realised recovery/writeoff and ACTIVE recovery reservations are subtracted separately. PAYMENT uses approved minus funded/paid-credit/writeoff and ACTIVE payout reservations. No earnings, payroll NET, take-home headroom or money conversion is calculated.
- Positive available and active reserved cash create disjoint portions. A/C and the full outstanding deduction balance occur once; the additional RESERVED portion carries H without duplicate A/C when an available portion exists. Fully reserved components remain Waiting; genuinely settled components are absent rather than manufactured as tickable zero repayments. A repayment with an outstanding positive obligation remains Ready without a recoverability estimate.
- Accepted scope uses actual SEALED Case selection, actual selected component, sealed run, run_command and PREPARE command. It does not require a worker. Fresh source also handles an existing PREPARING worker whose `case_selection_revision` is still0 by retaining the selected latest SEALED revision. This matches current0151: claim creates the worker before case preparation assigns that revision. Captured instruction money remains unknown until present. READY/DRAFT/ISSUED workers remain IN_PREPARATION; no finished-preparation loophole restores Ready before cancellation/issue closure.
- REVIEW, PAUSED, genuine future-date and matching snooze states have explicit priority beneath damaged evidence and accepted admission. The Case snooze basis uses exactly the same ordered native component/rule tuple as1100. No selected input is refreshed through parked F25 authority.
- `refresh_many` discovers genuine linked collection Cases before prelocking sorted Candidate controls, WORKs, Cases and metadata; singleton refreshes run afterward. Missing controls refuse. No unrelated Candidate debt/history enumeration occurs.
- Bank warnings change only the exact metadata row/version and pending-continuation barrier. The dirty Candidate count increments only on false-to-true pending transition. Equal warnings return without a version increment. The NEXT-gated name trigger listens only to display/first/last name/reference, preserves the previous name when first creating metadata, then records the new name/version and the same pending transition. Neither owner changes financial contributions/section or directly sweeps item rows. WB44 still owns actual adoption and flag clearance.
- Case refresh replaces its exact indexed group and feeds old/new typed contributions to the sole delta owner. New owners are SECURITY DEFINER with fixed `pg_catalog,private` search path and NEXT admission; the name trigger returns early outside NEXT. Final package ownership/ACL/registry/install checks remain root's later batch verification.

## Source receipt

At the latest read, Case/metadata draft SHA256 was `82e97cff0d611e0c97d056e5733f044b108b61c120e695ae2652197733aba95f`; this includes the revision0 accepted fallback and H02 limit correction, before H01 correction.

- Workbench design: `81fb741983bb6addb970b97b3a077cd9758789aa008c8e809c49d9e8d3c75182`.
- Native0300 schema: `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e`.
- Snooze1100: `9b6ceda100c313e6ee879f73102740f38d20cd7e9e6c49bf7bd9affc7117fdf2`.
- Rule-B0151: `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`.

Verdict: H01 remains open in this receipt; H02 is closed. No whole-WB08 or application execution claim.

## Fresh final1252/1316 closure

Fresh saved source was reread after packaging into `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql` and addition of `supabase/migrations/10102026_1316_banking_pay_next_workbench_native_conservation.sql`.

H01 is closed: PAYMENT now uses actual Case `due_date`; only DEDUCTION uses `coalesce(rule.next_due_monday,case.due_date)`. This retains the independent native payout and repayment lifecycles. H02's101-owner sentinel and the accepted worker revision0 fallback remain present. The four reviewed owners retain their fixed security/search-path settings, NEXT gating and owner-only ACLs in the final package.

The new Case guard sums exact per-component A/C/H/W/residual/debt from stored portions and compares them with native capacity, realised counters, active reservation, writeoff, available residual and outstanding deduction balance before old/new deltas are applied. The RESERVED sibling zeros duplicated A/C/W/debt only when an AVAILABLE row carries them; a fully reserved component carries its native facts once. A mismatch refuses the transaction. No headroom, valuation or new money writer was added.

Migration1316 adds exact nullable writeoff with explicit known/unknown and ZERO constraints, and a group certified-zero flag. The final owners initialize known WORK writeoff to0, retain actual Case writeoff, and keep unavailable evidence writeoff NULL. A certified-zero evidence group is qualified by complete current authority, certified-zero revision, zero approval, zero expected lines and absence of any native position. Only that typed informational evidence is excluded from unknown-amount counts; missing evidence remains unavailable. This supplement reviews those new seams, not the entirety of the independently reviewed WORK projection.

The name trigger remains an effective UPDATE hook only, preserves OLD initial metadata, and changes no earnings/section. Name/reference and bank-warning updates increment metadata version and dirty Candidate count only on a new pending transition; pending/adoption clearance remains explicitly WB44's unbuilt closure. No insert-hook, current-row sweep or fabricated completed adoption was introduced.

Final scoped source verdict: PASS; H01/H02 closed, no remaining actionable defect found in these Case/metadata/fanout and additive conservation seams. Whole WB08 acceptance, business scenarios and WB44 adoption are not claimed.

Final SHA256:1252 `ad8cd9ef8f19ad0833d03e9230a84f83d119a32048c6cb67b07c9b6f91c42a56`;1316 `367f4e133ff30453e6eeb9ce86cf96ff503f45ebb4da94b3ad96a91bf4098339`. No SQL/tests/runtime were executed by this reviewer.
