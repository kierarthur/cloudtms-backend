# WB-21 / E1 native eligibility seams

Read-only source investigation, 10 October 2026. No SQL, business/runtime/test execution or product changes. E1's signed eligibility means the exact native selectable member/portion, not a generic positive-amount storage rule. The original monetary owners and Rule B remain authority.

## Signed approval and exact selection identity

`03102026_1324_banking_pay_next_core.sql:188–211` stores approved lines with exact `(revision_id,id,component_key)` uniqueness, signed `approved_quantity` and signed `source_pay_ex_vat`. Its explicit comment says a complete Source head can contain signed correction components and the producer decides allowed origins. There is no shared nonnegative constraint to invent.

Current `1236_banking_pay_next_segment_authority_final_v1.sql:1685–1698` rejects negative Source hours only for the initial snapshot; a committed HEAD explicitly freezes every nonzero signed bucket. Negative Source correction hours are genuine evidence. Segment migration1100:11–31 stores signed approved hours/buckets/amount; `pay_excluded` implies amount zero. That exclusion is retained ordinary `exclude_from_pay`, not a new Timesheet hold or a negative-sign ban. Ordinary producer1236:714–751 copies its actual day line, original segment amount and excluded flag without changing the source amount. Never replace these signed stored facts with zero or take their absolute value.

Current component append0570:201–260 first proves the Work is APPROVED/current/applied at the exact expected revision and then proves each requested `approved_line.id` belongs to that revision. It has **no sign filter**. A missing/foreign line remains the actual `BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION` identity refusal, not a valid business skip. Original immutable page/replay comparisons precede fresh admission. Component seal0570:264–295 binds exact counts and original revision.

Segment append1236:236–287 proves the exact approved segment `(revision_id,segment_key)`, within the actual selected component inventory, then captures its original snooze disposition. It has no sign filter. Segment seal293–326 proves the **complete** selected approved segment inventory, including excluded/snoozed and signed members. Adding `approved_source_ex_vat>0` to these complete inventory counts would break conservation/seal semantics.

## Actual native selectable predicate

The current classifier is `private.bpay_next_workbench_refresh_work_v1(uuid)` in1410. Its complete approval predicate58–60 is:

```sql
w.approval_state = 'APPROVED'
AND r.id IS NOT NULL AND r.sealed_at_utc IS NOT NULL
AND w.applied_revision_id = w.current_revision_id
AND (SELECT count(*) FROM private.bpay_next_approved_line a
     WHERE a.revision_id = r.id) = r.expected_line_count
```

Its exact point/member authority is `private.bpay_next_workbench_work_members_v1(work_id,revision_id)` in1252:197–281. It retains original line/segment identity, actual position A/C/H, bound Source-book READY/approval/native-balance equality, parent completeness and full segment/origin attribution. A moved/removed segment can have real original credit even when its current approved amount is zero; the union includes those original native keys. Do not limit it to today's approved positive rows.

1410:137–158 requires complete approval **and** `parent_complete` **and** `segment_attribution_complete` before treating money as known. It partitions actual amounts:

```text
positive = max(A - C - H, 0)
recovery = max(C - A, 0)
held overhang = max(H - max(A - C, 0), 0)
```

1410:186–250 creates an AVAILABLE positive portion, or a genuine certified ZERO with an original approved line and A=C=H=0. `selectable` is `section='READY' AND amount_known`; the positive qualified Source gate may then change only AVAILABLE to ACTION/BLOCKED and clear selectable. A recovery-only Work portion becomes RECOVERY / INFORMATIONAL / `selectable=false`. RESERVED is separately retained and never selectable (252–262). No whole Work skip should erase a still-selectable positive sibling merely because another component/segment has recovery or an active reservation.

For E1's requested **Work payment member**, reuse a point predicate on the genuine accepted projection identity, without a sign test:

```sql
EXISTS (
  SELECT 1
  FROM private.bpay_next_workbench_group g
  JOIN private.bpay_next_workbench_item i
    ON i.group_key = g.group_key AND i.candidate_id = g.candidate_id
  WHERE g.group_kind = 'WORK' AND g.work_id = p_work_id
    AND g.current_revision_id = p_expected_revision_id
    AND g.complete AND NOT g.dirty AND g.progress_state = 'CURRENT'
    AND i.work_id = p_work_id AND i.revision_id = p_expected_revision_id
    AND i.component_key = p_component_key
    -- For component choice: i.approved_line_id = requested exact line UUID.
    -- For segment choice: i.segment_key = requested exact stable segment key,
    -- and i.approved_segment_id = that revision's actual segment UUID.
    AND i.projection_revision = g.projection_revision
    AND i.financial_view_revision = g.financial_view_revision
    AND i.progress_state = 'CURRENT' AND i.amount_known
    AND i.section = 'READY' AND i.selectable AND i.direction = 'PAYMENT'
    AND i.portion_kind IN ('AVAILABLE','ZERO')
)
```

This is a proposed finite **read** of existing fields, not a new calculator/helper/protocol. Bind it to the accepted Create context/expected revision and retain its decision; do not substitute a later current projection during upload. Query open/unavailable, future week, snooze, hold and missing approval have their genuine distinct E1 skip reasons; do not reduce every false predicate to a generic signed-line failure. Dirty/incomplete projection and unknown native evidence are unavailable, not proof of zero or negative disqualification. Exact foreign/malformed identities remain original refusals.

Do not apply the Work-payment predicate to Case selection. Genuine Case deductions/recoveries are `CASE_COMPONENT`, use their original typed Case owners/counters, and can be selectable. Current native capture1236:3212–3242 explicitly qualifies an original ordinary PAYE Work-collection liability for negative residuals, with exact Case/origin, revision/approved/realised tuple and native zero-held basis. Unknown/cross-basis shapes remain their original financial-resolution path. Full child capture2888–2958 also retains R/H and original parents; included/excluded **positive** portions change with selection, R/H remain genuine. E1 is not authority to rewrite those consumers or omit origin/negative rows required for conservation.

## Exact Timesheet pay-hold owner and current seam

The native flag is **public.timesheets_financials** `pay_on_hold`, `pay_on_hold_reason`, `pay_on_hold_since_utc`. Current Worker `handleTimesheetPayHold`109997,110083–110116 calls `guardCurrentTimesheetWrite`, obtains `guard.resolved.current_timesheet_id`, and PATCHes only `timesheets_financials?is_current=eq.true&timesheet_id=<that current UUID>`. Setting hold stamps UTC; removing clears the flag/reason/time. Existing legacy fast read02052026_1528:2546–2552 likewise reads alias `tf0` by exact Timesheet UUID and `tf0.is_current=TRUE`; lines2508–2510 expose these three actual fields.

The genuine NEXT point join for CREATE/append E1 is:

```sql
SELECT t.timesheet_id, t.version,
       tf.id AS financial_snapshot_id, tf.timesheet_version,
       tf.pay_on_hold, tf.pay_on_hold_reason, tf.pay_on_hold_since_utc
FROM private.bpay_next_work w
JOIN public.timesheets t
  ON t.booking_id = w.booking_id AND t.is_current = TRUE
JOIN public.timesheets_financials tf
  ON tf.timesheet_id = t.timesheet_id AND tf.is_current = TRUE
WHERE w.id = p_work_id;
```

Positive hold qualification is the returned `tf.pay_on_hold IS TRUE`. Use the booking's **current Timesheet**, never `w.original_timesheet_id` or a retained revision's old physical UUID as a current-hold fallback. Baseline22082026_1501:9806 has unique `(booking_id) WHERE is_current`;9968 has unique `(timesheet_id) WHERE is_current`. A booking may retain older Timesheets with each one's own current TF, so TF-current alone is insufficient. Missing current identity/TF is not positive “unheld” evidence; preserve actual unavailable/approval qualification rather than borrowing historical rows. No new writer/trigger/control framework is required for this finite E1 read.

This same join covers ordinary, Source and protected Source. Current1236 Source stage1630–1641 creates/finds the Work by genuine `v_ts.booking_id`; original physical UUID is separately retained. Protected stage2587–2590 uses that same booking key and `work_kind='SOURCE'`. Source query still binds its accepted physical revision through the existing gate/snapshot; reading current hold by booking does not replace Source origin/query authority.

WB08 schema1252:115 contains nullable projection `pay_hold`/`pay_hold_reason`, but no current1252/1332/1410 population or hold classification branch exists. The source census found those fields only declared and transported by reader descendants. **Do not rely on those null fields for E1.** WB40 owns the later native hold-change/rotation trigger, retained ordered hold control and classifier branch. `private.bpay_next_hold` / position `held_source_ex_vat` are Draft reservations, not the Timesheet flag. Existing legacy segment `exclude_from_pay` is already zeroed at authorisation and is also separate.

## Read snapshot

- 0570 component selection: `d19da6893ce30575e8e186d8ce56b82b76e9fb61160f0f560430c682c9955f46`.
- 1236 segment authority: `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`.
- 1410 current classifier: `3844f792bcf48228e3554a3a695d1d14d0706a196d519bb247d1ee1a48572dcb`.
- 1252 member/summary owners: `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b`.
- 1100 segment schema: `0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`.
- 1252 summary schema: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`.

All findings are source-only. No skips, monetary outcome or real hold/correction was executed.
