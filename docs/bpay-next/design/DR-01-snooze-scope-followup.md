# DR-01 snooze and empty-selection scope follow-up

Read-only source follow-up, 10 October 2026. No product changes, SQL, runtime, tests or fixtures executed. Clarifies the earlier current-scope investigation; existing Rule B seal/enroller remain unchanged.

## Reuse the existing WB36 Create cut

Current segment final `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:172–192` defines `private.bpay_next_segment_run_boundary_guard_v1()`. On INSERT of the actual pay run, it reads `bpay_next_command_clock.last_sequence FOR SHARE` into `new.snooze_control_sequence` and captures the London clock date in `new.snooze_business_date`. UPDATE refuses a change to either field. These columns already exist in migration `10102026_1100_banking_pay_next_segment_exclusion_v1.sql`; no extra date/clock/protocol is needed.

**Prefer `v_run.snooze_business_date`, not a freshly derived date or run.created_at_utc.** It is already the authoritative WB36 Create business date. The earlier note's created_at alternative is unnecessary support in the present schema. A fresh append uses this retained date and `v_run.snooze_control_sequence`, plus the actual selected expected revision's immutable whole worker-pay basis. Exact replay uses its already-retained skipped receipt and does not run the query again. The original CREATE cutoff is distinct from the caller-supplied official pay date.

For work-choice append, qualify the same exact `expected_revision_id` used by its existing approved/current/applied and sealed-revision checks. For ID append, use the current revision that its existing `record_all_work_choice` would capture; do not substitute an arbitrary latest basis row. Missing required immutable basis is unavailable evidence, not positive proof of no snooze. Current whole snooze survives a new revision when genuine `whole_basis` is unchanged; do not require the snooze's old expected revision to equal the new selected revision, which would break that existing basis semantics.

## Minimal exact whole-Timesheet predicate

Current `private.bpay_next_snooze_at_cut_v1(uuid,uuid,text,text,bigint,date)` in `10102026_1100_banking_pay_next_snooze_v1.sql:455–477` supplies the established historical-event semantics. It joins an approved component line and can also return SEGMENT/EXPENSE snoozes, so blindly calling it with NULL component is unsuitable for the whole-work append decision. Inline its **WHOLE_TIMESHEET-only** branch in the named append owners:

```sql
exists (
  select 1
  from private.bpay_next_snooze s
  join lateral (
    select ev.after_state
    from private.bpay_next_snooze_event ev
    where ev.snooze_id=s.id
      and ev.control_sequence<=v_run.snooze_control_sequence
    order by ev.control_sequence desc
    limit 1
  ) e on true
  join private.bpay_next_revision_worker_basis b
    on b.revision_id=v_expected_revision_id
  where s.work_id=v_work.id
    and s.scope_kind='WHOLE_TIMESHEET'
    and (e.after_state->>'active')::boolean
    and ((e.after_state->>'until_date') is null
      or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
    and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis
)
```

This is a design operand sketch using the actual columns, not installed SQL. In the ID-only append, `v_expected_revision_id` means its exact captured current revision, not a new caller parameter. Validate nonnull run cut and required basis through original invariant/refusal handling before treating absence as clear.

Do not add `s.active`, current `s.until_date`, current `s.worker_pay_basis` or current `s.control_sequence` filters: a row may have been cleared/amended after CREATE. Read the latest event **at/before** the original cut, not any old active event. Scope/work identity on `s` is immutable under the current metadata guard at1100:350–363; event after_state is bound by the actual deferred receipt guard. Indexes are already `snooze_scope_history_idx(work_id,scope_kind,scope_identity,id)` and `snooze_event_cut_idx(snooze_id,control_sequence desc)`. The partial active index is inappropriate for this retained historical lookup.

No approved-line/member scan, new hash, new receipt protocol or snooze owner rewrite is required. SEGMENT, EXPENSE_LINE, EXPENSES_ALL and CASE rows never generate `BPAY_NEXT_SELECTION_SNOOZED` for the whole work. Keep WB36 selected-segment inclusion/exclusion and recovery authority unchanged. Original capture uses the same retained run cut for nonsegment snoozes at1236:3294–3297 and selected segment append uses choice.segment_control_sequence inherited from the run at1236:200–208,276–282.

## Page guards and all-skipped boundary

Existing `bpay_next_selection_page` (core1324:439–449) has the bounded original ID array, positive first selection number, attempted item_count1..100 and PK(run_id,page_no). Source census finds **no dedicated selection_page row trigger** in the current guard/segment definitions. Its access is private and ordinary public/anon/authenticated/service_role table privileges are revoked by core1324. Existing upload owners hold the run root, require OPEN/contiguous page number and compare replay identity. Component/segment page triggers belong to their separate tables; they do not prove this ID upload page is already trigger-frozen.

Retaining a bounded exact original choice request and skipped output on this existing page is narrow support for replay. Keep attempted item_count positive even for all-skipped input and selection_count equal to actual appended rows. Do not add a blanket immutable DELETE guard: PF04 `private.bpay_next_terminal_cleanup_v1` at0406:38–47 deliberately deletes these temporary upload buffers only after locked COMPLETE/CANCELLED run qualification; accepted selection/choice/query/frozen rows remain. An UPDATE/INSERT check, if genuinely needed, must preserve this existing terminal cleanup authority rather than invent a new audit/money owner.

Current0151 seal first rejects `(expected_pages=0) IS DISTINCT FROM (expected_count=0)`. Current1590 enqueue additionally rejects zero-WORK selection when selection_page_count<>0, even when real Case candidates exist. This is an explicit **two-owner** boundary, not just a PIN problem. The public1236 SEAL_ENQUEUE calls seal then enqueue in the same transaction, so a rejected combination does not create a PREPARE command or enrollment.

DR01 explicitly requires individually skipped selection-page feedback and its 10-input/9-appended example. It does not explicitly require an all-skipped WORK page plus real Case candidates to become a zero-WORK executable Draft. Consequently that combination does **not** justify rewriting Rule B seal/enqueue/enroller for this card. Accept and record the all-skipped page, report zero actually selected work truthfully, retain the existing empty Draft refusal. The genuine existing Case-only path remains zero WORK pages, actual selected Case candidates and original Case seal checks. Future WB21/DR02 sequencing may need to distinguish no selected work from a Case-only submission; do not silently erase an accepted page, fabricate WORK or declare that the existing zero-page Case-only path supports pages>0. Any later required mixed flow must name its finite owner correction explicitly.

Source hashes: snooze repeatable `9b6ceda100c313e6ee879f73102740f38d20cd7e9e6c49bf7bd9affc7117fdf2`; segment final1236 `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`; Rule B0151 `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`; manifest1590 `47a0df6a06bd7816b0102c1aa5fca0bddd97ebc08a0a04d786254db3179967b3`.
