# WB-29 Ready date rollover — finite implementation note

13 October 2026. Source-only investigation. No SQL, product, database, business RPC or test operation. All proposed implementation/compile/timing/joined acceptance checks are NOT_RUN.

## Current owner identities and invariants

The actual queue/scheduler parent is **`supabase/repeatable/10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql`**, not12102026_1429. Current full successors remain:

| Signature | Current source |
|---|---|
| `public.bpay_next_workbench_week_rollover_page_v1(jsonb)` |11102026_0255_banking_pay_next_due_recovery_pages_v1.sql:122–215 |
| `private.bpay_next_workbench_queue_origin_v1(uuid)` |10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql:268–279 |
| `private.bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean)` |same1429:66–148 |
| `private.bpay_next_workbench_refresh_work_v1(uuid)` |10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql:318–351 |
| `private.bpay_next_workbench_step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer)` |12102026_0645_banking_pay_next_confirm_refresh_v1.sql:215–265/378–389 |

Original0255 selects inclusive London `FUTURE_WEEK` by `(eligible_from_date,item.id)`, advances by **visited** rows including busy/deferred rows, and CAS-updates/wraps its native seek203–208. The existing partial index is1252 summary migration:180. Keep that selection, native identities, refresh/refusal behavior,5s timeout, input `{limit}`1–200, service-only ACL and output unchanged. Native official pay date for chosen-rate qualification is separately read from the existing date-context owner; London today is not its substitute.

`broker/src/bpay-next-runtime-rpc.js`:291–296 requires exact four-field receipt, `changed+deferred=visited<=limit`, and **`complete === (visited < limit)`**. Background170–177 uses limit100, at most two pages per lane/tick, then wakes native Workbench tasks180–184. Do not change this adapter, budget, ID-only message or task owner.

## Static selectors and quota donation

Keep two local page buffers, FUTURE item IDs and chosen-date Work IDs. They are internal native records, not a new public cursor/envelope. Combined physical records stored/visited never exceed `limit`; do not deduplicate the two input counts just because one Work appears in both lanes. Deduplicate only business preholds and mutation scopes. Each buffered record remains associated with its originating selector so changed/deferred and seek advancement are truthful.

The FUTURE SELECT is precisely0255:146–149 with a local quota and, for a donor tail, the last native item key already returned in this call. The chosen SELECT is the approved existing-group partial index scope: WORK, date-sensitive, complete/non-dirty/CURRENT, positive READY-or-ACTION count, and stamp NULL or different from current official date. Use native `(chosen_display_pay_date NULLS FIRST,work_id)` order. Start means seek Work ID NULL; a non-NULL seek Work ID with NULL date means a visited NULL-stamped row. For a non-NULL seek date use its real date/UUID tuple. For a NULL seek date include later NULL-date IDs and subsequent non-NULL dates. A three-range NULL/older/newer official-date qualification may use static index branches; never materialize all stale groups before limiting.

For every `limit>=2`, the exact finite flow is:

```text
qF = (limit + 1) / 2                       -- integer division
F_head = original FUTURE SELECT LIMIT qF after original F seek
nF = cardinality(F_head)
F_exhausted = (nF < qF)

qD = limit - nF                          -- donates unused F quota immediately
D_page = chosen SELECT LIMIT qD after original D seek
nD = cardinality(D_page)
D_exhausted = (nD < qD)

if D_exhausted and not F_exhausted:
    qF_tail = limit - nF - nD
    F_tail = original FUTURE SELECT LIMIT qF_tail after F_head's last key
    F_exhausted = (cardinality(F_tail) < qF_tail)
    append F_tail to F_head; nF += cardinality(F_tail)

visited = nF + nD
complete = (visited < limit)
```

This performs at most three independently limited native SELECTs, with at most `limit` physical returned rows in total. No lookahead needs to be counted or transported. At an exactly full selector quota, exhaustion is deliberately unproven; complete stays false and the next page proves the empty tail. If visited<limit, the donation logic has proved both current seek suffixes exhausted. Cases: F20/D80 fills100 and is incomplete; F50/D3/Ftail47 also fills100; F50/D3/Ftail46 visits99 and completes. An empty first selector donates the entire limit to the second. No first100 truncation, new group-count cap or agency aggregate is introduced.

Concrete static SELECT templates follow; the chosen columns are the **approved additive fields**, not a claim they already exist. Accumulate only returned rows in the local bounded buffers. Run neither template when its quota is zero. Use original seek for the head; use F_head's last returned date/ID for the optional F tail. Retain the old seek separately for final CAS.

```sql
-- FUTURE head and optional tail: identical native0255 predicate/order.
select i.id, i.eligible_from_date as due_date,
       i.candidate_id, i.work_id, i.case_id
from private.bpay_next_workbench_item i
where i.reason_code = 'FUTURE_WEEK'
  and i.eligible_from_date <= v_today
  and (v_f_read_id is null
       or (i.eligible_from_date, i.id) > (v_f_read_date, v_f_read_id))
order by i.eligible_from_date, i.id
limit v_quota;

-- Chosen date page: native group Work key and NULL-first date tuple.
select g.work_id, g.candidate_id, g.chosen_display_pay_date
from private.bpay_next_workbench_group g
where g.group_kind = 'WORK'
  and g.chosen_display_date_sensitive
  and g.complete and not g.dirty and g.progress_state = 'CURRENT'
  and g.current_revision_id is not null
  and (g.ready_count > 0 or g.action_count > 0)
  and g.chosen_display_pay_date is distinct from v_official_pay_date
  and (v_d_seek_id is null
       or (g.chosen_display_pay_date is not null,
           coalesce(g.chosen_display_pay_date, date '0001-01-01'), g.work_id)
          > (v_d_seek_date is not null,
             coalesce(v_d_seek_date, date '0001-01-01'), v_d_seek_id))
order by g.chosen_display_pay_date nulls first, g.work_id
limit v_quota;
```

The chosen tuple predicate can be expanded into the static NULL/later-date point ranges described above if needed for the exact partial index plan; it preserves the same order/identities. Under acquired Candidate/Work locks, recheck actual Work APPROVED/current/applied identity and the current group/date/metadata qualification before queuing. A native withdrawn/incomplete informational group does not become an eternally failing date-repair scope. No query-plan/timing claim is made from these source templates.

### Limit1, agreed narrow rule

Root explicitly selected existing UTC-minute clock parity, rather than an additional persisted next-lane flag. Capture the minute once on entry:

```sql
v_future_first := mod(floor(extract(epoch from clock_timestamp()) / 60)::bigint, 2) = 0;
```

Even minute: FUTURE SELECT LIMIT1; if empty, chosen SELECT LIMIT1. Odd minute: chosen SELECT LIMIT1; if empty, FUTURE SELECT LIMIT1. A zero-quota/unprobed lane is **not** exhausted and its seek must remain unchanged. If priority is empty, record that lane's exhaustion/wrap even when the donated selector supplies the single row. Only both empty gives visited0/complete true; any visited row gives complete false, satisfying the original adapter.

This offers alternating priority by UTC minute, not deterministic same-instant/per-call alternation. Repeated requests during one minute can retain that priority; a caller intentionally visiting only one parity can do so. The real background always requests100, giving both selectors an initial positive quota. Busy rows still advance the native lane key and are revisited after normal wrap. Do not encode a turn in a fake date/UUID, increment a financial/Workbench revision for fairness, or add a persisted counter/flag.

## Independent CAS and wrapping

Read both old seek tuples from existing Workbench revision before mutations, as0255:143–145 does. Maintain, per lane: whether it was actually probed, the last native returned key, and exhaustion proved by an underfilled positive quota. FUTURE can have head plus donor tail; its last key is from the tail if present. Chosen has one page in the normal flow.

After mutations, issue one exact existing-row CAS per **probed** lane. FUTURE UPDATE compares only `(week_rollover_seek_date,week_rollover_seek_id) IS NOT DISTINCT FROM (old_f_date,old_f_id)`; chosen UPDATE independently compares only `(chosen_rollover_seek_date,chosen_rollover_seek_work_id) IS NOT DISTINCT FROM (old_d_date,old_d_id)`. Exhausted => that lane's tuple NULL/NULL; otherwise store its last visited real date/ID. A zero-quota unprobed lane performs no UPDATE. No cursor-only financial/context revision bump. An overlapping page whose CAS loses does not restore its stale seek; repeated work is idempotent through native scope checks/task reuse.

```sql
-- Execute this only when FUTURE was probed with a positive quota.
update private.bpay_next_workbench_revision
set week_rollover_seek_date = case when v_f_exhausted then null else v_f_last_date end,
    week_rollover_seek_id   = case when v_f_exhausted then null else v_f_last_id end
where id = 1 and (week_rollover_seek_date, week_rollover_seek_id)
  is not distinct from (v_f_old_date, v_f_old_id);

-- Execute this only when chosen was probed with a positive quota.
update private.bpay_next_workbench_revision
set chosen_rollover_seek_date = case when v_d_exhausted then null else v_d_last_date end,
    chosen_rollover_seek_work_id = case when v_d_exhausted then null else v_d_last_id end
where id = 1 and (chosen_rollover_seek_date, chosen_rollover_seek_work_id)
  is not distinct from (v_d_old_date, v_d_old_id);
```

Page `complete` means both visited seek suffixes exhausted, as the original circular sweep does; it is not a claim that no busy due row remains before the retained seek. Busy records remain due and are revisited on a later sweep. Date-stale reads remain fenced until repairs actually complete. Changing the date of a row or inserting an earlier key can require the ordinary end-wrap; no first-prefix completion fiction.

## Union preholds and the queue/refresh inversion

The new date lane queues ORIGIN. That queue locks task then metadata/global revision (1429:106–146). A subsequent original FUTURE Work refresh can acquire related Cases at1438:339–345. Acquiring a previously unheld Case after the first queue/global revision would invert the established Candidate→Work→Case→task/metadata/revision order. All business preholds must therefore finish **before either lane mutates**:

1. Discover both final donated native page buffers, with their original keys.
2. Union/deduplicate their Candidate IDs, acquire Candidate controls in UUID order NOWAIT; retain original per-Candidate defer behavior0255:157–163.
3. Union/deduplicate Work IDs, acquire Work in UUID order NOWAIT; remove a Candidate from ready scope on any failed native owner prehold, as164–171.
4. For FUTURE Work scopes, while Candidate/Work are held, discover the exact related-Case prefix used by the existing1438 wrapper: `SELECT DISTINCT case_id ... WHERE work_id=... AND case_id IS NOT NULL ORDER BY case_id LIMIT100`. Union it with explicit FUTURE Case IDs and acquire all Cases in UUID order NOWAIT. New date-only Work scopes need no immediate Case refresh; their native ORIGIN task pages own it later. This preserves the original finite related-Case bound.
5. Only then revalidate each original FUTURE item ID/date/scope and each chosen Work's current stale/sensitive qualification, and execute mutations. Keep the original FUTURE scope loop calls to `refresh_work_v1` / `refresh_case_v1`; chosen invokes only existing `queue_origin_v1`.

The original0255 related lookup178–179 limits raw `q.case_id` without DISTINCT; the wrapper339–341 limits **distinct** Case IDs. The new union prehold must mirror the wrapper's DISTINCT-before-LIMIT membership rather than assume duplicate collection rows cannot occur. This is narrow lock support, not a new Case inventory or a change to financial authority. At100 related Cases the wrapper deliberately queues ORIGIN instead of treating that prefix as full membership342–343. For fewer than100 it can acquire only those already-preheld Cases. Candidate and Work preholds keep the related scope stable across queue and subsequent refresh under the native writers' lock order.

An exact same Work present in both lanes may refresh first and no longer need date repair, or queue first and cause the original wrapper328–330 to reuse ORIGIN; revalidate after the earlier action. Do not force a second generation, falsely claim immediate readability, or count one metadata operation as two independent financial changes. Preserve per-input visited counts and only mark a row changed when its native scope action succeeded or the same call positively fulfilled that exact scope. Remaining rows are deferred. Caught scope errors retain the original per-scope behavior; no active lease adoption.

Current0645 ORIGIN_CASE discovers genuine related Cases215–220, takes whole-page Candidate/Work/Case preholds228–241 **before** the task lock242, verifies exact lease/generation/checkpoint243–265, refreshes Case pages378–386, then Work core and native finish387–389. Scheduler1429:110–115 reuses only matching current Work/applied/approval/FVR/WAIT scope. Retain those owners unchanged. Do not reorder finish ahead of display quotation or clear pending metadata early; the separately recorded private refresh-quotation admission seam handles pending state truthfully.

## Source hashes

0255 rollover `A59E6FDC063CA67181DA82625A3059BFEB1D4085E07ED22AD75042FC0607C4BB`; actual10102026_1429 `DC539089FAE0CD1A8BAF4765A89CEA9FF1D1E8039200ECDA5D80371DDA2335DD`;1438 wrapper `B8D445C4A4CFC02978E8AC77A159350B0E1978AAFF9A1172DA8E7E157A6414F5`;0645 STEP `5C1E24129CF8A14C97C80D4A7027EAF7F434BFFFEC2FF58872A159FE824552A1`;runtime due validator `97F043498D6C8CC2030742BA59EF826F6FFC093CCBB1A937CA6FEE22644BBB84`. These are disk source observations only; no runtime claim.
