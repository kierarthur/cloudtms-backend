# WB-44 owner A2 bounded stale-snooze continuation design

12 October 2026. Source-only design supplement to `WB-44-stale-snooze-investigation.md`. Authority: complete approved `02_WORKBENCH.md:1287–1330` (WB36, especially1300/1304/1312),1557–1580 (WB44), every current WB36/WB44 checklist row, and11October owner A2 in WB44 ITEM/D12. It closes that correction before WB39seq116; it does not build WB39 or reopen producers. Parent alone writes product. Business SQL, database checks, tests, fixtures, hosted calls, application journeys and timing **NOT_RUN**.

## 1. Existing native owner and one continuation

The registered financial owner is `private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)`, final `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql:238–440`. Its exact terminal reply275–281 and stale cursor readback288–293 remain ahead of all mutations. Keep its NEW financial inserts/updates308–365, REMOVED reconciliation366–393, approved-line counts, funds/reservations, accepted publication identity and original lease ownership. The required additional phase is **SNOOZE**, after REMOVED exhaustion and before original final APPLIED/pointer/command/job completion395–409.

This refines the earlier investigation's “final replacement” placement: an arbitrarily large legitimate active snooze scope cannot be cancelled in one final transaction. Intermediate cancellation pages belong to the same accepted publication; the publication remains APPLYING and the Work remains financially pending until the original owner completes all pages. Intermediate positions or today's latest revision are not authority for cancellation.

Reuse the existing publication and job `cursor_key text`; SNOOZE holds one canonical snooze UUID or NULL, never an encoded object. Existing publisher1346:205–231 seals the approved revision, advances current_revision, marks Processing, then accepts the original POSITION_APPLY command221 and its one exact Candidate member. No replacement command, job kind, table, request envelope, provider connection, synthetic actor, payment calculation or publication producer is needed.

## 2. Finite traversal and original completion

1. REMOVED exhaustion changes the existing publication to APPLYING/SNOOZE/cursorNULL and the existing leased job to SNOOZE/cursorNULL. It must not change Work.applied_revision_id, complete the command/job, or pretend the projection is current.
2. Each SNOOZE STEP reads at most the original p_limit1–100 active controls on the exact Work using UUID order: `work_id = native_work AND active AND (cursor IS NULL OR id > cursor::uuid) ORDER BY id LIMIT p_limit`. Acquire those snooze row locks in that same order after the existing owner locks. Advance the cursor over **every visited row**, including unchanged and later controls; don't use changed-count as traversal progress.
3. Cancel only genuine earlier controls whose worker basis differs from this publication's sealed revision, or whose stable scope membership has genuinely disappeared. Keep a later control and keep equal worker basis. Changed-count is separate from rows_visited. SNOOZE visits must not increase job.applied_line_count or invoke original financial reconciliation.
4. Persist the identical last visited UUID in the publication and job in the same transaction as its control/event changes. A full page remains SNOOZE. A short or subsequent empty page completes the original395–409 final block exactly once. A scope larger than100 is not truncated.
5. Preserve the original final financial-view invalidation421–424 and refresh_many430 after the real applied pointer; retain Processing434 on every intermediate page and when an older publication finishes behind a newer approved current revision426–434. Do not make mere unchanged SNOOZE visits a new financial mutation. Snooze's existing trigger already owns metadata dirty/ORIGIN scheduling.

Existing job equality and same-phase stale-cursor readback prevent repeated events after a lost STEP response. The original DONE reply remains publication-specific even if a later approval has changed Work. Queue continuation already copies returned POSITION_APPLY phase/cursor without encoding them (`broker/src/bpay-next-financial-queue.js:116–123`); DR04 queue body remains exactly{id,kind}.

## 3. Sequence and worker-basis authority

Use the **original publication command's agency_sequence** for cancellation control_sequence and its command UUID for last_request_id/event.request_id. Only controls with old.control_sequence strictly less than that sequence are eligible. This keeps version/sequence monotone and preserves later accepted controls: the current snooze context already permits controls for current_revision before that revision is financially applied (`11102026_0312_banking_pay_next_snooze_context_controls_v1.sql:97–100`). Do not mint a fresh SNOOZE_CLEAR command when the job runs, and do not move the cancellation across an accepted Create cut.

Use these already-registered bases, without changing their formulas:

| Native scope | Publication-revision point source |
| --- | --- |
| WHOLE_TIMESHEET | `private.bpay_next_revision_worker_basis(revision_id).whole_basis` |
| EXPENSES_ALL | same immutable row's expense_basis |
| SEGMENT | exact (publication.revision_id, old.segment_key), then `private.bpay_next_segment_pay_basis_v1(uuid)` |
| EXPENSE_LINE | exact (publication.revision_id, old.component_key), genuine EXPENSE/MILEAGE line, then `private.bpay_next_line_pay_basis_v1(uuid)` |

Helpers and seal are final `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:10–42,85–96`; equivalent scope mapping is0312:104–127. Native stable-key uniqueness already supplies the segment/line points. Require the publication revision to be sealed, belong to this Work, retain its declared revision number, and have its completed original line count. A missing entire revision basis or an inconsistent existing native membership is a refusal, not inferred zero. A genuinely absent stable segment/expense membership is a removed scope; no date/ordinal remap. CASE snoozes have WorkNULL and stay outside this Work phase.

Equal worker basis preserves charge-only changes, reorderings, labels and physical revision churn. Original snooze fields, original human created_by/updated_by and captured selected/run snooze references remain intact; frozen input/offers are not recalculated. Existing immutable event-at-cut readers continue using the same native sequence.

## 4. Exact cancellation row and event

Current tables already support the operation (`10102026_1100_banking_pay_next_snooze.sql:7–60`): event_kind CANCELLED, nullable actor_id, unique(snooze_id,version), before/after JSON and server time. Update only active=false,version=old+1,control_sequence=publication sequence,updated_at_utc and last_request_id; preserve all scope/basis/revision/physical/human-origin fields. Append one existing event: CANCELLED,actorNULL,reasonWORKER_PAY_CHANGED, same sequence/request/version/Candidate and exact before/after control snapshots. The immediate event guard also point-checks the previous version, including the existing historical cleared_reason normalization.

Prefer keeping the row's cleared_reason NULL: the reason is already the event's explicit reason. Then `bpay_next_snooze_due_reason_check` in migration `11102026_0255_banking_pay_next_due_recovery_metadata.sql:8–10` remains unchanged. If the implementation instead stores WORKER_PAY_CHANGED in that column, it needs its own exact additive CHECK extension; the installed DUE-only migration cannot be edited.

## 5. Immediate guard admission and deferred receipt are different checks

Current `private.bpay_next_snooze_metadata_guard_v1()` is final `11102026_0255_banking_pay_next_due_snooze_guards_v1.sql:43–171`. Its current RECEIVED/human SET/CLEAR and WOKEN/DUE rules55–90/109–132 deliberately reject this operation. Add an exact publication cancellation branch **before** those ordinary checks; keep their bodies and warning ACK/request rules unchanged.

At immediate row UPDATE/event INSERT, require service_role plus the current NEXT/module epoch, original POSITION_APPLY command **SEALED** with expected/enrolled1 and its exact Candidate member, publication APPLYING/SNOOZE, the command's unique POSITION_APPLY job LEASED/SNOOZE with the Candidate's native active_owner_epoch, valid lease at the mutation, twin **old** cursor, Work.applied_revision_id equal to publication predecessor, sealed matching new revision and original complete line count. Require the row's exact Work/Candidate, active→inactive, old control_sequence < command sequence, full preserved tuple and actual basis mismatch/removal. Its UUID must lie strictly after the old UUID cursor, or the old cursor must be NULL. The immediate event branch additionally validates actorNULL/reason/version/request and its exact preceding and resulting snapshots. It must not require an already advanced cursor: event INSERT precedes the page's final twin checkpoint UPDATE.

Current deferred `private.bpay_next_snooze_receipt_guard_v1()` is same0255:173–265; its ordinary fallback190–205 requires a human snooze_request, which does not exist for a publication. Add the separate system CANCELLED branch before that fallback. For each firing row/event, perform indexed **point** checks using that exact snooze ID/version/request. No all-events-for-command aggregation, receipt count, history traversal, permissive system bypass or artificial zero-member snooze receipt.

At end of the page transaction, verify one of these exact native states:

| Intermediate receipt | Final-page receipt |
| --- | --- |
| Command SEALED; publication APPLYING/SNOOZE; its exact job READY/SNOOZE with NULL nonce/lease after the existing financial STEP wrapper's own successful yield, or LEASED/SNOOZE with the real nonce and lease valid at this event's mutation time; job.owner_epoch matches the Candidate's native active_owner_epoch; publication.cursor_key equals job.cursor_key and parses as the visited UUID at or beyond this event's snooze ID; Work.applied is still predecessor. | Command COMPLETE; same publication APPLIED/DONE with applied_at; same native job DONE/DONE, NULL cursor/lease; original line count complete and the Work applied pointer changed to this publication's sealed revision in this final transaction. |

The intermediate branch must check the **page checkpoint**, not require an already final applied pointer. The current financial STEP wrapper validates the owner's result and twin checkpoint before yielding its exact lease to READY in the same transaction (`11102026_2259_banking_pay_next_prepare_retry_v1.sql:889–945`); deferred checks therefore must admit that exact READY state with NULL lease. Immediate admission still proves the active lease at mutation, and a direct native LEASED receipt checks the lease against the event's server mutation time. The final branch cannot demand an active lease that the original final block cleared. Do not invent a replacement nonce, persisted lease proof or new receipt. All owner locks stay held through the deferred checks.

For either branch: command/job/publication/member sequence/epoch/Candidate/Work tuple equality, original revision/line-count binding, one exact matching CANCELLED event with NULL actor/reason/snapshots/version, the exact previous-version event, and original basis mismatch/removal. The unique job(command,Candidate,kind), publication command PK, event(snooze,version) unique and row PK suffice. A small private point validator may share these assertions between the two guards; its signature should take the exact snooze ID/version/request (not a generic context/certificate) and remain private/no browser or service EXECUTE grant.

## 6. Required owner and schema/transport delta matrix

| Actual current object | Small necessary change |
| --- | --- |
| Publication phase CHECK, core migration `03102026_1324_banking_pay_next_core.sql:943` | Add SNOOZE in a new dated migration using the real installed constraint name; retain APPLIED↔DONE/applied_at956 and every FK. Native job.phase975 is plain text and needs no phase-column migration. Existing cursor944/976 is reused. |
| Active Work snooze read support, original1100:47–49 | Add only `ON private.bpay_next_snooze(work_id,id) WHERE active`. Existing Work/scope/history indexes do not lead directly to this UUID seek. No replacement of existing due/scope uniqueness indexes. |
| Apply position owner, final1346:238–440 | One new bounded branch plus REMOVED→SNOOZE and transfer of original final block to exhausted SNOOZE. Preserve NEW/REMOVED financial formulas, replay, lease and projection behavior. |
| Metadata guard / deferred receipt guard, final0255 | Exact native CANCELLED system branches as above. Existing guard triggers/ACLs reused. |
| `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)`, final `11102026_2259_banking_pay_next_prepare_retry_v1.sql` | POSITION-only STEP phase allowlist475–480, CLAIM precheck605–614 and claimed phase713 admit SNOOZE;530–536 conditionally validate a canonical UUID cursor in this phase. Existing STEP call777–781, returned twin checkpoint889–894 and output955–956 stay the same shape. |
| `private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean)`, same2259:239–362 | Treat SNOOZE as a completed financial-line phase319 and permitted pending publication phase340. Validate native SNOOZE cursor UUID/same Work point, preserve predecessor341 and repair only from the original publication's twin checkpoint346–351. Original receipt replay273–277/292–296 stays unchanged. |
| Worker `broker/src/bpay-next-job-step.js` | Add SNOOZE only to POSITION typedef34, PHASES64, field138/140. Request198–200 and CLAIM/STEP result226–231/266–267 require NULL-or-canonical UUID cursor conditionally for SNOOZE; keep existing component-text cursors for NEW/REMOVED. No new result fields, byte/deadline/page bounds or ordinary PREPARE enum changes. |
| Worker `broker/src/bpay-next-workbench.js:811` and FE `js/bpay-next-workbench.js:737` | Add SNOOZE to the existing POSITION retry receipt READY phase allowlist and conditionally validate its UUID cursor; leave PREPARE/PF08 clauses and retained request/response keys untouched. Their generic phase/checkpoint fields792–805 and718–731 already fit. |

**Unchanged registered owners:** claim_position_job_v1(uuid,integer) remains `03102026_1430_banking_pay_next_position_writer_v1.sql:397–451`; it has no phase allowlist and already preserves native Candidate ordering/earlier-command exclusion and fresh nonce ownership. Work pointer guard is `11102026_0928_banking_pay_next_daily_work_identity_guard_v1.sql:4–49`; keeping predecessor until completion and sealed new pointer on completion satisfies it without edits. Publisher1346, basis/seal1236, event-at-cut reader1100, existing snooze trigger1429:7–29 and final refresh_many remain their current owners.

Worker queue116–123, job RPC kind list, intake/index POSITION wake body, background id-only wake, ordinary financial owners and existing Office Retry controller require no new phase envelope/route/action. Update the final-definer registry/closure to the actual successors and only necessary finite source-gate callee edges if a private point helper is introduced; no historical/dynamic blanket waiver.

## 7. Source-known concurrency and evidence boundary

Lock order remains module SHARE → Candidate control → exact job → exact publication → exact Work → bounded snooze UUID rows; basis lookup is on sealed native rows. Snooze's existing Work trigger schedules ORIGIN metadata, and the original final refresh owns linked Cases. Do not take new Case locks in the cancellation branch or invert Work/Case order. No agency sweep or full active/history materialization is required. Later active controls inserted/updated between pages have later command sequence and survive this older publication; already cancelled rows leave the active index while the durable UUID cursor advances atomically.

Original parent raw hashes at investigation: apply/publish1346 **8934065D152AB69BBE2EC8EE298BD03B66F4C20A82FBBCDCED78567DB37BB69F**; snooze guards0255 **B0D1EFA5398F6340E3C3A399B7334BBABD46F33F9128731945A81587872D8C16**; bases1236 **F74EA457BF77F0D85CE7F3F21089BDECF9D9D9121D4005850DE430D50AA7379D**. They are source provenance, not installed catalogue or execution evidence. All proposed changes remain unbuilt by this reviewer; exact catalogue constraint name, compile, contracts, actual browser/worker/database journeys and load performance remain root's authorised checks.

