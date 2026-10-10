# PF-04 cleanup retention: narrow terminal metadata lifetime

10 October 2026. Read-only design assessment for root's BD026 decision. No product edits or gate/unit/SQL/runtime/fixture/hosted execution. This refines the initial conservative PF-04-next-scope inventory; it does not reopen F25 Rule B or propose an archive/replay framework.

**Recommended disposition:** implement actual bounded deletion of terminal temporary selection_page and eligible DONE job rows, retaining all financial/accepted-input authority. A late reference to deleted temporary metadata is a truthful unavailable/closed refusal, never a manufactured DONE receipt or financial success. Root's exact terminal predicates below are consistent with the current card's express cleanup requirement. Seven FK exclusions alone are not enough; terminal command/enrollment/lease predicates and retained original authority are also necessary.

## Source authority and the apparent tension

The actual native planning source is `codex-review-20260929/fresh-requirements-20261003/DETAILED_REPLACEMENT_PLAN.md`:

- §6.8:257 expressly requires cleanup of terminal temporary selections/jobs only after required references are retained; financial effects and archived Timesheets are never removed.
- §6.2:155 retains the unique business command and returns its original receipt on duplication; §6.2:161–168 makes job steps atomic and retryable, with durable open-job authority and duplicate/delayed queue wakes. §6.2:163 says a lost step response retries the same identity and returns the saved result/advanced cursor.
- §6.3:173 requires an explicit selected-ID manifest and replay-safe upload pages.
- §6.8.1:267 explicitly retains original documents/frozen detail through cleanup.

Current R3 Part B B7:225 and PF04 card06:1745,1752–1760 repeat the terminal-temporary cleanup boundary. No numeric metadata TTL, new archive, permanent retention of every upload receipt, or new cleanup command kind is specified. No native implemented `banking_pay_next_terminal_cleanup_v1`/`bpay_next_terminal_cleanup_v1` scaffold was found in the chosen current BUILD/RESET source or named plan package; the card's scaffold wording describes proposed work.

The bounded interpretation is: exact mutation replay is required while that upload/job remains an open or retained operational obligation; the card expressly permits retiring its temporary metadata after terminal closure and retained financial origins. Financial/business command idempotency and immutable accepted economics continue indefinitely through their original retained authority. This is a documented lifetime decision, not a claim that deleted metadata still returns its old positive reply. No new time threshold should be invented.

## Selection-page eligibility: real rows may be removed

Delete only `private.bpay_next_selection_page` rows whose original pay_run is **COMPLETE or CANCELLED**, after taking/rechecking that parent scope under a bounded page transaction. PREPARING/REVIEW/DRAFT/EXECUTING remain retained. Never reopen/reset the terminal pay_run to permit another append.

Keep the original pay_run, run_command, command/member, run_selection and selection_candidate; all work_choice expected_revision_id/ALL/SUBSET facts, component/case selections, accepted revisions/publications, Rule-B query snapshots and pin_v2, run_worker/run_work/run_line/frozen advice and financial records remain. These are selected-input or financial authority, not disposable upload buffers. There is no inbound selection_page FK in the current source census, and no delete/immutable trigger attachment on that table.

The page's original ordered ID JSON/first ordinal/count served upload replay (core1324:436–448). Actual append0570:148–165 checks page replay before checking appendability. After cleanup the missing page falls through to the existing terminal state refusal; it does not append or recalculate. Thus a late old page retry ceases to be a positive upload receipt, while the financially authoritative original membership/choice/revision remains unchanged. No append-owner, money-owner or archival protocol rewrite is needed for this deliberately closed temporary-input lifetime.

## Job eligibility: exact finite conjunction

For each bounded candidate row require all of:

1. `job.status='DONE'`; no current lease; never READY/LEASED/BLOCKED/FAILED. FAILED remains recoverable.
2. Its exact command has `status='COMPLETE'`, sealed and fully enrolled (`expected_member_count` known and equal to enrolled_member_count), and `private.bpay_next_enrollment_clock.last_enrolled_sequence >= command.agency_sequence` on its singleton id=1. This actual field is core1324:963 and service0560:23/26; use the retained database sequence, never a caller sequence.
3. No remaining queued work/enrollment authority can recreate or progress that job from the original command. Existing completed command/membership and all original request records stay retained.
4. No inbound edge from any of the seven actual RESTRICT references below.
5. No deletion of a referencing financial/accepted record merely to satisfy (4). Unknown scope/state/kind remains retained rather than guessed eligible.

| Retained reference to job | Source |
| --- | --- |
| case_allocation_state(job_id,command_id,candidate_id,module_epoch) | 0300:339 |
| preparation_expiry_worker.job_id | 0501:55 |
| preparation_expiry_worker.original_job_id | 0501:57 |
| work_collection.last_job_id | 1519:32 |
| destination_posted_leg.job_id | 2025:19 |
| run_work_query_pin_v2.prepare_job_id | 0044:15 |
| effective_work_cut.prepare_job_id, retained unused2153 schema | 2153:190 |

Keep command, command_member, run_command, all accepted financial requests, effect/outcome/hold/case/transfer/CSV originals and immutable instruction/advice records. These preserve original accepted financial meaning and command replay; the DONE job's cursor/lease/time fields are temporary execution metadata. Do not disable any guard, change a RESTRICT FK, cascade into another table, delete F25/schema records or recreate a job with new IDs. No immutable/delete attachment on job was found; new0352 lifecycle clock attaches only INSERT/UPDATE.

The existing public financial_job wrapper0280:150/311/415 strictly requires the job row. After an eligible deletion its late RPC yields the existing P0002 -> BPAY_NEXT_JOB_NOT_FOUND definite typed refusal. Preserve that honest absence; **do not return fake DONE or adapt an arbitrary missing lookup into financial success**. Original accepted command retry is a different retained boundary; resending a job/step or inventing a fresh business command is not an automatic recovery from deleted metadata.

## Supporting cleanup owner and transport

Use the named private cleanup plus, if necessary, one fixed service-only wrapper in the existing runtime transport. One small limit/cursor page, explicit NEXT/service admission, locked eligibility revalidation, bounded deletes and truthful visited/deleted/retained counts are sufficient. Schedule one awaited lane on the existing NEXT minute tick. No extra financial command kind, new worker protocol, archive table, general lifetime engine or cron/config change.

FKs still protect concurrent reference insertions. Keep deletion order limited to the two named temporary tables. A bounded scan may legitimately delete zero because rows are still referenced; the implementation must also have the genuine terminal-unreferenced branch above and must not substitute a permanently blocked stub.

## Lost response and delayed transport caveat

While open, preserve the original atomic job/step and same-identity retry behavior (§6.2:163); cleanup must never race a LEASED/unfinished obligation. After the explicit terminal lifetime closes, absence is operational metadata retirement and not evidence that an unknown financial mutation succeeded. The current queue special-cases only CLAIM/JOB_TERMINAL as ACK (financial-queue:190–192). JOB_NOT_FOUND otherwise follows failure/onFailure, and index's callback currently returns RETRY. This design does not claim a successful late-message ACK or application recovery journey. PF07/PF08 recovery handling and hosted queue acceptance remain NOT_RUN.

Do **not** broadly ACK every BPAY_NEXT_JOB_NOT_FOUND: job-step:333 maps any P0002, including a nested strict lookup of damaged authority, to that code. Such an ACK would hide a real integrity failure when the original job still exists. Preserve the present typed refusal and later exact recovery ownership; no wrapper change or archive is required for the financial safety of the proposed deletion.

## Reviewed source receipts

- DRP: `26ab75fe1fb072ca66ada1bcbcae77832eb8de6e96492c66a70ee3f0987c4425`.
- public job service0280: `6fdd30c7b53b4e2d18d048aca46fbaadc7733a3cd5edefe4773f11b50288334d`.
- actual page owner0570: `d19da6893ce30575e8e186d8ce56b82b76e9fb61160f0f560430c682c9955f46`.

No current source defect in the proposed narrow deletion criteria is identified here. The explicit positive-reply lifetime distinction and unrun late-transport caveat must be recorded with BD026; no immutable provenance or financial idempotency is waived.