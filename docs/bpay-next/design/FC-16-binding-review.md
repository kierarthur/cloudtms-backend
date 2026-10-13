# FC-16 binding / disposition source review

11 October 2026. Read-only review of the root-authored 11102026_1509_banking_pay_next_case_change_guard_v1.sql. **Final verdict: SOURCE PASS; all B01–B05 findings closed in the current saved source.** Final repeatable SHA-256: 087c3c5d3ae9cfaa94b66ea58b338358d0236f2b4953be5ee6adf7e3dfc60fd4. The initial review below used d2d9521ea65e8f17c8b88b46fdb46e6edb4325cc8de0b20000eec1c2c92ff966 and is retained as history. No product, database, test, fixture or financial action was performed by this reviewer.

## Initial findings (historical; closed below)

| ID | Finding | Smallest correction |
| --- | --- | --- |
| FC16-B01 | The assertion's generic capacity expression uses principal_approved minus recovered/written-off/paid-credit/active holds but omits funded principal. A funded Loan/Advance payout therefore appears to have free payout capacity again. It is not the purpose-specific native capacity calculation. | Restrict amount authority to a qualified existing amendment family, or use the exact native purpose-specific balance. Do not let this protection summary become a second allocation/money owner. WHOLE_CASE/FUTURE_ONLY protection and factual hold totals remain distinct from amount permission. |
| FC16-B02 | Discard/expiry disposition exclusions do not join the actual terminal job and command/original Prepare identity. They also wait for the entire parent to finish, so a successfully removed Candidate is still phantom protected while siblings complete. | Use the exact per-Candidate completed receipt, matching request/run/Candidate/original Prepare/current revisions and DONE DISCARDED or EXPIRED job. Parent CANCELLING/PARTIAL does not negate a genuinely completed successful member. BLOCKED or incomplete members stay protected. |
| FC16-B03 | Allocation exclusion checks state.status=COMPLETE and result revisions, but not the exact prepare_stage/net_stage, current NET projection identity/retirement and completion job. A generic COMPLETE flag is weaker than the native completion proof. | Follow the exact DRAFT/NET proof below. Positive result disposition must retain the native hold/capacity/event tuple; a status-only RELEASED/REALISED row must not override contradictory active protection. |
| FC16-B04 | The page cursor requires a string case_component_id, but the installed case_hold schema deliberately permits retained untyped holds with a null component. Such an ACTIVE hold is included by the helper; if it ends a page, the emitted next cursor cannot be consumed. | Preserve factual nullability or use the already unique native hold/member ID as the seek key without a redundant non-null component requirement. Do not drop an ACTIVE hold or invent a component identity. No business-row existence is claimed. |

## Exact current native facts

The accepted source is the current 1358 Create's selected CASE_COMPONENT run_choice_member plus the original sealed selected-case fallback. No producer/consumer of accepted_case_input exists in current repeatables beyond its dormant guard. Keep those old 2256 tables dormant. Do not build a second accepted-input protocol.

The current native case append owner (0454:390–413) rejects a second case-selection header for the same run/Candidate. Worker-present fallback must match that Worker's case_selection_revision. Missing Worker is a genuine accepted/enrolment-pending state, not proof of no protection. The current accepted CTE correctly prevents a legacy fallback from overriding an explicit run-choice member, including selected=false.

Use one VOLATILE private typed binding predicate for reader and assertion. ACTIVE holds always remain protection, independently of parent status or a purported completed disposition. A PENDING binding supplies identity/count with protected_amount=NULL and unreserved_capacity=NULL; absence of a hold alone never proves zero.

## Completed original Candidate release

Reuse the existing **private.bpay_next_worker_release_complete_v1(worker_id, cutoff)**, final 0559:6322–6345, rather than copying a partial proof. It already joins:

- current target preparation revision, captured preparation revision and case selection revision;
- matching original Prepare run-command/job and current active Prepare job;
- RELEASE request COMPLETE/stage COMPLETE with finished timestamp at/before cutoff;
- current-epoch DONE/CANCELLED job, exact Candidate/run identity;
- equal expected/released work and case-hold counts, zero active case holds;
- no realised effects, net request/projection, transfer or surviving work/case holds.

Current release acceptance is 0814:3590–3725. It holds run header before Candidate and invalidates readiness/review/counters before release. Current project completion is **0847:1280–1372**, not the older 0559 projection body: only its final projection checkpoint sets request COMPLETE/stage COMPLETE, job DONE/CANCELLED and the Office command COMPLETE.

A bare Worker CANCELLED, original job DONE/DISCARDED, or RELEASE request accepted/running is insufficient.

## Discard completion, including Worker missing

Current owner: **0847 enroll_preparation_discard_one_v1:4–115; discard_preparation_worker_page_v1:153–397**. The member row may have run_worker_id=NULL. Its final checkpoint sets DISCARDED, stage COMPLETE, outcome REMOVED/ALREADY_REMOVED and finished_at_utc, after exact released counts and projection exhaustion. The job then becomes DONE/DISCARDED.

Minimum identity closure is x.run_id/candidate_id → q.run_id/command_id → actual discard command/job. Join selection_candidate by run/Candidate/member_no. q.original_prepare_command_id must match the run's original Prepare binding. With a Worker, also match x.run_worker_id, preparation_revision=captured_preparation_revision, expected_execution_revision=current preparation_revision, selection_revision=current case_selection_revision, CANCELLED and no active holds. Use the existing release-complete proof when already_worker_released=true.

Without a Worker, x.run_worker_id must be null, native expected/released counts must be zero, and there must be no Worker for that run/Candidate. The original un-enrolled branch in 0847:36–53 closes the accepted descriptor's original PREPARE command as COMPLETE with zero enrolled/expected members and no original jobs/Workers. Do not demand an original job in that exact branch, or it will never clear.

The per-Candidate successful receipt can clear this Candidate while q is still CANCELLING or ultimately PARTIAL because another member is BLOCKED. Do not treat BLOCKED/stage COMPLETE or q PARTIAL alone as successful removal. The original finished aggregate is owned by finish_discard_member_v1 (0814); do not recompute parent counters in this reader.

## Expiry completion, including Worker missing

Current owner: **0910 expire_preparation_worker_page_v1:4–234**, with final projection/count check at 176–230. Join expiry member/request and its matching PREPARATION_EXPIRY command/job, exact run/Candidate/member identity and original Prepare binding. Require member EXPIRED/stage COMPLETE/finished timestamp, job DONE/EXPIRED, equal released/expected counts and no active holds. Existing Worker revisions must match; nullable Worker absence is supported by the native member shape.

The accepted expiry request or a passed deadline is not completion. One member's genuine completed expiry receipt can clear its binding before the other members finish. The parent is CANCELLED only after native completed/enrolled counts close; the reader must not fabricate that aggregate or a completed timestamp.

## Exact completed allocation / zero / NET

Current DRAFT owner: **0904:432–550**. Follow the current Worker's captured_preparation_revision and case_selection_revision to the exact selected instruction, result and allocation state. Require state pass DRAFT/projection_no=0/status COMPLETE/prepare_stage COMPLETE and its exact expected/processed/captured instruction counts. A completed result with allocated_source_ex_vat=0 is factual zero; missing result/state/Worker is unavailable, not zero.

Current PAYE NET owner: **0559 apply_case_paye_net_page_v1:2988–3344**, exact completion 3292–3323. If a PAYE NET request has entered and is not completed, retain pending protection; an old DRAFT zero or old projection does not clear it. Require net_request_revision=net_projection_revision>0, matching current request_no/command, NET state projection_no/current revisions/status COMPLETE/net_stage COMPLETE, exact projection_id to the non-retired native net_projection, and its DONE/PROJECTED job. For NET_DEDUCT use its NET result. Non-NET stages remain the immutable DRAFT result/hold; the NET owner deliberately does not create an additional NET result for those instructions.

Current code always requires NET for PAYE NET_DEDUCT. A zero DRAFT result before any NET request therefore remains conservatively pending; document this intentional boundary or use the native proven zero where the card requires it. Do not infer an amount from nominal or capacity_source_ex_vat: they are frozen nominal/ceiling, not allocation.

Positive result with no ACTIVE hold needs exact real disposition. The realisation owner **0410 post_case_transfer_member_v1:346–538** matches transfer/member/result/hold/capacity and original settlement/internal receipt, creates the exact FUNDED/RECOVERED/PAID event, then marks hold/capacity REALISED and debits active counters. Match allocation_result_id, instruction/run/Worker/Candidate/component, purpose/week and the original source/target tuple. RELEASED is valid only through the native completed release/expiry/discard/cancellation proof. ISSUED/SETTLED/COMPLETE flags alone are insufficient; unused previews never qualify.

## Native indexes / references

Current saved SQL provides:

- active case_hold (case_id,id) and (run_worker_id,id); unique active instruction and unique allocation_result_id;
- run_case_instruction unique (run_worker_id,preparation_revision,case_component_id), component/week index;
- case_allocation_result unique (state_id,instruction_id) plus (instruction_id,state_id);
- allocation_state unique (Worker,selection revision,preparation revision,pass,projection no);
- worker action unique (Worker,target preparation revision), original Prepare/Worker index;
- discard/expiry request unique run_id; member primary (command_id,Candidate), unique job/member number and Worker/command index;
- case_selection_item (component,run) and selected (run,Candidate,revision,selection_no);
- run_choice_member currently only run/kind/member and run/Candidate/kind/member keys: a component-first protected-binding seek may require one narrow partial native index. Do not compensate with a history scan.

The VOLATILE set-returning helper is not a guaranteed inlined page query: the outer LIMIT bounds returned rows but does not itself prove bounded physical work. Root must verify the exact indexed predicate/seek shape and ensure accepted history is excluded by genuine dispositions. Exact summary counts may inspect the indexed native case family; no browser aggregation or unbounded response array is allowed.

Existing pay_run fields provide actor_user_id, created_at_utc, pay_date and confirmed timestamp. Existing preparation_progress_v1 (0814:2846–2889) obtains actual public.tms_users.display_name by actor ID; that stored display name may name the preparer. There is no human NEXT Draft reference field established here: use factual channel/pay date/actor and native IDs for machine identity. Never show a fabricated Draft number/name.

## Review boundary

These are source findings and suggested native predicates. Root owns product changes, fresh metadata/index confirmation, compile and contract checks. No DB/business observation, runtime acceptance, screenshot approval or whole FC-16 completion is claimed. The final source closure follows; the earlier findings above describe the superseded initial bytes.

## Final source closure

Re-read the complete corrected repeatable, the current native disposition owners and the narrow migration, then re-read the final outer ORDER BY and hash. No essential remaining source defect was found in this boundary.

| Finding | Current saved correction / evidence | Result |
| --- | --- | --- |
| B01 purpose-specific capacity | Fifth operand is a closed PAYOUT/RECOVERY purpose. Loan/Advance requires an explicit purpose for amount authority; its payout subtracts funded principal, while recovery uses funded principal. CREDIT defaults to payout; debt defaults to recovery. Incompatible purpose refuses. Unknown mixed purpose does not invent capacity. Actual allocated hold totals remain separate; nullable pending authority refuses a new amount. | PASS |
| B02 exact completed disposition | Discard and expiry join the matching native DONE terminal job, command/epoch, checkpoint and original run-command PREPARE identity. Successful per-Candidate rows clear while the parent is still CANCELLING/PARTIAL; incomplete/BLOCKED rows do not. Worker missing remains supported. The original volatile release-complete helper is reused. The original completed SIMPLE_CANCEL path is qualified by its typed cancelled binding, native DONE/CANCELLED job/command, checkpoint, count/revision closure and zero effects/holds. | PASS |
| B03 exact allocation / realised proof | DRAFT requires completed prepare stage and exact captured/processed/expected counts. NET requires completed net stage with no issue, the exact current request/projection revisions, a non-retired matching projection and DONE/PROJECTED request job. Positive disposal requires matching REALISED hold, capacity-use and original financial event/source/target tuple; RELEASED alone cannot clear. ACTIVE holds remain an independent protected branch. | PASS |
| B04 nullable component pagination | Seek key is now binding_kind/run_id/native_id. An untyped retained ACTIVE hold can retain a factual null component without emitting an unusable cursor. Internal seek/limit precedes the outer actor join; strict cursor shape, 1–100 row limit and 120,000-byte row/page bound remain. | PASS |
| B05 outer joined order | The final FOR query explicitly orders by b.binding_kind,b.run_id,b.native_id after its tms_users join (lines 208–212), matching the internal seek key. The possible join reorder no longer affects cursor advancement. | PASS |

The existing write-off owner is carried into 1509 with one guard call after its original case/component locks and before any event/money mutation. It passes RECOVERY and distinguishes WHOLE_CASE from an exact unreserved amount. The new pending/protected exceptions become the existing retained BLOCKED result, with no recursive Candidate release or run lock inversion. Original non-null request.protected_amount remains the allocated active-hold amount; BPAY_NEXT_WRITE_OFF_INPUT_PENDING distinguishes the unknown accepted input instead of presenting that field as the complete pending total.

The new immutable migration adds only the selected CASE_COMPONENT reverse lookup index and the one closed write-off issue-code member. It preserves the five original issue codes and performs no business-row update. Four exact current final registry signatures point to 1509: case_protected_bindings(uuid,jsonb,integer), case_reserved_slices(uuid,jsonb,integer), case_change_assert(uuid,bigint,text,numeric,text), and apply_write_off(uuid,uuid,bigint).

Dormant accepted_case_input tables remain unused. There is no new accepted producer, financial command/job kind, allocation plan, public reader, screen or framework. Actual preparer display_name is read from the existing tms_users relation. The native ID remains the machine reference; no human Draft number is fabricated. Pending/unknown summaries remain nullable and never silently become £0.

| Final reviewed artifact | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_1509_banking_pay_next_case_change_guard_v1.sql | 087c3c5d3ae9cfaa94b66ea58b338358d0236f2b4953be5ee6adf7e3dfc60fd4 |
| supabase/migrations/11102026_1509_banking_pay_next_case_protection_lookup_indexes.sql | f9fe0d603447db42aff4f3837e5896fa6e1f5ab24e67d67c3f1ceb262dfca027 |

This PASS is source-only. Root's compile/source/metadata checks are separate evidence and were not executed by this reviewer. Joined business scenarios and financial/runtime acceptance remain **NOT_RUN**; FC-16 has no direct A4 screen deliverable.
