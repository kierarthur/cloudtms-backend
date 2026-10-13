# WB-06 position boundary investigation

Read-only10 October2026. Read full card02:247–279 and every checklist object D01–D23. Read complete1430 publish/claim/apply/coarse bodies, current1332 WAIT/pending/full-refresh seams, current0280 financial service CLAIM dispatch,0560 enrollment/wake lanes and actual Worker background/queue/index seams. Used saved final-definer registry to identify current physical owners and a source-only native position-writer census. No product edits, SQL, tests, fixtures, runtime, network or secrets.

## Minimal1430 successor

The master orders1430 WB06→CM10; it is now the named owner. Preserve the full native bodies, signatures, security settings and ACLs. No new command, public trigger, producer protocol, phase or parked F25 dependency is needed for these connections.

1. **Publication, before first WORK lock and before command clock.** After existing pure input/NEXT validation, read the immutable WORK Candidate identity without a lock, acquire that Candidate's existing worker_control lock, then acquire the existing exact WORK lock. Preserve immutable revision/physical Timesheet/predecessor/replay checks and the exact existing replay return. After sealing the genuine revision and setting current_revision/APPROVED, mark the exact maintained group/items Processing/nonselectable in this transaction, with a coherent projection revision/dirty transition. Acquire required metadata/projection locks here, before `receive_command_v1`. The existing comment that command clock is last in the upstream business-lock chain remains true. Do not invoke a projection owner after the clock if it acquires a previously unheld Candidate/Case/metadata/revision lock.
2. **Intermediate NEW/REMOVED pages.** Keep the exact published revision, applied predecessor, lease/epoch/expected phase+cursor guards and bounded page writes. Do not replace complete payable contributions using partly updated native positions. Publication invalidation must remain in force through both passes. If intermediate invalidation needs refreshing its financial version, update only the same Processing state; do not clear dirty/restore Ready or consume the preserved withdrawal WAIT marker.
3. **Final REMOVED completion.** Preserve APPLIED/DONE receipt, exact applied pointer, job terminalization and command completion. After the existing financial_view_revision increment, refresh the exact WORK and its genuine linked collection Cases in that same transaction. `refresh_many(array[work_id],null)` supplies sorted Candidate/WORK/Case/metadata prelocks and native101 sentinel before singleton projection locks. Only refresh owners call apply_delta. Exact pointer/financial version, group contributions, Case facts and rollups commit together.
4. **Replay/order.** Keep the current DONE/APPLIED terminal readback before new work/projection reads/writes; a lost final reply must not rerun refresh or adopt a lease. Keep mismatched expected cursor as readback, not repeated finance. Do not require publication revision=current revision to complete an earlier legitimate job: its applied predecessor determines authority. If a newer current revision exists, the earlier job may finish its own receipt but the current WORK must remain Processing, not Ready. No old page may mark the newer revision applied.

The final refresh must occur after financial_view_revision is bumped; calling it inside the earlier final-REMOVED block would otherwise store a stale financial version. Linked Case locks precede metadata/singleton revision locks; avoid refreshing WORK first and discovering additional Case locks afterward. The same Candidate control serializes publication, apply and ordinary/Source withdrawal.

## Preserve WB02's actual current contract

Current1332 reads the stored-before AWAITING_REAUTHORISATION marker and computes after-state once. Its re-authorised/current≠applied branch now increments workbench_revision/dirty_group_count once per false→true dirty transition, marks group/items Processing/nonselectable, preserves the WAIT reason and returns. Full refresh later decrements dirty count once and sets dirty=false. Keep this seam: publication is not the effective re-authorisation boundary; final application is.

At the final boundary1332 refresh_work detects a real WAIT transition, prelocks exact native collection Cases, replaces the complete WORK contribution and refreshes those Cases. Its acceptance state still comes from Rule-B selected expected revision/Candidate snapshot/query snapshot plus existing pin_v2, not2153/resolver/producer protocols. Withdrawn work reads applied facts; no zero revision/position job/debt is created by withdrawal. Later authorised changes reconcile native collection principal through the existing money owner.

For normal pending work, a WB06 invalidation must retain the last complete contribution or unavailable evidence as appropriate, with explicit dirty/Processing and no selectability. Do not generalize this into a whole Candidate/history rebuild. Dirty/adoption count changes must describe actual stored dirty groups, including first publication with no prior group. WB44 remains the full seed/concurrent-producer adoption owner; direct hooks do not enable Workbench reads by themselves.

## Existing authority and bounded execution

- `publish_staged_revision_v1(uuid,uuid,uuid)` already has immutable same-command replay, physical current Timesheet/version/booking checks, monotonic revision, predecessor, one POSITION_APPLY command/member/publication and sealed count. Keep all of these.
- `claim_position_job_v1(uuid,integer)` already locks Candidate→job, requires module epoch/POSITION_APPLY/READY or expired LEASED and refuses any earlier same-Candidate non-DONE job before allocating a new owner epoch/nonce. Do not weaken the predecessor barrier.
- `apply_position_page_v1(uuid,uuid,bigint,text,text,integer)` has NEW approved-line and REMOVED native-position keyset passes, exact line-count assertion, original source-basis preservation with realised/held target money, and native reconcile_work_collection per changed component. The current private input permits256/default128, but0280 service and Worker requests/replies are bounded100. A WB06 successor can narrow the named private page default/max to100 while preserving cursors and receipts; no one-shot large-revision apply is justified.
- `work_payable_state_v1(uuid)` remains the existing SQL STABLE coarse NOT_APPROVED/APPLY_PENDING/READY contract. Do not replace it with a fake full classifier. Processing display is WB12/WB15's later renderer ownership.
- Keep current D03/D04 capture/hold final1236 bodies, selected-segment authority and native conservation; D05 pin_selected/D22 claim_prepare final0151 Rule-B authority remain unchanged. No late current-pointer/channel substitution, no saved-quote replacement, no ratio-derived residual or new Case allocation owner.
- More than100 total linked owners must refuse/defer without presenting a truncated final projection as complete. Existing refresh_many now bounds the union to101 before materializing its array. WB44 must close genuinely larger indexed fanout; a100-ID page is not proof that all linked Cases were refreshed. Preserve this explicit later dependency rather than guessing completeness or adding a protocol here.

## Queue/service reuse and actual retry limit

Registry current public owners are enroll_service/wake_page0560 and financial_job0280. Enroll_service performs one genuine metadata enrollment. Wake_page has explicit <=100 READY/BLOCKED and LEASE_EXPIRED index lanes, rotates only queue wake time and leaves claims to native owners. Position financial STEP is the exact NEW/REMOVED phase/cursor/limit request. Financial service reclassifies claim contention into terminal/busy/predecessor/retry outcomes without adopting a stored winner nonce.

Current index uses real runtime/job/expiry factories and BANKING_PAY_CONTINUATION_QUEUE, fresh NEXT owner admission, one-message contract, complete tail retry and awaited background dispatch. The minute Workbench lane fresh-checks NEXT and calls runBpayNextRecoveryTick. Background enroll persists job/next ENROLL wakes before ACK; recovery gives ENROLL, READY, LEASE_EXPIRED, EXPIRY and CLEANUP independent bounded turns. Financial queue awaits a validated continuation send before ACK; unknown send/step preserves same-step/lease-expiry recovery. Retired legacy envelopes are qualified/retained by existing F13 rather than executing an old Workbench drain under NEXT. Opening Office does not rebuild positions.

**D06/D08 qualification:** current claim rejects status FAILED, and current due lanes do not discover FAILED. Setting only available_at on FAILED would not restore claimability. No current first-party NEXT failure setter was found in the narrowly searched active NEXT source; Worker failure path retains/retries delivery and does not write FAILED. Do not claim an installed definite FAILED→READY reset owner exists, and do not invent an unconditional reset of all FAILED jobs. A confirmed failure/reset owner, if required by the actual branch, must reset the exact native supported job to claimable state and due wake, preserving phase/cursor/command/epoch; unknown outcomes first reconcile existing receipt. Service transport/runtime reuse alone is not full queued data/application acceptance.

## Current final native position writers

Source-only census of final registry authorities, excluding historical/parked duplicate definitions, found these eight direct INSERT/UPDATE owners. Each completion note must identify its existing refresh or precise later W11 hook; WB06 does not broaden to rewrite them all.

| Current function | Final physical | Projection ownership |
| --- | --- | --- |
| apply_position_page_v1 |1430|WB06 final boundary described above |
| source_disposition_seed_v1 |1309|WB08 already calls exact refresh after position update, NEXT-gated |
| hold_fresh_position_page_v1 |1236|Retain native capture/hold; later named reservation/current projection hook |
| post_simple_outcome_page_v1 |2018|Retain real outcome writer; later named outcome/W11 hook |
| apply_work_collection_recovery_v1 |0316|Retain actual collection recovery; later named money/W11 hook |
| cancel_simple_worker_page_v1 |0260|Retain actual cancellation; later named cancellation/W11 hook |
| cancel_case_worker_page_v1 |0460|Retain actual Case cancellation; later named cancellation/W11 hook |
| expire_preparation_worker_page_v1 |0510|Retain actual expiry release; later named expiry/W11 hook |

This is a saved-source writer census, not database catalog execution or a claim that all later hooks are built. Source origin remains the native origin reader; certified zero, ordinary negative recovery and active reservations retain real ownership.

## Source hashes and acceptance

-1430 `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa`.
-1332 `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391`.
-0280 `6fdd30c7b53b4e2d18d048aca46fbaadc7733a3cd5edefe4773f11b50288334d`.
-0560 `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa`.
- Worker background `bf485315e29a6180337ff04ee38d97fa4e9afb4f35dc4c69294f69ccc151cf14`.
- Worker job-step `53e2f466929e6640de7be4823aebf4979b81510c3de38931d198897e2de2c810`.
- Worker financial-queue `59bb0fc90f37d3e211f9bcdbf9a17ee3f1ecbfbd6e6d3b94197cf2369ba38e20`.

Required allowed verification remains root's local load/db:check/source/seal/coupling and source syntax; no fixtures/rehearsal or joined app scenario was run here. Real multi-book totals, decreases/increases, lost wakes, Candidate isolation and screen Processing remain recorded later acceptance, not source-only PASS.
