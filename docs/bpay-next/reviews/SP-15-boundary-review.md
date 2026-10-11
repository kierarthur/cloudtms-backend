# SP-15 SQL boundary review

11 October 2026. **SOURCE PASS for the current2337 SQL, twelve exact registry closures and the Candidate/security boundary.** This is a read-only source verdict. No SQL, business call, test, fixture, render, hosted action, deployment or product change was performed. Worker/Office/A4 and full joined acceptance are separate; no SP15 ITEM/App-tested completion is claimed.

Reviewed the full SP15 card05:515–542, all checklist rows3331–3355, the named chain/dependencies, owner start-sheet overrides, current SP12 admission/frozen census, original Rule-B/OR47 scope and `design/SP-15-current-investigation.md`. The root's saved `.codex-local/SP15-parents.json` supplies eleven exact current originals. Fresh static full-statement comparison showed every original body/configuration/signature unchanged except the explicit post-write hooks; every recorded parent file SHA matches. The original money/publication/replay/permission/admission statements remain in those complete successors.

## Findings and closure

| Finding | Current source closure |
| --- | --- |
| Local completion originally skipped its tail whenever a manual review existed, although only APPROVE/AMEND/WITHDRAW/RECONCILE invoke nested RESOLVE. Other actual completed control/receipt kinds could miss refresh. | **CLOSED:**2337:3292 now refreshes when no review exists **or** request_kind is outside those actual resolver branches. The nested0428 hook covers the four resolving branches; original PROTECTED_PAY and pending behavior remain unchanged. |
| Identity adapter needed the named Source Work scope. | **CLOSED:**2337:19 adds w.work_kind='SOURCE' alongside exact raw booking/Candidate/Contract/native week. |
| Protected NEXT already owns its exact staged Work; another event/root resolution is unnecessary after its original publication clock. | **CLOSED:**2337:2858–2860 uses the original NEXT-gated **v_stage.work_id** directly after receipt/family/run completion. No new public root/family locks or second publisher. |

No remaining essential defect was identified in this SQL scope. Per-event refresh rather than global dedup is a legitimate narrow choice: each call is bounded to the actual affected Work, after complete actual business writes, under the existing admitted Source transaction. Repeated replacement is not a money write or exactly-once claim. The card does not require a new unbounded dedup inventory, Source continuation/protocol or request limit.

## Native identity and bounded refresh

The actual new helper is **private.bpay_next_source_query_refresh_v1(uuid,uuid) RETURNS void**, VOLATILE SECURITY DEFINER, search_path=pg_catalog,private at2337:4–29. VOLATILE is correct for this combined identity/refresh adapter because it calls the existing mutating projection owner. The earlier two-helper STABLE mapper recommendation is not a requirement to add another function.

The helper's NEXT check10 returns without changes otherwise. It starts with event PK and Contract PK, verifies native event Candidate/Client through the Contract join13, selects primary nonadjustment Contract Week for the event's **own work_date**14–15, reads that actual primary physical Timesheet16–18, and matches Work by exact raw booking plus Source kind/Candidate/Contract/week19–20. It never uses Banking source_event_id as a durable event, equates original_timesheet_id with a rotated current root, chooses latest history or invents another Work. Native zero match is a no-op; more than one matching Work refuses with the exact existing adapter's named ambiguity exception23–25 rather than picking one. It collects at most two distinct Work IDs to detect ambiguity and calls the original refresh for the sole exact ID27. It emits no money or producer/certificate envelope.

The native primary week lookup has existing uq_contract_week(contract_id,week_ending_date,additional_seq) and idx_cw_contract_we; Work booking_id is unique in1324:127–128. No new index/schema/table/command/public route is present. Root identity qualification deliberately does **not** require approved/is_current/unrevoked query eligibility or pay_query_scope_v1 success: unchanged classifier2223 decides availability and accepted/H/R/WAIT behavior. The mapper is not used as financial admission or proof of query clearance.

The existing refresh wrapper remains1438:318–351, including Candidate→Work and bounded related-Case/ORIGIN behavior. A scalar hook has one actual Work, within WB08's native envelope. Neither wrapper nor current2223 classifier/PIN/OR47/financial formulas are replaced by2337. The current published Work already has its Candidate control; pre-publication absence is no-op, not seeding or alternate financial authority.

## All eleven successor seams

| Complete original owner | Current2337 hook |
| --- | --- |
| public.weekly_source_manual_review_open_v1(jsonb) |128, only actual new OPEN after original immutable command result; original same-key replay exits before hook/admission |
| public.weekly_source_manual_review_resolve_v1(jsonb) |318, after native resolution/evidence and immutable result |
| private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) |390, only actual genuinely later contained review resolution; both original Final/CorrectFinal callers are reused |
| public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) |487, after exact incident/resolution events |
| public.weekly_source_manager_review_respond_atomic_v1(jsonb) |788, after actual accepted response/control facts; original Candidate-correct exception retained |
| public.weekly_source_candidate_response_submit_atomic_v1(jsonb) |1106, after original accepted immutable response/material branch |
| public.weekly_source_query_sync_atomic_v1(jsonb) |1524 changed-present comparison;1612 omitted-event resolution uses that incident's actual current comparison.contract_id, not the previous v_contract_id |
| private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone) |2269 changed comparison and2317 actual omitted-event resolution; original native membership Contract and event are retained |
| public.weekly_exceptional_pay_publish_next_v1(jsonb) |2859 direct v_stage.work_id after original completed receipt |
| public.weekly_exceptional_pay_complete_local_v1(jsonb) |3293 remaining material completion branch; actual nested resolver supplies its own hook |
| public.weekly_exceptional_pay_wait_atomic_v1(jsonb) |3581 after original WAIT/family/run facts; completed no-write replay still exits beforehand |

Each public original Source admission call remains before its first original business locks: OPEN64, RESOLVE180, later-Final341, accept439, manager597, Candidate911, Sync1292, protected NEXT2460, Local2950, WAIT3380 (only fresh actual write). The private Candidate comparator inherits the unchanged admitted0853 app/self/direct-materialise owners and existing prefinal publication outer; no first key is acquired deep in this helper.

Original participating Source transactions take the global **try** admission key before Source locks, and the existing refresh takes Candidate→Work→Case. A competing Draft/Source caller cannot wait indefinitely on that admission key while holding Candidate: its original55P03 rollback semantics remain. This source review does not substitute for future both-order contention/timing/liveness tests. No foreign run/header/public-root locking or inline release is added.

## Candidate and security STOP-CHECK

The owner start sheet §1/§2 preapproves card-required Candidate changes with logs. The current2337 Candidate response and private comparator bodies differ only by explicit refresh calls; their service/session/environment/actor/generation/membership/comparison, semantic request hash/set, exact replay/collision, original signature lineage and original Source/TSFIN/invoice authority are unchanged. The current0853 public Candidate outer definitions remain untouched.

All five protected-boundary-lock.json file SHA values were freshly checked against saved bytes and matched. The lock itself remains SHA60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c. This is static saved-file evidence, not an executed isolation verifier or release. No frozen byte, signature, ownership or browser permission is weakened.

All twelve registry entries now select2337. The eleven original public/private function signatures/configurations are retained; the sole new helper is private and owner-only. Tail3592–3624 explicitly owns postgres, revokes PUBLIC/anon/authenticated/service_role for the private helper/comparator/Final clearer, and restores only the original service_role EXECUTE for existing public owners. Revoking then restoring service_role on public successors retains the original final permission; no new browser service or public signature is introduced. Existing notify pgrst packaging is retained.

F25 producer/version/latch/certificate/resolver and original immutable Rule-B cut remain parked/untouched. There is no capture-time query rewrite, blanket Candidate hold, query-driven recovery pause, post-cut repricing or new financial owner. SP09's future tax successor must merge the complete current2337 protected publication parent.

## Reviewed current hashes

| File | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql | 058e400f131b5f82d53f3d3aaf299b44d37d305c6d2413380da4fc3c454625d0 |
| .codex-local/SP15-parents.json | 6342ab9d8d0da958379217f66cfd11b55bdaf8008aa163f1336ca17429bc3b67 |
| docs/bpay-next/final-definer-closures.json | 6d804afb96533069147fe3b4ff3cb2a6b2a50d5157d94c85153d8d2676c66efa |
| supabase/release/protected-boundary-lock.json | 60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c |

Normal compile/db integrity/gates/contract checks, actual deployed versions and joined acceptance are root-owned and not executed by this reviewer. The current verdict is SOURCE PASS only.

## Final equivalent native-week range amendment

Fresh saved2337 bytes retain **SOURCE PASS**. The helper's sole final logical delta at line15 is `cw.week_ending_date BETWEEN e.work_date AND e.work_date+6`, equivalent to the previously reviewed `e.work_date BETWEEN cw.week_ending_date-6 AND cw.week_ending_date` for these native dates. This expresses the existing Contract/Week index range directly; identity, cardinality and all eleven hook owners remain unchanged. Current SQL SHA-256: **7a5b4b56a424b2a07a8a54c895c4841ce6e0c2cbb0e9a80d06c27842fde5c96a**, superseding the earlier SQL hash above. Static source review only; no SQL, business calls or tests executed.

Raw-byte inverse comparison independently confirmed exactly one predicate replacement reconstructs the complete previously reviewed file SHA058e400f131b5f82d53f3d3aaf299b44d37d305c6d2413380da4fc3c454625d0. Thus every other byte, including all eleven complete hook definitions and ACL tail, is retained.

