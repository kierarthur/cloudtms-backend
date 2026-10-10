# WB-44 continuation source review

Verdict: **SOURCE PASS — F1–F6 are closed at the final 717-line snapshot below; no open finding in the reviewed SQL scope.** Historical findings and intermediate hashes are retained for provenance. Worker integration and app/timing acceptance are not certified by this read-only source review.

## Reviewed snapshot

| Source | Raw SHA-256 |
| --- | --- |
| `supabase/migrations/10102026_1429_banking_pay_next_workbench_current_continuation.sql` | `03e28413e6f1c692ffe8aadcf7de8fe1b893b8718db0412dcdd7dcf9806726fb` |
| `supabase/repeatable/10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` (641 lines) | `870833b253eb3aee66dc2146069cef8cbfb0424274f02aa5ff2f01fc6a3adc53` |
| `supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` | `4380deff20ea501ed2511f8392648c12550946f23aefc455c1fbd80028476b47` |
| `supabase/repeatable/10102026_1438_banking_pay_next_hold_workbench_projection_v1.sql` | `0dc24d3eeab2606ea5ee1988874e5c351f9a048b8056979a944d97b5fcc2890c` |

The review uses the full WB44 card, all checklist rows, R2 section 5 and BD062's separate metadata-task design. It does not promote the earlier suggested financial command kind: the draft adds no financial command kind, agency sequence or enrollment owner. No parked F25 producer protocol, finance recalculator or future finance owner is required by these findings.

## Historical findings (closed by the final review below)

### F5 — P1: expiry watermark UPDATE is rejected by the native Snooze guard

The new DUE_PAGE updates `private.bpay_next_snooze.workbench_expiry_version` at repeatable lines 536–538. The installed `private.bpay_next_snooze_metadata_guard_v1()` in `10102026_1100_banking_pay_next_snooze_v1.sql` runs BEFORE every Snooze UPDATE (436–437). It selects the original `last_request_id` command and requires RECEIVED at 343–347, then requires `new.version=old.version+1` and a strictly advancing original agency control sequence at 352. The watermark-only UPDATE has none of those properties; an ordinary completed native Snooze command is COMPLETE. It therefore refuses before the expiry receipt can commit. The deferred native receipt guard also remains attached, so bypassing just the first test would not make this an independent Workbench metadata mutation.

Keep native Snooze rows/events/request/command receipts and both guards unchanged. The minimal support is a separate Workbench-owned expiry receipt for the actual `(snooze_id, version)` identity, using the real due date and bounded indexed traversal. Acknowledge only exact visited versions in the same transaction as their queued origin barrier. This is metadata continuation, not a Snooze economic mutation. The original native due index exists; the new private continuation owns its watermark and finite progress independently.

### F6 — P1: a new seed can adopt stale retained Candidate metadata after inactive-owner edits

Candidate and Umbrella metadata callbacks return without updating the Workbench cache under LEGACY/DISABLED (1429 repeatable 157 and 178). A retained metadata row can therefore have `continuation_pending=false` and a nonnull `target_pay_channel`, while its actual Candidate name/reference/channel/Umbrella controls change outside NEXT. On the next owner epoch, SEED_METADATA only visits rows already pending (318–323); `ensure_metadata` only initialises retained mapping where `target_pay_channel is null` (26–34). Neither path compares/adopts the actual changed metadata for such a retained row. Work/group seed refresh can consume the stale cache, then SEED_DRAIN can declare CURRENT.

The seed must boundedly adopt actual typed Candidate metadata for its current seeded origins/retained current projection rows. Compare and replace the exact name/reference/channel/Umbrella control values without recalculating rates, NET, earnings or immutable history. Preserve the warning semantics: actual missing bank details may produce MISSING; lack of retained historical bank evidence must not manufacture CHANGED or clear a retained warning. Reuse the same Candidate barrier and metadata tasks for genuinely changed cache values. This does not require any financial command or record-version protocol.

## Closed initial findings

| Finding | Current source closure |
| --- | --- |
| F1: zero-delta metadata replacement left stored Candidate summary display fields stale | New METADATA_SUMMARY discovery has a stable natural-key fence and matching index. The phase at 428–438 updates actual name/reference/bank warning/metadata version/projection revision, including ALL, without changing any count or monetary measure. |
| F2: SEED_DRAIN checked counts without the revision lock | Lines 461–466 acquire revision FOR UPDATE, recheck exact pending/dirty counters, finish the Seed and adopt CURRENT atomically. A competing enqueue uses the same revision row. |
| F3: retained pre-WB44 metadata could omit the actual missing-bank warning | Mapping initialisation at 26–34 sets MISSING when the effective native hash is absent and otherwise preserves the existing warning. It does not invent a change or clear the warning. F6 is the separate later-epoch cache-adoption issue. |
| F4: public inactive-owner lookup exposed P0002 | Lines 501–502 use a nullable NEXT lookup and explicitly refuse with `55000/BPAY_NEXT_MODULE_NOT_ACTIVE`. |

## Source checks that pass within this scope

- The schema has finite task types/phases, natural task identity, positive generation/module/owner epochs, explicit READY/LEASED/DONE/ERROR lifecycle, exact lease-shape checks and a DONE/phase equivalence. Private table RLS is forced and public/browser/service direct table privileges are revoked; the owner policy uses CURRENT_USER for the audited release-owner mapping.
- Seed uses indexed current APPROVED/WITHDRAWN Work, OPEN/PAUSED Case, current group and pending metadata pages, never an application-open agency sweep or immutable payment-history scan. Upper fences/cursors persist. Bootstrap returns after locking Seed; it does not then acquire Candidate controls in inverse order.
- Step discovery is plain and finite. Candidate controls are preheld in sorted order, then actual Work/Case rows, then the task. Nonce, owner epoch, generation, step, phase/cursor and lease expiry are rechecked after waits before the discovered page can advance. Native Candidate financial-view and origin revision/applied/state/wait checkpoints reset obsolete origin generations. Metadata-version changes reset metadata work.
- Task scheduling coalesces a still-current generation. A restart advances generation/owner epoch, clears the old lease and restarts the fenced cursor. Only a successful claim reveals a fresh nonce; stale/lost-reply inputs cannot advance another owner's lease. Completion decrements pending task counts once and clears the Candidate dirty barrier only after the last Candidate task ends.
- The inner page exception block rolls back page mutations before persisting ERROR, a bounded error code and delayed recovery. It does not report a completed projection for a failed page. Seed ERROR is explicit.
- The 1438 core body preserves the installed 1410 Work classifier except for moving its formerly unbounded linked-Case prehold/refresh loops to the continuation. `refresh_many` validates finite real IDs. A large linked-Case fanout queues a fenced origin task; ORIGIN_CASE walks the actual native Case IDs and ORIGIN_WORK runs only after the Case cursor finishes. There is no first-100 success, guessed Case or per-shift payment owner.
- The 1438 hold body compares with installed 1236 as the original complete function plus a bounded visited-Work ID collection and final `refresh_many` call after the original financial-view update. Valuation, immutable identity, amounts, holds and original money mutations are unchanged. The hook is a current projection refresh in the explicitly named hold seam, not a future hold-economic rewrite.
- DUE_PAGE discovers actual expired native Snoozes and prepared FUTURE_WEEK rows from real date indexes, bounds the Work IDs, preholds their Candidate controls/Work rows and marks their origin barrier. It performs no expiry financial action. F5 must be closed for its expiry watermark to work.
- The public wrapper is service-role-only, limits its argument envelope to 1024 bytes, uses a finite action set and private helpers, and grants only service_role EXECUTE. No browser-readable authority or new public financial trigger is added.

No SQL, database, tests, runtime, fixtures, app, browser, provider, payment, network, deployment or Git mutation was executed for this review. Installed behavior/performance, exact source-gate results and later Worker integration are not inferred from source inspection.

## Fresh F5/F6 correction review (708-line snapshot)

Reviewed migration SHA-256 `c89b160ebb66275b9f60a6e969b7ab15a961750d56b57d95447ed4d5c53d5978` and repeatable SHA-256 `6a813b2ebfa789cadf23a8570941e6ec03f1b33fd0fd5e18397a52ccda3149f5`. Earlier line numbers and hashes above remain historical review evidence.

**F5 closed.** The migration no longer alters native Snooze or adds a mutable field there. Separate `private.bpay_next_workbench_snooze_due` retains the actual Snooze ID/version, finite exclusive Work/Case identity, date and pending state with a matching due index. Its private AFTER callback copies actual native version/date/active only, preserving native guards and event/command receipts. Seed discovers at most the supplied 1–100 real active Snooze IDs below its retained fence, preholds their Candidate/Work/Case scope, then adopts actual native rows. The scope exclusivity matches native 1100 Snooze checks 27–28: CASE has Case ID and no Work; non-CASE has Work and no Case. DUE_PAGE bounds the union of actual Work/Case natural keys from due receipts and FUTURE_WEEK items, preholds sorted Candidate controls, Work rows, Case rows and the exact due receipt rows; queues Work origin/Processing or refreshes the actual Case. It consumes only the actual version it visited. There is no native Snooze update, economic expiry action, invented related Case or callback acquiring an unrelated Candidate. Original Snooze owners already hold the corresponding Candidate and Work/Case before the native row write, so the receipt callback and due path do not introduce a demonstrated inverse lock cycle.

**F6 name/reference/control cache adoption closed.** Every Seed page compares its actual Candidate metadata against the retained cache under the Candidate barrier, invoking metadata_changed only for actual differences. SEED_METADATA is now a fenced PK page over retained Workbench metadata cache, not all public Candidates or history, and schedules summary adoption even for zero economic delta. The actual control difference is carried separately; unknown past bank identity cannot produce CHANGED.

**F6 remaining edge (P1).** A retained metadata row can have nonnull target mapping and `bank_warning_code=NULL`; bank details can then be removed while NEXT is inactive without a name/reference/channel/Umbrella control change. The Seed tuple comparison at repeatable 412–418 does not inspect the actual effective missing bank hash. ensure_metadata at 43–51 only refreshes the MISSING warning when the target mapping is NULL. That row therefore passes the new Seed comparison and its summary can be adopted with warning NULL. Add the actual missing effective hash / non-MISSING cached warning as an additional reason to call metadata_changed(false, actual_control_difference) in this same bounded loop. Preserve an existing warning when the native hash is present; do not infer CHANGED, clear a warning or invent historical bank evidence.

The continuation and hold scope checks remain as recorded above. No SQL/runtime/test execution was performed.

## Final source closure — F1–F6 PASS

| Final reviewed source | Raw SHA-256 |
| --- | --- |
| `10102026_1429_banking_pay_next_workbench_current_continuation.sql` migration | `c89b160ebb66275b9f60a6e969b7ab15a961750d56b57d95447ed4d5c53d5978` |
| `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` (717 lines) | `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd` |
| `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` | `b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5` |
| `10102026_1438_banking_pay_next_hold_workbench_projection_v1.sql` | `0dc24d3eeab2606ea5ee1988874e5c351f9a048b8056979a944d97b5fcc2890c` |

F6's final edge is closed at 426–427: the bounded Seed loop also compares actual missing effective bank hash against a cached non-MISSING warning. It calls the existing metadata replacement with bank_changed=false and the actual typed control difference. A present hash does not infer CHANGED or clear a retained warning. Together with the actual name/reference/control comparison and all-retained-cache summary adoption, inactive-owner edits cannot pass this reviewed Seed path using stale cached values.

The private native Snooze AFTER callback now also queues the real Work origin barrier or refreshes the real Case at 17–23, in the original native setter transaction. The actual original setter preholds Candidate control then Work/Case (1100 owner 209–213 and 255), so the callback reuses the same scope and does not acquire a new cross-Candidate financial owner. It copies only the actual version/date/active into the separate private receipt first. Native Snooze rows, immutable event/request/command receipts, original BEFORE/deferred guards and native agency sequence remain unchanged. A future due receipt is consumed separately by the bounded service action. The Case branch reads the actual CASE Snooze scope; it does not fabricate a related Work or touch finance counters.

Re-ran the static complete-body comparison only (no function invocation): the final 1438 core versus installed 1410 differs solely by removal of the two formerly unbounded linked-Case prehold/refresh loops, which the reviewed wrapper/continuation now owns. The final 1438 hold versus installed 1236 differs solely by the UUID array declaration, one append of each actually visited Work ID, and the final refresh_many call after the original financial-view update. Qualified rowtype SELECT aliases in the origin wrapper preserve that identity and satisfy the projection source convention. The native valuer, counts, amounts, immutable identities and economic mutations are preserved.

Private task/receipt RLS and ACLs, service-only finite wrapper, exact generation/lease/cursor revalidation, current Seed fences, locked atomic CURRENT adoption, true pending/error counters, >100 linked-Case handoff and final Work closure pass source review within this SQL batch. The proposed separate metadata task is genuine WB44 continuation support, not a financial command/enrollment protocol or early replacement of later finance owners. No product source was edited and no SQL, DB, runtime, tests, fixtures, app, browser, hosted, provider or payment operation was executed. This verdict does not certify the pending Worker wiring or later application/timing receipts.
