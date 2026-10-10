# WB-05 pair projection lockset source review

**PASS for the finite D09 source successor.** Read-only, 10 October 2026. Reviewed the full156-line `supabase/repeatable/10102026_1403_banking_pay_next_source_pair_projection_lockset_v1.sql`, complete1540 predecessor, current1346 publisher/Processing helper,1252 projection FK/lock definitions, WB-05 card/all nine child rows and the saved pair investigation. No SQL, tests, runtime, fixtures or product edits. No whole-WB05/application acceptance claim.

The new source retains the exact original `(uuid[],uuid[],uuid[])` inputs, table result, security-definer mode and fixed search path. A pure source comparison found exactly one added block and proved the **entire original function definition** equal after removing it (line endings normalized only). Thus all native validation, Candidate/work/revision/physical locks, source-event qualification, sealed-revision refusal, all-or-none receipt replay, deterministic command identity, sorted publication order and original return values are unchanged.

The added block1403:122–135 runs only for the proven all-new pair (`v_existing=0`), after original complete replay/conflict qualification and before either publisher call at146–147. It selects only Contract IDs of the already-locked one/two actual WORKs, ordered by native UUID, and takes `FOR SHARE OF c`. It then selects only their actual Client IDs, ordered by native UUID, and takes `FOR KEY SHARE OF cl`. Duplicate Contract/Client identities collapse through the `IN` predicates; neither query broadens to Candidate history, other works or a summary scan. Contract share stabilizes the helper's later Contract.client_id read, including a non-key mapping update; Client key share preholds the new projection references before the first command clock. Null Client mappings do not create a fabricated target.

Complete replay skips this new prehold block. It retains the predecessor's existing earlier native locks and exact publisher replay reads; it adds no metadata, projection or clock mutation. Partial replay still refuses. A failure of either prehold or either publication remains in the same transaction, preserving atomic rollback of the native pair. No helper is pre-run or called twice to obtain locks.

The first1346 publisher still takes Candidate metadata and the singleton Workbench revision through mark_processing before receive_command. The same Candidate control excludes an independent exact-work projection writer, and the revision lock precedes all apply_delta summary mutation. These existing locks cover the second member's projection/summary writes;1403 adds only the necessary external Contract/Client FK parent scope. It changes no Source approval owner, producer, capture/hold, allocation, money, Case, PIN/enroller, Worker, public route or Rule B authority.

One BEGIN/COMMIT envelope and final `notify pgrst, 'reload schema'` are present. Owner remains postgres; PUBLIC/anon/authenticated/service_role execution is revoked. The original immutable source_command_id helper is not redefined and remains registered at1540. Registry pair entry correctly makes1403 finalPhysical/finalRoot/finalClosures, preserves1540 as predecessor and allowedPhysical, and retains the original baseline identity.

| Reviewed source | SHA256 |
|---|---|
| New1403 | `ea53f357817e88504cf3e2c6bad75ca228b1abe264aeeb10c8d58dd068a7fc21` |
| Original1540 | `f4af5a88d1ede025fdf227df2442a2c49b801104bf02a99a466366663378a4da` |
| Existing1346 publisher/helper | `8934065d152ab69bbe2ec8ee298bd03b66f4c20a82fbbcdced78567db37bb69f` |
| Fresh final-definer registry | `2732d3593adf166b12c533d15c903784b8d98c71f52c2fec5a17b3c844d3dbd6` |

No actionable defect found in this bounded change. The WB-05 producer/path completion evidence and future joined acceptance remain separate; WB44's high-fanout dependency is not closed here.
