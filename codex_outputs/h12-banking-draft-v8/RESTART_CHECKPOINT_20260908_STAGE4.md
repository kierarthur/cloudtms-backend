# H12 Stage 4 restart checkpoint

Saved at `2026-09-08T16:54:42+01:00` before a user-requested Windows reboot.

## Exact source state

- Worktree: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\h12-source-less-cancel-alert-v1-backend`
- Branch: `codex/h12-source-less-cancel-alert-v1`
- Base commit: `34d073fef04c365fc0c557f881b749ce88a7e7d7`
- Base tree: `0c7c1324bbc490e06ce833b549b2f5597c07fef0`
- Worktree is intentionally dirty with the complete local H12 candidate and evidence. Nothing was committed, pushed, installed into Miget, deployed to Cloudflare, or used for a real payment.

Latest snapshot-harness files:

- `scripts/verify-banking-pay-source-less-cancellation-v1.mjs`
  - bytes: `222350`
  - SHA-256: `949b4c3e5d35e14f53dd0fc38c503c6c0f2c2dfd8f1bd84b854a8c5305ab09ae`
- `tests/banking-pay-source-less-cancellation-runtime.test.cjs`
  - bytes: `63799`
  - SHA-256: `b7a3b5dfbef5a9cb2692d25a112ea3f699bbc0af2fe278071f8f526abd98a4e3`
- `tests/postgres-jsonb-build-object-arity-guard.test.js`
  - bytes: `30667`
  - SHA-256: `486a5e4578e4c999a937cbc9e6be1f6077d2f515e358ed9a828cbf9589cb2f8d`

`git diff --check` passed immediately before this checkpoint.

## Completed immediately before reboot

- Diagnosed the PG17 snapshot-equivalence stop as a harness ordering defect, not a payment-policy or economic defect.
- Source proof: PAYE and Umbrella batches share one Workbench session. The old harness captured session-wide history after one side settled but before all sibling batches settled. Later legitimate publication bookkeeping then changed the pre-quiescence full-row hash.
- Applied a harness-only fail-closed correction:
  1. settle every sibling batch in the shared session;
  2. take the exact full historical snapshot only after complete convergence;
  3. replay every sibling;
  4. require zero iterations and zero processed repair work;
  5. retain exact source/preview history hashes, unique current ordinals, zero active jobs/builds, fixture absence, zero external effects and exact clone equality.
- Added five mutation guards that reject removal of either sibling pass, the no-work proof, or either exact history comparison.
- Focused source-less cancellation suite passed `23/23`, with `0` failures, TODOs or skips.
- Both independent reviewers agreed that the correction is test-ordering only and does not weaken policy/economic evidence.

## Interrupted checkpoint

- Command: PG17 candidate-green `--snapshot-equivalence` using the frozen 15-path candidate-source list from the existing PG18 targeted result.
- It was progressing through bounded Workbench calls and had reached the shared PAYE/UMBRELLA session comparison.
- The user requested an urgent reboot, so the command was interrupted deliberately.
- No cancellation, provider, settlement, remittance, email or external payment action began.
- No PostgreSQL session remains active in the task database.
- The interrupted run did not produce a PASS or FAIL and must be rerun from the beginning after reboot.

## Docker state

- `h12-v8-restart-pg17` is backed by named volume `h12-v8-release-pg17-data`.
- `h12-v8-restart-pg18` is backed by named volume `h12-v8-release-pg18-data`.
- Both containers were running and healthy at checkpoint time.
- PG17 currently contains disposable databases `h12_sourceless_baseline_pg17` and `h2_cancel_v8_pg17`; the latter is an interrupted task clone. The harness safely drops/recreates these exact task-owned databases at restart.
- Do not remove or alter any other task's container, database, image or volume.

## Exact resume point

1. Reconfirm the worktree, hashes, Docker volumes and zero active PG17 task sessions.
2. Rerun `node --check` and `node --test tests/banking-pay-source-less-cancellation-runtime.test.cjs`.
3. Recreate and rerun the PG17 candidate-green `--snapshot-equivalence` checkpoint from the beginning.
4. Assert emitted boundary `AFTER_COMPLETE_SESSION_CONVERGENCE` and zero-work replay for every sibling.
5. If PG17 passes, run the identical PG18 checkpoint.
6. Only after both pass, start the 40-case cancellation matrix.
7. Then close the remaining promised Stage 4 checks: genuine simultaneous starts, exact allocations and totals, alert acknowledgement, non-sent email negatives, explicit settlement/remittance/email zero deltas, paired-Timesheet cancellation/rebuild, and traceable alert-scale evidence.

No policy decision, payment calculation, database timeout, payment route, provider action or external state was changed by the last correction.
