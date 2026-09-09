# H12 reboot checkpoint — 9 September 2026 11:09 Europe/London

## Exact source identity

- Worktree: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\h12-source-less-cancel-alert-v1-backend`
- Branch: `codex/h12-source-less-cancel-alert-v1`
- Base HEAD: `34d073fef04c365fc0c557f881b749ce88a7e7d7`
- Worktree is intentionally dirty with the saved H12 candidate and evidence files. Do not reset, restore, or discard them.
- Resume-capable harness SHA-256: `ba8b039c5087050ce795e6859adc086b1fc01979ed698459c0b3a1110b841465`

## Cancellation matrix checkpoint

- Logical cases passed: `1-16` (`16/40`).
- The earlier failure occurred after cases `1-10`.
- The corrected failed PAYE block, cases `11-16`, has exact saved PASS evidence on PostgreSQL 17 and PostgreSQL 18.
- Cases `17-20` (whole-Draft Umbrella no-money paths on both engines) were running when the user requested a reboot. They were stopped safely before completion and remain `NOT PASSED`; resume from case `17`.
- Cases `21-40` remain `NOT YET RUN` in this continuation sequence.
- Do not rerun cases `1-16` until the single mandatory final clean `40/40` audit after every first-pass block is complete and any failures are corrected.

## Saved evidence for the corrected failed block

- PG17: `P12_SOURCE_LESS_CANCELLATION_RUNTIME_RESULTS_PG17_NO_MONEY_WHOLE_DRAFT_PAYE_FINAL_V1.json`
  - SHA-256: `3a5c87d9b953412dc02bfb37c4989d269a08e25ae5b8652b2465f6dd92f6a204`
- PG18: `P12_SOURCE_LESS_CANCELLATION_RUNTIME_RESULTS_PG18_NO_MONEY_WHOLE_DRAFT_PAYE_FINAL_V1.json`
  - SHA-256: `f47a59912dd0e27dab2543c8263b53eca835877c7e3cab71d1f7e66e981e8bc2`

## Verified reusable database copies retained across reboot

PostgreSQL 17 container: `h12-v8-restart-pg17`

- Verified original: `h12_rg5_prepared_pg17`
- Untouched spare: `h12_rg5_spare_pg17`
- Source fingerprint: `2c43b0d3ea53be13f60c0ac910d4a97b9cc0070e54dc169d27b3aeae0ba199a5`
- Semantic SHA-256: `5ea67f93fc2d136c932e05fdd7168729915e4b3ae4f3a11ca404ae103d22d44f`
- Exact SHA-256 for this fresh prepared copy: `3ad6f3e4493a953867ad0aa933b9b17a5f4769f1f4a7d4bc9f5b56b930c9068a`

PostgreSQL 18 container: `h12-v8-restart-pg18`

- Verified original: `h12_rg5_prepared_pg18`
- Untouched spare: `h12_rg5_spare_pg18`
- Source fingerprint: `2c43b0d3ea53be13f60c0ac910d4a97b9cc0070e54dc169d27b3aeae0ba199a5`
- Semantic SHA-256: `75005bc03dfc0ff2db33ded4dbf83de593a68d131e500de0d3be8941850a6276`
- Exact SHA-256 for this fresh prepared copy: `928fdf89b2f265fe7d2389112ac8f793abec76e7bf869a922a4e5b5ec81eb683`

The two interrupted disposable working databases `h2_cancel_v8_pg17` and `h2_cancel_v8_pg18` were removed before reboot. No other Docker database, container, image, volume, or task resource was removed.

## Immediate resume action

1. Confirm Docker restored containers `h12-v8-restart-pg17` and `h12-v8-restart-pg18`.
2. Confirm both verified originals and both spares exist.
3. Re-run only the filtered `NO_MONEY / WHOLE_DRAFT / UMBRELLA` journeys from the verified originals, one physical journey per engine. These are logical cases `17-18` on PG17 and `19-20` on PG18.
4. Save their result files and update the counter after each engine completes.
5. Continue only with cases `21-40`; do not repeat `1-16`.

## Safety status at shutdown

- No H12 Node test runner remains active.
- No repository commit, push, Miget mutation, Cloudflare deployment, real Draft, provider action, settlement action, remittance action, or payment action occurred.
- Payment policy/economics remain unchanged.
