# WB-40 database source review

11 October 2026, sequence 76. Independent static review of the authored, not-yet-loaded0426 migration and three0426 repeatables, against the full card/checklist, R2-F01/F02/WB-40, master successor chain, BD085/build scope, current investigation and independent scope review. Root alone writes product. No SQL/database calls, tests, fixtures, browser/hosted calls or commits were performed by this reviewer.

## Verdict

**Source PASS for the narrow WB-40 definitions.** No essential WB-40 source defect remains in the reviewed saved source. The CREATE-to-SEAL presentation limitation below is an existing, explicitly retained WB-20 seam; it is not claimed fixed or covered by this PASS. Compilation, installation, actual trigger/concurrency behavior and app acceptance remain NOT_RUN by this reviewer.

## Reviewed definitions

| Saved source | Reviewed closure |
| --- | --- |
| `supabase/migrations/11102026_0426_banking_pay_next_pay_hold_control_fields.sql:7` | Small typed control fields on existing Work and work_choice; no table or business-row backfill. |
| `supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_controls_v1.sql:7` | Named D08 private trigger, NEXT early return, exact two-table native bindings. |
| `supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_classification_v1.sql:4` | Latest1946 Work core successor; no replacement of current1438 wrapper or Case core. |
| `supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql:4`, `:104`, `:166` | Final1236 choice guard, current1832 append skip helper, current0005 original CREATE successors. |

## Source findings and checks

- **Canonical ownership and finite scope:** controls16–38 resolve at most the native old/new booking dependencies, using current Timesheet/current TSFIN and unique Work booking. Financial changes to a historical Timesheet do not qualify merely because its TSFIN has is_current=true. The locked canonical read49–54 has no original/historical fallback. Inserts and currentness/identity replacement are covered. The early no-op comparisons exclude since/updated-at-only changes, and the effective tuple comparison55–56 normalizes reason away when false.
- **Native serialization:** controls42–47 lock Candidate worker_control then exact Works in deterministic order, before rereading canonical identities. No public row lock or new command-clock lock/increment is acquired after Work. This avoids the public-row-write -> private Work versus CREATE private Work -> public-row-lock inversion identified in scope review. The control version increments under those locks, changed time uses clock_timestamp at58, the existing Candidate financial view advances at60, and refresh goes through the current wrapper62. Its retained queued ORIGIN path (`10102026_1429:268–279`) schedules and marks Processing.
- **Missing-current-row gap:** classifier24–29 explicitly records canonical completeness. At192–197 a missing Timesheet/TSFIN blocks the new unselected offer as DATA_UNAVAILABLE priority15. It is never interpreted as a false hold proving readiness. CREATE519–522 and legacy skip133–136 retain bounded REVISION_NOT_CURRENT skip vocabulary for this absence rather than capturing an unqualified live default.
- **Reason precedence and retained money:** classifier185–186 saves the pre-hold `v_base`; only the new offer branch189–197 gains canonical/hold priority. PAY_ON_HOLD priority40 cannot replace stronger missing-data/approval/Processing facts, and subsequent query priority60/61 cannot override hold40. `is_selected` portions are excluded from the new hold branch. Recovery and reserved branches255–270 retain the native base, amounts, run linkage and original priority logic. No financial position, reservation hold, Case allocation, producer or capture/PIN admission is changed.
- **Original accepted control:** choice guard23–32 captures genuine current canonical IDs, boolean/reason and Work control version/time on the original work_choice INSERT, under exact Work SHARE and plain public MVCC reads. CREATE already takes the bounded Work SHARE locks before its clock/run cut. OPEN and SEALED updates both protect the six captured columns at35–40; original component/segment counters and sealing rules remain intact. Control version zero remains the untouched baseline, and no earlier changed time is invented.
- **Immutable skip/retry:** original CREATE retains the existing ordered per-Work run_choice_member skip at573–574 and accepts native work_choice only on the unchanged successful path580–582. New current hold output is the exact card code BPAY_NEXT_SELECTION_ON_HOLD527. Accepted helper113–119 reads original run_choice_member.skip_code first, including held Work without a choice row, and fails closed if accepted membership is absent. The existing accepted page acknowledgement (`10102026_1832:601–619`) already replays that ordered skip and never calls the late live helper. Later set/clear cannot silently add or remove accepted membership.
- **Retained boundaries:** private ACLs/owners are explicit, and only the new named private trigger/bindings are added. The three successors preserve the actual current owners instead of reinstalling older cores/wrappers. The retained Worker PATCH at `broker/src/index.js:110084–110134` and its unfenced route at `broker/src/bpay-next-legacy-fence.js:162–184` are unchanged. Existing UI/projection fields and closed skip decoder remain sufficient; there is no new command, route, reader/transport schema, control-history store or browser calculator.

## Existing WB-20 presentation limitation

`private.bpay_next_workbench_accepted_work_v1`, final `10102026_1252_banking_pay_next_workbench_summary_v1.sql:167`, joins only selection_state=SEALED. Therefore between original accepted CREATE and page SEAL, a later hold can make the current Workbench row BLOCKED because is_selected is not yet exposed. The accepted Work membership, original skip/control and subsequent capture remain immutable; the hold does not invalidate that first offer. Do not describe this as every accepted-at-CREATE row already staying WAITING before SEAL.

The builder confirmed this is the still-open WB-20 contract, "Exact frozen offer exclusion/link from Create, not confirmed-only", and will retain/correct handoff wording rather than expand WB-40 into that owner. This review preserves that boundary explicitly. Already linked/SEALED accepted portions and actual reservations retain their native WAITING behavior through the new branch.

## Static source identity and remaining evidence

SHA-256 at reread:

- control-fields migration: `3bbe10c7fc86b1aaf5da291f75aa3bd4ab340140a81bdce34d4d7581458f3fb0`.
- controls repeatable: `aeedd544d3e1a89d4cc5802e8304e0826b4d0044d1ea43bcfb52b5b3b6ae2de9`.
- classification repeatable: `055b9c2fe70b085e7122dde2ef205da89cc0c2e96ffe6f2a48044b410f84f663`.
- accepted-choice repeatable: `aec4c904cee22e748ea1ab3006e8cfdbf50bcdb9dd567b1fb1b2bc9da7391b92`.

This identifies authored saved source, not an installed database or passing runtime. Sole local compilation, source/contract coupling and exact final-definer registration remain builder evidence. Actual current-row insert/rotation, same-value no-op, historical-row isolation, concurrent hold/CREATE ordering, retained PATCH result and app behavior need their separately authorised checks. No acceptance box is implied by this static PASS.
