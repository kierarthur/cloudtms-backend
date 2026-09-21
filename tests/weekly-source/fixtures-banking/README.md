# Banking Pay evidence fixtures (Weekly Source Plan 6.2, WP-16a)

Banking Pay evidence in exact states, for the Weekly Source proofs `R1–R44`
(`P:\proof\32 §12`), `ROT-001..012` and `UNA-001..019`.

Under contract decision **D2** these are **fixtures in the existing Banking Pay
evidence tables**. Banking Pay's unfinished new logic is never exercised and no
result that depends on it is ever inferred. Nothing here defines, wraps,
re-creates or re-points any Banking Pay function, table, trigger or constraint.
Every object this library creates lives in the schema `ws_banking_fixture`, which
exists only inside a disposable local clone; no file under `supabase/` references
it and it is not part of any release.

## Running it

```bash
export PGOPTIONS='-c jit=off'          # required for this database (environment report §6.1)
export PSQL_BIN='C:\Program Files\PostgreSQL\18\bin\psql.exe'

# a disposable clone
psql "$ADMIN_URL" -c "create database ws62_wp16a_run template ws62_template;"
psql "$ADMIN_URL" -c "alter database ws62_wp16a_run set jit = off;"

node tests/weekly-source/fixtures-banking/load-banking-fixtures.mjs \
  --url "postgresql://postgres:***@127.0.0.1:55433/ws62_wp16a_run" --selfcheck
```

or, with `psql` directly, from this directory:

```bash
psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f install.sql
psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f build-all.sql
psql "$DB_URL" -X -v ON_ERROR_STOP=1 -f selfcheck.sql
```

**Do not wrap `build-all.sql` in an explicit transaction.** psql's default
autocommit gives each statement its own transaction, which the real pre-bank
cancellation chain needs — see *Two transactions, and why* and *How far the
correction chain is driven* below.

`ws_banking_fixture.assert_local_only()` refuses to run anywhere that is not a
disposable local proof database (`banking_modal_v2_test`,
`banking_modal_v2_release<N>_<8 digits>`, `banking_modal_v2_contract_<8 digits>`
or `ws62_*`), and refuses outright when `private.cloudtms_database_identity`
says the environment is `LIVE`.

## Files

| File | What it is |
| --- | --- |
| `010_fixture_library_core.sql` | the local-only guard, deterministic identities, the **constraint verifier**, the seed register and the state register |
| `020_base_world.sql` | Candidates, Contract, Timesheet families (including the rotated family and the whitespace-padded booking id), TSFIN, Workbench session, and the Draft-batch seed |
| `030_real_owner_cancellation.sql` | driver for the **real installed** pre-bank cancellation chain (Binding A) |
| `040_real_owner_abort_and_paye_net.sql` | drivers for the **real installed** Binding B and Binding C owners |
| `050_seed_settlement.sql` | named seeds for settlement evidence, each cited to the `pay_settle_rail` statement that writes it |
| `060_seed_transfers_operations_reservations.sql` | named seeds for transfers, operations, reservations, born-voided items, the synthetic unbound void and correction blockers |
| `070_states.sql` | one function per named evidence state |
| `install.sql` | installs the library |
| `build-all.sql` | builds every state |
| `selfcheck.sql` | 28 assertion groups that read the database, never the builder's return value |
| `load-banking-fixtures.mjs` | thin loader; shells out to `psql` exactly as `tests/weekly-source/adapters/database-scenario-adapter.mjs` does, adding no dependency |

## The base world

| Family | `booking_id` | Versions | Candidate | Notes |
| --- | --- | --- | --- | --- |
| A | `'  ws-fixture-booking-a  '` | v1 superseded, **v2 current** | A (Umbrella) | **rotated** family; the booking id carries surrounding whitespace on purpose (`R37`, `ROT`). v1 also carries a non-current TSFIN row so a family-wide census sees financials on an earlier version (`R21`) |
| B | `ws-fixture-booking-b` | v1 current | B (PAYE) | |
| C | `ws-fixture-booking-c` | v1 current | C (PAYE) | |
| D | `ws-fixture-booking-d` | v1 current | A (Umbrella) | used by `R28`, a root settled in two batches |
| E | `ws-fixture-booking-e` | v1 current | D (PAYE) | reserved for the Binding C state; see *Build order matters* below |
| F | `ws-fixture-booking-f` | v1 current | E (PAYE) | reserved for the terminal Binding A state |
| G | `ws-fixture-booking-g` | v1 current | F (PAYE) | the cancelled Candidate of the `R41` pair |
| H | `ws-fixture-booking-h` | v1 current | G (PAYE) | the surviving, later-settled Candidate of the `R41` pair |

Candidates E, F and G are each used by exactly one state. The installed
`REFRESH_WORKBENCH` phase proves the preceding scope invalidation against the
Candidate's **live** scope generation
(`pay_workbench_enqueue_candidate_refresh`,
`PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED`), so a Candidate whose
generation another state has since bumped can no longer complete its own
correction. States that must reach a terminal operation therefore get a
Candidate nothing else touches.

Candidate A starts on Umbrella **one**. `state_umbrella_batch_level_transfers_v1`
moves it to Umbrella **two** *after* the Draft item has frozen Umbrella one, which
is the last arm of `R44`.

## Every state

Build method: **REAL_OWNER** — produced entirely by installed Banking Pay owners;
**MIXED** — produced by an installed owner, then one named seed on top;
**NAMED_SEED** — seeded directly, with the installed writer and its exact code
lines cited in `ws_banking_fixture.seed_register`.

### Built by the real installed owners

| State | Built by | Serves |
| --- | --- | --- |
| `batch_cancelled_whole_binding_a` | `pay_payment_correction_request_start` → `banking_pay_operation_claim_next` → `pay_payment_correction_selection_prepare_chunk_v1` → `pay_payment_correction_reauth_bind_v1` → `request_start(START_PREPARED)` → `pay_payment_correction_expand_work` → **`pay_pre_bank_cancel_apply_work_item`** → **`pay_payment_correction_process_chunk`** | `R1`; `proof/32 §5.1` Binding A; `§4.2` class 2 |
| `candidate_cancelled_out_of_multi_draft` | the same chain, one Candidate of two | `R41`; `R3`; `UNA-019`; `proof/32 §4.3` C6; `proof/36 §4` W3/W4 |
| `batch_aborted_failed_draft_create` | **`pay_batch_abort_failed_draft_create_partial`** | `R29`; `proof/32 §5.1` Binding B |
| `paye_net_manual_void` | **`pay_set_paye_net_manual`** | `R30`; `proof/32 §5.1` Binding C |
| `pre_bank_cancel_voided_transfer` | the same chain, with a transfer bound to the item before it is cancelled | `proof/32 §5.1` common conditions; `§4.3` C5; `proof/36 §4` W2 |

Proved outcomes (asserted by `selfcheck.sql`, read back from the database):

* whole-batch Binding A → `pay_batches.status = 'CANCELLED'`, `cancelled_at_utc`
  set, `cancel_reason = 'DRAFT_PAYMENT_CANCELLED_BY_USER'`, zero non-voided items,
  correction request `APPLIED`, work item `APPLIED` with `result_code APPLIED`;
* one-Candidate-out-of-many → batch back to `DRAFT`, exactly one of two items
  voided, the remainder alive;
* Binding B → `CANCELLED` + `cancelled_at_utc` with **no correction request at
  all**, which is precisely what distinguishes Binding B from Binding A;
* Binding C → the `LOAN_REPAYMENT` item voided with `reservation_id` nulled, its
  reservation `RELEASED` with `released_reason = 'PAYE_NET_REPROJECTION'`, and the
  batch still a live `DRAFT`.

### Built by the real owner, then one named seed

| State | Seed on top | Serves |
| --- | --- | --- |
| `applied_with_blockers_outside_family` | the request promoted to `APPLIED_WITH_BLOCKERS` with a blocker naming an item **outside** the member family | `R19`, released half |
| `applied_with_blockers_inside_family` | a blocker naming a **family** item | `R19`, frozen half |
| `applied_with_blockers_no_item_identity` | a blocker with **no** item identity | `R38` |
| `batch_cancelled_whole_binding_a_complete` | the installed `banking_pay_operation_finish` is driven, then **one column** — `phase = 'COMPLETE'` — is seeded | `R1` terminal; `proof/32 §5.1` Binding A third bullet |
| `same_family_draft_remainder` | a re-projected `TIMESHEET_PAYMENT` item for the same Candidate, same family, same Timesheet, added to the `DRAFT` remainder | `R3`; `R19` second case; `proof/32 §4.3` C6 |
| `multi_draft_remainder_settled` | settlement evidence applied to the remainder after the cancellation | `R41` second half; `UNA-019` |

### Named seeds

| State | Serves | Shape |
| --- | --- | --- |
| `batch_settled` | `R2`; `proof/32 §5.2` | `SETTLED` + `COMMITTED` + `completed_at_utc` + `execution_committed_at_utc`; Candidate `SETTLED` with `settled_at_utc`; exactly one `timesheet_pay_state_history` row whose signature equals the snapshot the settle rail's own selector would choose; bound transfer classifies `FINAL_PAID` |
| `settled_batch_retains_schedule_kind` | `R16` | a `SETTLED` batch that still carries `schedule_kind = 'SCHEDULED'` and `scheduled_at_utc` |
| `batch_failed_completed_mixed` | `R43` | `FAILED` with `completed_at_utc` and `COMMITTED`; one Candidate `SETTLED` with full proof, another `FAILED` |
| `partial_settlement` | `R4` | one Candidate `SETTLED`, one `PENDING`, in a `SETTLED` container |
| `settlement_history_duplicate_different_signature` | `R20` | two history rows for one `(timesheet_id, pay_batch_id)`, different signatures |
| `settlement_history_duplicate_identical` | `R34` | two history rows, identical JSON and signature |
| `settlement_snapshot_missing` | `R35` | no `pay_batch_timesheet_snapshots` row |
| `settlement_snapshot_empty_signature` | `R35` | one snapshot whose `signature` is `''` |
| `settlement_snapshot_conflicting_second` | `R35` | two snapshot rows for one Timesheet with different `target_snapshot_json` |
| `settlement_snapshot_signature_mismatch` | `R35` | history signature ≠ the chosen snapshot's |
| `root_settled_in_two_batches` | `R28` | one Timesheet with two history rows in two batches; the `timesheet_pay_state` cache holds only the later one |
| `transfers_unknown_and_pending` | `R5` | a provider-`UNKNOWN` transfer and a `PROCESSING` / `IN_FLIGHT` one |
| `transfers_returned_and_reversed` | `R18` | `RETURNED`, `REVERSED`, and the bare `COMMITTED` / `EXECUTED` raw strings |
| `operation_history_forms` | `R17`, `R36` | a `COMPLETE` operation keeping `scope_freeze_status = 'FROZEN'` with both leases null; a terminal operation whose legacy `lock_expires_at_utc` is still in the future; and a genuinely live operation |
| `umbrella_batch_level_transfers` | `R44` | batch-level Umbrella transfers with a null `candidate_id`: matched, contradictory, ambiguous; an item with **no** frozen Umbrella; and the Candidate's current Umbrella changed after the Draft froze |
| `reservation_lifecycle` | `proof/32 §4.3` C2, `§5.1`, `§5.2` | all four of `RESERVED`, `COMMITTED`, `SETTLED`, `RELEASED` |
| `synthetic_unbound_void` | `R31` | a voided item in a `SETTLED` batch with no correction request, a still-`RESERVED` reservation and a `FINAL_PAID` bound transfer, so no binding can prove it |
| `born_voided_item` | census 01 §4.3 unbound writer 1; contract OR-3 | an item inserted with `is_voided = true` from the start |

## Why settlement is seeded and cancellation is driven

`public.pay_settle_rail` is the sole installed writer of batch terminality
(census 01 §4.2). Its full-batch path refuses unless the batch already carries a
server-frozen `execution_intent_json` naming a `PAYMENT_EXECUTE` operation, and
exactly one `AUTHORISED` `pay_batch_auth_requests` row bound to that operation
whose own `execution_intent_json` reproduces the authorised PAYE-net state hash,
bank-payment projection hash and explicit-zero counts
(`BATCH_EXECUTION_INTENT_REQUIRED`, `EXECUTION_OPERATION_REQUIRED`,
`EXECUTION_OPERATION_INVALID`, `AUTHORISED_EXECUTION_INTENT_REQUIRED`). Reaching
it means driving payment execution and authorisation, which contract section 2
puts on the do-not-touch list. No repository test drives it either —
`grep -rl pay_settle_rail tests/ supabase/verification/` returns only static
SQL-contract tests. So settlement evidence is seeded to the exact shape the
installed writer produces, and `050_seed_settlement.sql` cites, per column, the
`pg_get_functiondef(public.pay_settle_rail)` statement that writes it.

Cancellation, by contrast, **is** drivable end to end, and is driven.

## Two transactions, and why

`pay_payment_correction_expand_work` sets the operation's
`run_after_utc = clock_timestamp()`, while `banking_pay_operation_claim_next`
compares against `now()` — the transaction start time. Inside one transaction the
next claim therefore always returns `RUN_AFTER_NOT_DUE`. That is installed
Banking Pay behaviour and this library does not work around it: the
`PROCESS_CHUNKS` / `FINALISE` step lives in
`ws_banking_fixture.finalise_pre_bank_cancellation_v1`, called from a later
transaction, and `build-all.sql` calls
`ws_banking_fixture.finalise_pending_cancellations_v1()` five times, one
transaction each.

## How far the correction chain is driven, and where it stops

The chain is driven through `PREPARE_SELECTION`, `EXPAND_WORK`, `PROCESS_CHUNKS`
and `FINALISE` by the installed owners, and **into** `REFRESH_WORKBENCH`: for a
Candidate whose live scope generation is still the one the void transaction
produced, `pay_payment_correction_process_chunk` runs the phase,
`pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1` calls
`pay_workbench_enqueue_candidate_refresh` and a `WORKBENCH_CANDIDATE_DIRTY_APPLY`
job is queued. `state_batch_cancelled_whole_binding_a_complete` does exactly that,
and `selfcheck.sql` asserts the chunk really ran.

It stops there. A **further** `process_chunk` call cannot advance past
`REFRESH_WORKBENCH`, because the installed enqueue then returns

```json
{"ok": true, "no_op": true, "blocked": true, "resolved_mode": "BLOCKED",
 "resolved_job_type": "NOOP",
 "fallback_reason": "DELTA_REFRESH_DISABLED_FOR_RESERVATION_PATCH"}
```

and `patch_preview` treats that as `PAYMENT_CANCEL_FULL_REFRESH_JOB_INVALID`. The
queued `WORKBENCH_CANDIDATE_DIRTY_APPLY` job has to be consumed by the Workbench
candidate refresh worker first, and that worker rebuilds the Workbench preview —
the dependency closure and source build that contract section 2 keeps out of
scope.

So the terminal operation is reached like this:

* `public.banking_pay_operation_finish(operation_id, 'COMPLETE', …)` — an
  **installed owner** — writes `status = 'COMPLETE'`, `runner_state = 'COMPLETE'`,
  `completed_at_utc`, and nulls both lease forms (installed body lines 372-387).
  It does **not** write `phase`.
* `phase = 'COMPLETE'` and the `progress_json.workbench_refresh_status` marker are
  therefore the **only** seeded values, reproducing the installed terminal write
  in `pay_payment_correction_process_chunk` (installed body lines 2402-2419).

Both shapes are kept as separate, named states, because the mid-flight one
exposed a real defect in the census:

* `batch_cancelled_whole_binding_a` — batch `CANCELLED`, operation still
  `RUNNING` at `REFRESH_WORKBENCH`;
* `batch_cancelled_whole_binding_a_complete` — operation `COMPLETE` / `COMPLETE`,
  which is what `proof/32 §5.1` Binding A's third bullet requires.

Nothing about this primes a Workbench session setting or re-creates a Workbench
owner: `private.pay_workbench_scope_invalidate_v1`,
`pay_workbench_mark_candidate_dirty` and the enqueue stay call-only throughout.

## Installed facts that change what a test may assert

1. **`REVERSED` is not a legal `pay_bank_transfers.status`.**
   `pay_bank_transfers_status_chk_v3` permits `PENDING, PROCESSING, UNKNOWN,
   COMPLETED, FAILED, DECLINED, REJECTED, CANCELLED, VOIDED, RETURNED, REVERTED,
   BLOCKED, SUBMISSION_FAILED, FAILED_BEFORE_COMMIT` — there is no `REVERSED`.
   `proof/32 §4.3` C5 and `§12 R18` name it, so it has to be expressed somewhere:
   the word reaches the installed classifier only through `rail_state` (or the
   rail/provider JSON), because the classifier's ambiguity test reads the whole
   term array — `v_ambiguous_return_or_revert := EXISTS (... status_text IN
   ('RETURNED','REVERTED','REVERSED'))`. The `R18` fixture therefore carries
   `status = 'REVERTED'` with `rail_state = 'REVERSED'`, and the same for the
   bare `COMMITTED` / `EXECUTED` strings. **A test that expects
   `pay_bank_transfers.status = 'REVERSED'` is testing a shape no installed
   writer can produce**, and the constraint would reject the row anyway.
2. **`VOIDED` does not classify as terminal-no-money.** It is absent from the
   installed classifier's terminal list, so a `VOIDED` transfer resolves to
   `UNKNOWN`. Terminal-no-money needs `CANCELLED`, `FAILED`, `REJECTED`,
   `DECLINED` or one of the other listed codes. This matters directly for
   `proof/32 §5.1`'s common condition and for `proof/36 §4` W2, both of which
   turn on `is_terminal_no_money`.
3. **What the installed writers actually produce for a cancelled pre-bank
   payment.** Nine installed routines write `public.pay_bank_transfers`
   (`pay_bank_event_ingest`, `pay_bank_transfers_apply_rail_updates`,
   `pay_execute_provider_submit_review_resolve`,
   `pay_no_money_unwind_apply_work_item`,
   `pay_payment_correction_reauthorisation_overlay_reset_v1`,
   `pay_pre_bank_cancel_apply_work_item`, `pay_provider_submit_chunk_stage_record`,
   `pay_settle_manual_confirm`, `pay_settle_rail`). For a cancellation the
   relevant ones are:

   | Writer | `status` | `rail_state` | other |
   | --- | --- | --- | --- |
   | `pay_pre_bank_cancel_apply_work_item` (Binding A) | `'VOIDED'`, but **only** when the transfer's remaining non-voided amount reaches 0 and it is not already `COMPLETED` (installed body lines 2060-2100); otherwise the status is left as it was and only `amount` is reduced | **never written** | `failed_reason = 'PRE_BANK_CANCEL_VOIDED'`; `rail_meta_json` gains `pre_bank_cancel_applied: true` and `pre_bank_cancel_status_note` |
   | `pay_no_money_unwind_apply_work_item` (Binding A) | `'FAILED'` unless already `COMPLETED` (installed body lines 1752-1763) | **never written** | `failed_reason = 'NO_MONEY_UNWIND'`; `rail_meta_json` gains `no_money_unwind_applied: true` |
   | `pay_payment_correction_reauthorisation_overlay_reset_v1` | `'VOIDED'` (census 01 §3) | not written | also clears `pay_batch_items.pay_bank_transfer_id` / `bank_reference` |
   | `pay_settle_rail`, `pay_bank_event_ingest` | provider-driven `COMPLETED` / `FAILED` etc. | provider-driven | the only route to `FINAL_PAID` |

   So `VOIDED` and `FAILED` are the two cancellation outcomes, and they classify
   **differently**: `FAILED` is `TERMINAL_NO_MONEY`, `VOIDED` is `UNKNOWN`. No
   cancellation writer ever sets `rail_state`.

4. **A pre-bank cancellation of a `DRAFT` batch can leave a bound transfer
   completely untouched.** This was observed on a real run of the installed
   chain, and is what `state_pre_bank_cancel_voided_transfer_v1` records: the
   item is voided, and its bound transfer is still `status = 'PENDING'`,
   `rail_state` null, `amount` unchanged, `failed_reason` null, with no
   `pre_bank_cancel_applied` marker — classifying `PENDING_NON_FINAL`. The
   `VOIDED` branch above did not fire; this package does not claim to know the
   exact gate that opens it, and establishing that is open work for WP-16c and
   WP-08b. The consequence is testable now: `proof/32 §5.1`'s common condition
   requires "every transfer bound to `i` … classifies `is_terminal_no_money =
   true`, **or no transfer is bound**", so such an item cannot be proved by
   Binding A and `§4.2` class 1 makes it a `CENSUS_ERROR`. In practice a pre-bank
   Draft usually has **no** transfer bound at all, which is the "or no transfer is
   bound" arm — every other cancellation state in this library is that shape.

5. **A voided recovery item has a NULL `timesheet_id`, so the census never
   enumerates it.** `public.pay_batch_apply_finance_adjustments`, the writer that
   creates the three families `pay_set_paye_net_manual` voids, supplies
   `null::uuid as timesheet_id` for `OVERPAYMENT_RECOVERY` (installed body line
   2218), `MANUAL_DEBT_RECOVERY` (2863), `LOAN_REPAYMENT` (3286) and the dormant
   recovery template (3597). Only `LOAN_PAYOUT` (661) and `UNDERPAYMENT_PAYMENT`
   (1619) carry `linked_timesheet_id`. `proof/32 §4.2` enumerates items by
   `timesheet_id = t` over `F(root)`, so **a Binding C void is never enumerated,
   classified or bound** — and neither is the born-voided template, which answers
   the practical half of open ruling OR-3: it cannot become a `CENSUS_ERROR`
   because it is never reached. A census that requires every voided item in a
   terminal batch to be bindable must scope that requirement to enumerated items.
   The fixture keeps the null, and `selfcheck.sql` asserts it.

## Feature flag and Workbench scope rows

Two operational preconditions are set in the clone and recorded here in the open:

* `settings_defaults.banking_pay_candidate_cancellation_enabled` is set **true**
  by `ws_banking_fixture.enable_candidate_cancellation_flag_v1()`. A freshly
  built local database has it false and
  `pay_payment_correction_request_start` raises
  `PAYMENT_CORRECTION_FEATURE_DISABLED`. This is the same boolean the Office UI
  toggles; it is an operational TEST feature flag, not a change to Banking Pay
  behaviour, and it is never written to a hosted database.
* `banking_pay_workbench_session_scope` rows are created for every fixture
  Candidate, in the shape the repository's own Banking selection fixture
  `tests/fixtures/28082026_1429_banking_pay_selection_setup.sql` uses. Without
  them `pay_workbench_enqueue_candidate_refresh` raises "candidate % is not in
  session scope".

## Build order matters for one state

`public.pay_set_paye_net_manual` runs `public.pay_batch_validate_freshness`,
whose installed PAYE guardrail refuses `BATCH_STALE` /
`PAYE_GUARDRAILS_CHANGED` with *"A PAYE draft batch already exists. Cancel or
delete the existing PAYE draft before creating another PAYE draft."* as soon as
any other PAYE Draft is open. `build-all.sql` therefore builds
`paye_net_manual_void` **first**, on Candidate D / family E, while no other Draft
exists. Its batch stays `DRAFT` afterwards, which is exactly `R30`'s "while the
Draft is alive the root stays FROZEN".

## Constraint proof for every seeded row

`ws_banking_fixture.assert_rows_satisfy_constraints(relation, row_predicate,
context)` re-validates the named rows against **every** installed CHECK
constraint, NOT NULL column, UNIQUE / PRIMARY KEY index (including partial ones)
and FOREIGN KEY of the table, by reading `pg_constraint`, `pg_index` and
`pg_attribute` live and evaluating each `pg_get_constraintdef` body against the
rows. It is a re-derivation from the catalogue, not a copy of a constraint list,
so a schema change in WP-01a cannot silently weaken it. Every seed function calls
it; `selfcheck.sql` also proves it fails closed.

## Registers

* `ws_banking_fixture.seed_register` — one row per direct seed, with the
  installed writer whose output it reproduces and the exact lines cited. A seed
  with no citation is refused at insert time
  (`WS_BANKING_FIXTURE_SEED_CITATION_REQUIRED`) and `selfcheck.sql` asserts the
  register is complete.
* `ws_banking_fixture.state_register` — one row per state, with its build method,
  the proofs it serves and its identity map.

## Which tests these serve

`R1` (both mid-flight and terminal), `R2`, `R3`, `R4`, `R5`, `R16`, `R17`, `R18`,
`R19` (both cases), `R20`, `R28`, `R29`,
`R30`, `R31`, `R34`, `R35`, `R36`, `R38`, `R41`, `R43`, `R44`; the `UNA` W2–W6
refusal half (`proof/36 §4`), in particular `UNA-019`; and the rotated,
whitespace-padded family that `ROT` and `R37` need. The suites themselves belong
to WP-16c; this package delivers only the evidence they read.

## Not covered here

* `R6`–`R15`, `R21`–`R27`, `R32`, `R33`, `R37`, `R39`, `R40`, `R42` depend on the
  Weekly Source pending-publication owner, its receipt relation, the lease and
  the serial gate, none of which exists yet (WP-01a, WP-02, WP-08). They are not
  Banking Pay evidence states and are out of this package's scope.
* No Weekly Source table is written by this library: WP-01a owns that schema.
* The exact gate that opens the `VOIDED` transfer branch of
  `pay_pre_bank_cancel_apply_work_item` is not established (fact 4 above). The
  branch's code and its effects are cited; the condition under which it fires is
  open work for WP-16c and WP-08b.
* `TH-031`'s privacy-safe fixture builder (contract G12-4) is not in this
  package's brief and is not built.

## Hour segments in the frozen snapshot, and the restatement model (WP-16c)

WP-11a proved that every settled family in this library returned
`UNAVAILABLE / SNAPSHOT_SEGMENTS_ABSENT` from
`private.weekly_source_settlement_allocation_v1`, because the three seeded
snapshot literals carried no `segments` array at all. The verdict was correct;
what was missing was any fixture that could carry real paid hours, so no
paid-hours figure could be proved on a fixture at all. WP-16c closed that gap
here, in `050_seed_settlement.sql` and `070_states.sql`.

**The model is restatement, not accumulation.** The independent review of WP-11a
(`WP-11a_REVIEW.md` finding F1) executed the installed chain and established
that Banking Pay does **not** accumulate settlements: every settlement snapshot
restates the Timesheet's complete position for that root and shift, and the
money moved is the residual against the previous position. A week paid at 8
hours and later adjusted to 9 is a **9-hour position, not a 17-hour one**. These
seeds therefore restate, and a shift keeps the **same `segment_id`** across every
settlement of the same root, so a reader can see it is one shift restated rather
than two different shifts to be added together.

**Three positions.** Only the Monday shift moves, so the residual is unambiguous:

| Position | Monday day | Wednesday night | Saturday | Total |
| --- | --- | --- | --- | --- |
| `BASE` | 8.00 | 4.00 | 3.00 | **15.00** |
| `UPWARD` | 9.00 | 4.00 | 3.00 | **16.00** |
| `DOWNWARD` | 7.00 | 4.00 | 3.00 | **14.00** |

**Two model cases.**

| State | Family | Sequence | Meaning |
| --- | --- | --- | --- |
| `root_settled_in_two_batches` (`R28`) | D | `BASE` then `UPWARD` | an upward adjustment: the second settlement restates 16.00 |
| `rotated_root_restated_downward` | A, on the **demoted** `timesheet_a_v1` | `BASE` then `DOWNWARD` | a downward recovery on a rotated family's superseded version — the `UI-012` shape WP-11a's N1 asked for |

Every other settled state carries a single settlement at `BASE`, so a
single-settlement family still gives a clean figure of 15.00.

**Family A is deliberately the multi-settlement, multi-version family.** It
carries three settlements on `timesheet_a_v2` (`batch_settled`,
`partial_settlement`, `batch_failed_completed_mixed`) plus the two on
`timesheet_a_v1` above. That is on purpose: a reader that cannot prove the
restatement reading has a real fixture to fail closed on, **with a reason**,
which is a correct outcome rather than a missing figure.

**Signing.** `public.pay_batch_create_timesheet_snapshots` signs the frozen row
`md5(target_snapshot_json::text)` (installed definition line 329). The seeds now
sign the same way, so the signature is verifiable against the content the hours
are derived from — the point of `WP-11a_REVIEW.md` finding F2. The deliberate
`EMPTY_SIGNATURE` and `SIGNATURE_MISMATCH` modes still produce their own wrong
values on purpose.

**One frozen object, three places.** `ws_banking_fixture.snapshot_target_json_v1`
is the single builder. `pay_batch_timesheet_snapshots.target_snapshot_json`, the
`timesheet_pay_state_history.snapshot_json` copy the rail makes of it, and the
never-authoritative `timesheet_pay_state.last_settled_snapshot_json` all call it,
so they cannot drift apart the way three separate literals did. Before this
change the history seed built its own literal, which is both why N1's original
request would not have lifted `SNAPSHOT_SEGMENTS_ABSENT` — the reader derives
hours from the **history** row, not the snapshot row — and a fidelity defect in
its own right.

**What a consumer must do with these fixtures.** Read the figure only when the
reader reports `state = 'AVAILABLE'`; `NO_SETTLEMENT` is not a zero figure and
`UNAVAILABLE` carries no figure at all. On a family with more than one
settlement, an `UNAVAILABLE` with a reason is a **correct** outcome while the
restatement reading is unruled.

**New callables.**

| Function | Purpose |
| --- | --- |
| `ws_banking_fixture.snapshot_segments_v1(timesheet_id, position, week_ending)` | the shift array for one position; `segment_id` stable per `(root, shift)` |
| `ws_banking_fixture.snapshot_target_json_v1(pay_batch_id, timesheet_id, position, variant)` | the whole frozen target, segments plus the five bucket totals |
| `ws_banking_fixture.snapshot_signature_v1(target)` | `md5(target::text)`, as the installed writer signs |

`seed_timesheet_snapshot_v1`, `seed_settlement_history_v1`,
`seed_last_settled_cache_v1` and `apply_settlement_evidence_v1` each gained a
trailing `p_position` argument (`apply_settlement_evidence_v1` reads it from
`p_options->>'position'`), defaulting to `BASE`, so every existing caller is
unchanged.
