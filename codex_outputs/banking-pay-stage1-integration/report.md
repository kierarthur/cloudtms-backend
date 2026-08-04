# Banking Pay integrated database Stage 1 report

## Outcome

Database Stage 1 for the bounded-scope Workbench integration and the separately
implemented Banking Pay cancellation work has been integrated, installed in
`test-cloudtms`, and verified.

The installation contains:

- two one-time schema migrations;
- 27 cancellation repeatables;
- 30 bounded-scope repeatables, with the trigger bundle installed last;
- one canonical installable repeatable for each replaced function;
- an exact pre-install rollback pack for both change sets.

Cancellation remains disabled. No bounded-scope bootstrap, READY cutover,
Worker change, frontend change, Worker deployment, payment execution,
settlement, provider action, or production action was performed.

## Repository integration

The bounded-scope handoff was integrated first, followed by the cancellation
handoff, as required by the collision brief:

- bounded-scope handoff source: `3bcb25ee6e66f82557a25cc0e3e18726cbd2b887`;
- cancellation handoff source: `800b72d4b2aef0903f881f6736c85a143f3d9765`;
- integrated backend commits before final publication: `db0cade` and `dbed59f`.

The three superseded deployed-definition review files were removed from the
active `supabase/repeatable` installation surface and retained under
`tests/fixtures/banking-pay-installed-baselines`. Tests that need historical
formula evidence read those fixtures; deployment tests read the new canonical
`04082026_*` owners.

No duplicate active function owner remains for:

- `pay_workbench_candidate_source_build_chunk`;
- `pay_sync_overpayments_from_preview`;
- `pay_workbench_repair_orphaned_pending_source_build`.

## TEST installation

Target authority:

- Supabase project: `test-cloudtms`;
- project reference: `yakevhtttcsljosbdpov`;
- PostgreSQL: `17.6`;
- repository baseline before integration: `51862116f470f7cba2332c4b310cb92af52c0a5b`.

Pre-install checks proved:

- the new cancellation and bounded-scope relations were absent;
- the two settings surfaces were absent;
- there were no duplicate active correction requests per batch;
- there were no running Workbench source-build jobs;
- there were no active correction requests;
- all changed-function identities and installed baseline hashes were present.

The cancellation schema migration installed first. The first bounded-scope
submission was rejected at parse time because a diagnostic file reader had
truncated the SQL text. PostgreSQL applied none of it; a catalogue check proved
the bounded registry was still absent. The unchanged migration was then read as
exact bytes, resubmitted, and installed successfully.

All runtime definitions subsequently installed successfully in dependency
order. Supabase records 59 Stage 1 migrations for this operation:

- two schema records;
- 27 cancellation runtime/ACL records;
- 30 bounded-scope runtime/trigger records.

The schema migrations perform only the plan-authorised DDL and the insertion of
five disabled `PAYMENT_CORRECTION` operation-phase configuration rows. No
financial or legacy bootstrap data was written.

## Installed catalogue verification

Post-install read-only catalogue checks proved:

| Check | Result |
| --- | ---: |
| New bounded private tables | 8 |
| Bounded named constraints | 88 |
| Bounded indexes | 47 |
| Canonical staging named unique constraint | 1 |
| Canonical `NULLS NOT DISTINCT` backing index | true |
| New statement-level invalidation triggers | 16 |
| New cancellation and bounded function names | 17/17, one overload each |
| Changed public function names | 35/35, one overload each |
| Wrong target function owners | 0 |
| Public target functions missing `SECURITY DEFINER` | 0 |
| Untrusted execute grants on private helpers | 0 |
| Untrusted execute grants on public private-boundary RPCs | 0 |
| Public private-boundary RPCs missing service-role execute | 0 |
| Retired compatibility entry points executable by service role | 0 |
| Direct application-role grants on private durable tables | 0 |
| Cancellation operation phases installed disabled | 5/5 |
| Cancellation enabled | false |
| Candidate registry rows | 0 |
| Economic build rows | 0 |
| Attempt rows | 0 |
| Cancellation membership rows | 0 |
| Shared source-line constraints containing `STAGED` | 0 |

The three retained row-level finance dirty triggers are installed with the
required `BEFORE` timing. The 16 new statement triggers exist at their exact
stable identities.

The Supabase security advisor reports only the intentional INFO notice that the
forced-RLS cancellation membership table has no policies. This is the designed
fail-closed boundary: there are no direct application-role grants. The
performance advisor reports expected unused-index INFO notices because the new
bounded tables are deliberately empty before bootstrap, plus an INFO advisory
for the immutable cancellation membership table's non-leading candidate FK.
No advisor finding indicates a browser-access path or a cutover blocker.

## Policy X and contract verification

The installed hashes of the untouched post-draft Policy X helpers remain:

- `_pay_batch_item_economic_components`: `c42fe0e924490ba6974abe10123245ab`;
- `_pay_batch_item_source_reservation_amount_ex_vat`: `00dec6f321142ad179ba1e8712247f62`;
- `_pay_policy_x_resolve_post_draft_economic_key`: `caec9f40c244449eac2dd2cbf7805027`.

The shared source-line table retains its existing public status lifecycle; no
private `STAGED` status or RLS change was introduced. Public Banking Pay
function signatures and response contracts were preserved.

## Local SQL and test verification

A fresh disposable PostgreSQL 18 cluster proved:

- both migrations install in order;
- all 57 repeatables install in order;
- all trigger objects install after their function dependencies;
- all 19 exact cancellation rollback function/ACL files compile;
- cancellation schema rollback completes;
- the 1.5 MB exact bounded-scope rollback completes;
- the combined database returns to the mocked pre-install catalogue.

Repository verification after TEST installation:

- integrated focused Stage 1 suite: 39 passed, 0 failed;
- full `tests/*.test.cjs` sweep: 298 passed, 17 skipped, 0 failed;
- `npm test`: 206 passed, 0 failed;
- `git diff --check`: passed.

The 17 skipped tests are pre-existing disposable-runtime cases requiring their
separate PostgreSQL fixture; the combined Stage 1 disposable compilation and
rollback checks were run directly and passed.

## Rollback evidence

The rollback pack is at:

`codex_outputs/banking-pay-stage1-integration/rollback`

It contains:

- 15 exact pre-install cancellation replacement-function definitions;
- four exact pre-install compatibility definitions and ACLs;
- cancellation schema teardown and original constraint restoration;
- an ordered rollback README;
- a reference to the separately frozen exact bounded-scope rollback at
  `codex_outputs/banking-pay-bounded-scope-v12/rollback.sql`.

Each exported function file includes the pre-install definition MD5, exact
`pg_get_functiondef`, owner restoration and execute-grant restoration.

## Deliberately not performed

- No bounded legacy-data bootstrap.
- No candidate READY finalisation or ordinary-selector cutover.
- No two-call Worker implementation or Worker deployment.
- No cancellation Worker/backend Stage 2 implementation.
- No frontend change or deployment.
- No browser mutation test.
- No payment, settlement, remittance, provider, webhook or banking action.
- No production access or deployment.

Stage 2 must integrate the bounded-scope Worker lane first and then rebase the
cancellation Worker changes by symbol in the designated integration pass.
Bootstrap and all financial/performance/concurrency gates remain later cutover
work; they were not silently treated as passed by this schema installation.

## Safety confirmation

- Secrets printed: no.
- Destructive financial SQL/RPC/actions run: no.
- TEST schema/function installation: yes, explicitly authorised.
- TEST bootstrap/cutover: no.
- Normal TEST Worker deployment: no.
- Production deployment/access: no.
- Policy X drift: no.
- Raw diagnostic outputs committed: no.
