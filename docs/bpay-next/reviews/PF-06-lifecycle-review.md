# PF-06-D01 saved lifecycle source review

10 October 2026. Independent source-only review. **PASS for PF-06-D01's exact missing lifecycle-clock supporting implementation and preservation of the existing count authorities. No actionable source defect found.** No SQL, runtime, tests, fixtures, hosted calls or product edits were performed. No installation/app-tested claim.

Read full saved migration `supabase/migrations/10102026_0352_banking_pay_next_job_lifecycle_clocks.sql`, repeatable `supabase/repeatable/10102026_0352_banking_pay_next_job_lifecycle_clock_v1.sql`, exact registry entry, `design/PF-06-SCOPE.md`/BD025, and PF-06's full card/checklist. This closes the earlier conditional lifecycle design review against actual source; D02 Worker hooks and D03 future joined in-app checks are separate.

## Implementation findings

- The immutable migration adds exactly three nullable timestamptz columns to the existing private job: created/started/finished UTC. There are no defaults, business data updates, backfill, counter/owner change, new relation, grant or public attachment. Existing historical rows therefore retain NULL clocks.
- The repeatable has one private SECURITY INVOKER function with fixed `pg_catalog,private` search path, logical owner `postgres` and direct EXECUTE revoked from PUBLIC/anon/authenticated/service_role. Its only data mutation is the three NEW fields. It has no table read, auxiliary lock, monetary/helper call, admission operation or post-clock counter. The sampled server `clock_timestamp()` is part of the actual row's transaction.
- INSERT overrides supplied clock fields with actual creation and only actual initial LEASED/DONE/FAILED observations. UPDATE first restores all three OLD values, so recorded clocks cannot be changed or erased by a caller. Both missing start and finish gates explicitly require `OLD.created_at_utc IS NOT NULL`. A historical row cannot acquire a misleading original start/finish through later discovery, reclaim or completion.
- Start requires OLD not LEASED and NEW LEASED; finish requires OLD not DONE/FAILED and NEW DONE/FAILED. Cursor-only updates, queue recovery wake updates, LEASED→LEASED reclaim, terminal replay and timestamp-only requests cannot overwrite/create a clock. Actual job BLOCKED remains resumable. A terminal job with phase BLOCKED still correctly records its DONE transition.
- No start-before-finish constraint was introduced: READY PREPARE can legitimately expire without ever being leased (`04102026_0510:125–132`). New captured jobs then have creation/finish but NULL start; historical jobs remain NULL. The trigger changes neither status authority nor owner replay rules.
- The attachment is only BEFORE INSERT/UPDATE on `private.bpay_next_job`, with DROP IF EXISTS/CREATE in the repeatable. Sealed pre-change contract has zero triggers on that table; existing immutable revision/effect/policy guards attach elsewhere. There is no current competing job trigger/order issue and no existing attachment is removed.
- Registry contains exactly one matching signature `private.bpay_next_job_lifecycle_clock_v1()`, the exact 0352 finalRoot/finalPhysical/allowedPhysical/finalClosures, and an empty predecessor set. It does not make an immutable migration the final function owner.
- Existing `%rowtype` readers and explicit service JSON/TABLE outputs retain their fixed contracts. The financial service's `j.*` internal read adds no duplicate clock-derived aliases and does not emit a whole job row. No response/Worker authority change is needed for these private supporting columns.

## Count coverage and boundaries

The existing `applied_line_count` is a genuine existing cumulative business count, including position apply (`03102026_1430:559–580`), cancellation release (`04102026_0260:303,435–436`) and expiry release (`04102026_0510:485`). Existing owner-specific progress counters and finite `rows_visited` receipts remain authoritative; this migration/repeatable introduces **no duplicate count** and does not repurpose attempts as rows.

BD025 correctly retains unknown/not-applicable per-step rows as NULL. Ordinary return/PAYE and CLAIM receipts do not uniformly expose a generic visited-row count; their own completion timing comes from their actual job kind and new lifecycle clocks, not a manufactured universal count. D02 must retain this distinction and preserve the queue's closed outputs/ACK/retry. The supporting D01 source is complete within the stated census/three-column scope; it is not a new per-step ledger or proof that future FIFO/Pay Query contention/browser-latency checks have run.

The checklist's installation/app boxes remain unchecked in the source read. All future joined PF-06 checks remain NOT_RUN. Historical absence, Worker elapsed time and actual DB execution duration must keep their documented meanings.

## Exact reviewed hashes

| File | SHA-256 |
| --- | --- |
| `supabase/migrations/10102026_0352_banking_pay_next_job_lifecycle_clocks.sql` | `96ba93b380b7fdd1aef712dd54c3a9fbdc51b890384a26d8217922ea16c76420` |
| `supabase/repeatable/10102026_0352_banking_pay_next_job_lifecycle_clock_v1.sql` | `3ef9116ca77508cc1f419a2b7e209ed4a5e729e00f3b66de98d5bd40305ba0bc` |
| `docs/bpay-next/final-definer-closures.json` | `bdf64c0e3918fc4e88dc57b9477119749fa4a45a1972e714bac870997d96e9bf` |
| `docs/bpay-next/design/PF-06-SCOPE.md` | `1799cb0680cefc7bf63e6bafd27eafe8a3f6d28dac21db1643195d71a4c90681` |

Hash verdict applies to these saved bytes and the exact supporting implementation reviewed, not every unrelated registry change in the shared worktree.
