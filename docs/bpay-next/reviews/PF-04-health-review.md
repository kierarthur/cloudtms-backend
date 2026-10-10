# PF-04-D02 health and D04 future checklist source review

10 October 2026. **PASS for the saved D02 private health reader/service wrapper and D04 future joined checklist scope. No actionable source defect or substantive card mismatch found.** Independent source-only review; no SQL, runtime, tests, fixtures, network or hosted execution and no product changes. This is not installation or application acceptance and does not close PF-04's cleanup, minute wiring or history gate deliverables.

Read the full PF-04 card (`06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1741–1762`), all five current checklist rows, `design/PF-04-SCOPE.md`, the existing source/count/cleanup census, the full `10102026_0403_banking_pay_next_operational_health_v1.sql`, both new registry entries, actual core job/work-hold/case-hold definitions, and the PF-06 lifecycle clock authority. The named private signature is implemented directly; the small public wrapper is the required service transport, not a new financial owner.

## Five measures and their actual meanings

| Required measure | Saved implementation and assessment |
| --- | --- |
| Queue age | Actual unfinished READY/LEASED/BLOCKED jobs, count, oldest known creation age and separate unknown-created count. Empty/all-unknown creation age is JSON NULL. No age is inferred from availability, due time, lease or a historical row's missing creation clock. If some clocks are unknown, the reported oldest **known** age remains qualified by that explicit count. |
| Error counts | Current jobs with stored `last_error_code`, grouped by bounded `BPAY_NEXT_` uppercase/digit/underscore code; other stored text becomes the fixed `UNCLASSIFIED` bucket. No error message, detail, hint or payload is returned. This reports jobs carrying a code, not an invented cumulative failure/attempt counter. Existing cancel/expiry writers retain their own fixed business refusal codes. |
| Oldest active hold age | UNION ALL of actual ACTIVE work holds and ACTIVE case holds, using their existing nonnull timestamptz creation fields. REALISED/RELEASED holds are excluded. Empty active scope yields NULL, not zero. No amount or financial target is selected. |
| Latency | Complete DONE/FAILED jobs with actual PF-06 start and finish clocks and nonnegative elapsed time, grouped by stored job kind and terminal phase. Explicit `measurement=JOB_FIRST_START_TO_FINISH`, sample count, average and maximum milliseconds make this truthful job lifecycle duration, including pauses/reclaims where present. Unknown history/unstarted expiry is excluded. The comment correctly identifies individual RPC timings as PF-06's six-field Worker logs; this is not mislabeled per-RPC or isolated DB execution time. |
| Table sizes | Catalog-only enumeration of ordinary/partitioned `private.bpay_next_%` relations and `pg_total_relation_size(oid)`. Fixed private schema/prefix excludes public application tables and unrelated schemas. It returns relation names and actual relation total bytes, not monetary/admission figures or business rows. |

The JSON result has exactly the five named top-level measures. Counts, elapsed values and sizes are textual numeric representations, preserving existing SQL numeric/bigint precision; absence uses JSON NULL or an empty array. Aggregate aliases/types and the selected job/hold/catalog column names match actual source. `min` applies only to timestamps, and no ROWTYPE projection, UUID aggregate or qualified special-form construct is introduced.

## Read-only security and closure

- Both routines are STABLE with fixed `pg_catalog,private` search path. The private reader is SECURITY INVOKER, with one statement timestamp reused across age calculations. Its body contains only SELECT/aggregate/catalog reads and the returned JSON; no mutation, lock/admission helper, history reader, processing price, command, current financial input refresh or external effect.
- The public wrapper is SECURITY DEFINER and first checks the existing service JWT role representation, then actual singleton module ownership NEXT. Its only subsequent call is the exact private health reader. Missing/wrong ownership fails with the existing typed module error; browser roles fail with the fixed service-forbidden error. On-demand monitoring does not acquire financial admission authority or alter owner state.
- Both logical owners are `postgres`, preserving the repository's audited provider mapping. Direct EXECUTE is revoked from PUBLIC, anon, authenticated and service_role on the private reader. Public wrapper EXECUTE is revoked first and then granted only to service_role. No table grants, RLS changes, public trigger or new schema are introduced. Reload NOTIFY follows definitions/owner/ACL operations with COMMIT as the tail.
- Registry has exactly one entry for each zero-argument signature, with 0403 as finalRoot/finalPhysical/allowedPhysical/finalClosures and no predecessors. The wrapper's actual private dependency is defined in the same closure file. There is no parked F-25 resolver/producer/PIN/enroller call or Rule-B scope change.

The card requires latency from PF-06 identities/timestamps; its delivered lifecycle authority provides exactly the job duration used here, while the Worker records individual calls. An additional persistent metric ledger or full-row/history snapshot is neither implied nor introduced. The other PF-04 deliverables and all future full Worker/queue/minute/application proofs remain separate and NOT_RUN by this reviewer.

## Fresh aggregate cast closure and D04 checklist

Re-read the full current 0403 source after its finite F-23 typing change. Every timestamp `min` argument now explicitly casts the existing timestamptz field to timestamptz; maximum duration explicitly casts the already numeric EXTRACT-derived expression to numeric. These casts preserve NULL, precision, age/elapsed meaning, grouping, output keys, security, ACL and read-only behavior. There is no timestamp reinterpretation, UUID aggregate or financial successor change. Both health registry entries remain unchanged in the latest registry bytes. D02 source PASS therefore applies to the updated hash below.

Read `PF-04-JOINED-ACCEPTANCE.md` in full. D04 source scope **PASS**: it retains the year-of-unrelated-history preparation time/money comparison, actual admitted Rule-B inputs, negative routine-history source controls, bounded terminal cleanup with financial/audit origins retained, active/contended/leased/incomplete/PREPARE/FK retention, truthful late temporary-metadata refusal, all five health measures, unknown NULL clocks/ages and sensitive-output/ACL negatives, plus the actual four recovery lanes and fifth cleanup lane under NEXT. It explicitly requires later pages to advance past retained rows and separates job lifecycle latency from actual Worker call timings.

All checklist rows and full Worker/queue/minute/history/application paths remain **NOT_RUN**. The document reuses a later owner-authorized joined TEST receipt, supplies no scenario runner/fixture or present data-creation authority, requires no completed UI or new screen, and keeps the sole separately governed phase-3 300-Timesheet timing check separate. OR-45 correctly removes old Banking tick compatibility work. D04's future assertions are acceptance clauses, not approval of any still-separate D01 deletion predicate or D05 implementation.

## Exact reviewed hashes

| File | SHA-256 |
| --- | --- |
| `supabase/repeatable/10102026_0403_banking_pay_next_operational_health_v1.sql` | `2d79f5c0e14a9937a51ee2f0a798370c11d46e9894a61933b1c8457d7b46d4b1` |
| `docs/bpay-next/PF-04-JOINED-ACCEPTANCE.md` | `8ec385562b1ba116d4f3cac143f7658235a0240420825a817246e09f75202dd4` |
| `docs/bpay-next/final-definer-closures.json` | `06b12aca8673b4d5b113684e21c744ba92d29618de43b70e585b71042cab3309` |
| `docs/bpay-next/design/PF-04-SCOPE.md` | `8c6779d1f9bcc05c5db772dc8cc20a959771f4c04ce857526d456957c929ac8c` |

The registry verdict covers the two health entries, not unrelated shared-file changes. These hashes bind source review only; no local compile, service transport or live timing result is inferred.
