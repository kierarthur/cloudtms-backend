# F-25 QuerySync true-completion source review

Reviewed 10 October 2026. **PASS for the three-function completion assembly and exact registry slice; no concrete source defect found.** The full public QuerySync writer remains pending. No SQL, runtime, tests, fixtures, hosted operations or product edits were performed. This is not an installed/runtime result, whole-F-25 completion, or evidence that the actual public body has reached its business tail.

Reviewed source SHA256:

- `supabase/repeatable/10102026_0059_banking_pay_next_query_sync_true_completion_v1.sql`: `08d6e2310e383644a15beb78fd890c378a02957e0cbefa13faf2b1190fdb2136`.
- `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql`: `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb` (previous schema review).
- `supabase/repeatable/10102026_0043_banking_pay_next_query_sync_completion_digests_v1.sql`: `79e3787728d2550a423f9552cd3b4e3b670c292e4715607b0a792ee9ff9f77eb` (previous digest review).
- `supabase/repeatable/10102026_0051_banking_pay_next_query_sync_certificate_evidence_v1.sql`: `bd9920f0440406a7710f713391db25646ebc475fa906d7faac6fad03aa133768`.
- `supabase/migrations/10102026_0057_banking_pay_next_query_witness_envelope_v1.sql`: `121eafa88611f7bd79c24ee5d2325f1077b6afa43d7e4ceec57a92323695b958`.
- `supabase/repeatable/10102026_0058_banking_pay_next_query_sync_completion_guards_v1.sql`: `8027ed7810e0345b4370a897551ddca3fae0868119728253ab9fa4abe5b09ce8`.
- Supporting exact stage closure0047: `d8f0e2f689eeb22ba480f039318a01b90187dbec5fbe098190f12428155b059e`.
- `docs/bpay-next/final-definer-closures.json`: `b6ea0a8d1b78c4a8fd0e7776b2cb37882136cd286df76e5fb7908912ccc997a0` at this registry read.
- QuerySync design: `e78e73c208b29aef2e12ae4531959e70d74e38846c92c6b1221ac42128a0714c`.

## Typed supplied arrays and complete original sets

The helper takes actual named SQL table-composite arrays, not anonymous JSON records. `unnest(p_inputs) r` / `unnest(p_omissions) r` exposes the exact0036 composite fields; inserted `r.*` and digest arguments use those named layouts. The stored expected-witness wrapper aggregates actual table row composites and explicitly casts canonical empty arrays to the matching named array types (0059:27–31). It does not return an array of witness records or a different anonymous tuple to the collector.

0059:45–57 requires the producer's current xid, BUSINESS_OPEN, STANDALONE_QUERY, exact QUERY_SYNC root stack, absent completed witness, and matching OPEN input transaction. Nonempty composite/seen/omitted arrays must be one-dimensional with lower bound1. Composite sets are individually capped at1000; NULL sets and NULL seen/omitted IDs refuse. A NULL composite element cannot disappear: the unnest aggregate counts it, the distinct mandatory work/incident count fails, and its completion UUID cannot equal the owned ID. Required row fields and branch-specific NULL shapes additionally meet the0036 constraints on insertion; exceptions roll back the whole helper, with no success masking.

0059:58–74 derives native-UUID-sorted work IDs and aligned complete issue hashes from every supplied input row, and actual original counters from the finite outcome values. It compares those exactly with the pure complete requested arrays/counters, actual sorted `p_seen_work_ids`, and sorted supplied omissions against `p_omitted_ids`/original resolved count. Duplicate mandatory identities cannot pass the distinct counts. Every child's completion UUID and recomputed row SHA must match. The canonical0038 request digest validates actor/cycle/publication, full aligned hashes, required SHA lengths, bounded distinct sorted requested IDs and exact request SHA. Tuple `IS DISTINCT FROM` comparisons reject NULL/mismatched supplied counters or arrays without boolean UNKNOWN acceptance.

The future original public body must pass `p_omitted_ids` captured from the entire real omission selector before changing those incidents, then independently assemble actual omission composites/results. Likewise `p_seen_work_ids` must be the actual original loop's seen array, not a copy invented from requested IDs. The helper cannot independently reconstruct an earlier mutable selector or prove that the caller executed restart/outreach/audit. Its authority is the private same-xid true-tail invocation in that sole full original owner. This obligation remains explicit until the public successor is reviewed.

## Immutable assembly, guards and exact proof closure

0059 computes raw actual stages and checks `3*NEW+2*CHANGED+omissions` before insertion. All counts are derived from bounded arrays, so untrusted supplied bigint counters do not enter an unchecked multiplication. The later0047 assertion validates the complete join/transaction scope and a two-way expected-operation/stage bijection; plausible totals alone cannot qualify unrelated stages.

Header fields, input/omission ID digests, both canonical child-set SHAs, complete original witness count/SHA and final0043 completion SHA are all computed before the single immutable header INSERT (0059:78–102). The input/omission aggregate domains, counts, native identity order and canonical `[]` empty serialization match0043. No placeholder header or UPDATE is used. Completion time is the actual transaction timestamp, matching the existing0032 header insert guard.

Header, child and witness inserts occur before any agency clock. Existing0032 header/witness guards require the actual current xid, matching NEXT epoch, BUSINESS_OPEN/input OPEN and absent completed witness.0058 child guards additionally require the exact QUERY_SYNC root and actual incident/work/current-comparison/episode scope, legal branch lifecycle and matching row digest. Actual FKs are inserted before finish, not deferred to a postclock reader. The final call0059:108 goes through0058's complete root assertion: exact producer/cause/header/digest;0051 child set/counter/request/original-event qualification and no input/omission overlap;0047 full raw-stage closure; complete canonical original witness manifest/count/digest. Partial inserted rows or mismatched hashes cannot return successfully.

## Shared witness collector and finite envelope

0059:6–21 preserves0051's six-branch `UNION` manifest exactly: every current input comparison, CHANGED prior comparison, omission comparison, NEW/CHANGED SOURCE_RECHECKED, UNCHANGED_RESOLVED decision, and omission SOURCE_MATCHED. `UNION` deliberately deduplicates the exact kind/UUID identity, not whole incidents or arbitrary publication events. Finite kind rank plus native UUID orders `row_number()` contiguously; every original evidence SHA comes from the current0030 helper.

The stored expected collector at23–32 delegates to this same supplied collector, eliminating a separate manifest interpretation. Both header calculation and actual witness INSERT call it, and0058 subsequently compares actual stored witnesses against that same canonical manifest. Valid original evidence is immutable. There is no LIMIT, missing-evidence filter, exception catch, invented event or silent duplicate substitution. A missing/wrong original witness raises instead of truncating.

The helper caps the exact distinct witness count at4000;0057 permits matching header counts0–4000 and ordinals1–4000 without increasing input/omission caps or canonical stage capacity. Zero children give count0, literal JSON `[]` witness SHA and canonical empty child-set SHAs. An unchanged input still carries its comparison/optional historical decision witness, while having no material stage.

## Empty completion and pending public finish

The zero-issues/no-omissions case passes the exact empty requested/seen/omitted arrays and zero original counters, retains raw count0 with NULL first/last ordinals, and inserts a genuine empty immutable completion header. All-unchanged cases preserve their actual supplied input certificates and witnesses while raw stages remain0. Neither path creates a fake SOURCE_RECHECKED, command, sequence or job in0059. The function returns the completion SHA and leaves the producer/input transaction open for the actual outer owner alone to call finish after its entire business tail.

The existing0002 standalone finisher's `actual_count=0` branch validates the completion witness and seals the own producer/input lifetime before returning without receive_command.0059 does not call that finisher, so public wiring remains required. A NULL producer returns NULL solely for the unchanged non-NEXT original path; it does not insert metadata. Nested Source/Candidate owners do not acquire this standalone QUERY_SYNC header under another root identity.

All three exact signatures are private SECURITY INVOKER, postgres-owned, pinned-search-path and revoked from PUBLIC/anon/authenticated/service_role. The pure manifest readers are STABLE and the writing helper uses normal VOLATILE PL/pgSQL semantics. Registry0059 is the sole final root/physical/closure for all three; expected-witnesses retains0051 as its exact predecessor, while supplied-witnesses and complete are new signatures. No public writer/trigger is installed by this repeatable.
