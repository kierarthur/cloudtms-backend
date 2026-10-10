# F-25 QuerySync completion digest source review

Reviewed 10 October 2026. **PASS for the four digest functions and their registry entries; no concrete defect found in this source slice.** No SQL, database, runtime, tests, fixtures, hosted calls or product edits were performed. Current-xid insertion guards, original owner qualification and the full QuerySync true-tail successor remain pending. This is not a finalizer, installation, behavioral verification or whole-F-25 verdict.

Fresh sources:

- `supabase/repeatable/10102026_0043_banking_pay_next_query_sync_completion_digests_v1.sql`: SHA256 `79e3787728d2550a423f9552cd3b4e3b670c292e4715607b0a792ee9ff9f77eb`.
- Original header digest in `supabase/repeatable/10102026_0030_banking_pay_next_query_completion_evidence_v1.sql`: SHA256 `4be599c83fe18bb3b8761fecebb3e0a9785d2d69a716b83225787f8cc431dd54`.
- Corrected branch schema `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql`: SHA256 `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb`.
- `docs/bpay-next/final-definer-closures.json`: SHA256 `bafa6dae95835d05635e66e5019622c87bf8337b01702389e449ca8352153bfb` at the bounded registry read.

## Header compatibility and noncircular binding

0043:13–22 retains the complete original `BPAY_NEXT_QUERY_ROOT_COMPLETION_V1` SHA expression, with identical field order, bytea hex encoding and UTF8 JSON-array serialization to0030:66–75. A read-only source string comparison found the two expressions identical after removing whitespace. SYSTEM_HOURS_ACCEPT takes `else base.digest` directly at0043:13, so its previous digest value and meaning are preserved for identical valid typed headers. It receives no wrapper or extra child field.

QUERY_SYNC alone uses the separate `BPAY_NEXT_QUERY_ROOT_COMPLETION_QUERY_V1` wrapper at0043:9–12, binding the original base SHA and both0036-required child aggregate SHAs in a fixed order. The original witness aggregate keeps its comparison/event meaning. Header `completion_sha256` and completion timestamp remain excluded as in0030, so neither the resulting proof nor a later producer completion reference hashes itself. The schema explicitly requires both QUERY child SHAs and forbids them on System Hours headers; this pure digest does not purport to enforce schema or owner authority on an arbitrary composite argument.

## Typed child rows and complete sets

0043:26–32 binds every semantic input-certificate column in the design's fixed order: completion UUID, work UUID, full issue SHA, outcome, incident, captured episode, current/prior comparison, recheck/resolved decision, narrow matched-facts SHA. The row's own `evidence_sha256` is correctly excluded.

0043:34–39 binds every semantic omission-certificate column: completion UUID, incident UUID, original work UUID, captured episode, comparison and original resolve event. It likewise excludes its own proof SHA. All fields use actual0036 names/types. No current mutable incident state, timesheet/source mapping, policy, contacts or business data is read to recreate these records.

0043:41–54 recomputes both complete stored child sets for exactly `completion_id=p_completion_id`. Each aggregate counts actual rows, uses its separate domain, and orders native UUID identities in `jsonb_agg(... ORDER BY uuid)`: input by work UUID, omission by incident UUID. The corresponding0036 primary/unique keys prohibit tied identities within one completion. There is no limiting, state filter, inner evidence join or digest-match filter that could silently discard a row. Each manifest uses the recomputed typed row digest rather than trusting its stored self-proof. Empty aggregates normalize to literal JSON `[]` with count0, and the two one-row aggregate subqueries yield a canonical empty result even when both relations have no child rows.

The eventual qualification helper must still require every stored row's `evidence_sha256` to equal its recomputed digest, counts/UUID sets to equal the immutable header's complete original selection, and both aggregate values to equal the header before invoking finish. A canonical digest of an absent/empty set is a pure value, not proof that a real completion exists or that a no-op root is genuine. Exact supplied input/omission branch qualification, historical resolution predicates and true-tail census remain the original owner's pending responsibility.

## Definitions and registry

All four functions are private `SECURITY INVOKER`, with a pinned `pg_catalog,private` search path, postgres ownership and all EXECUTE privileges revoked from PUBLIC/anon/authenticated/service_role (0043:55–62). The three typed value functions are IMMUTABLE; the actual child-set reader is STABLE. There is no data mutation, clock/command call, job, business locking, finalizer or caller/trigger attachment. `CREATE OR REPLACE` plus exact-signature owner/revoke statements support repeatable reapplication.

The registry at12797–12811 correctly makes0043 the sole final header-digest root/physical/closure while retaining0030 as the predecessor and allowed historical definition. The three new exact signatures at12974–13013 point only to0043 with empty predecessors. All four definitions are physically present in the declared source; no historical file, signature alias or migration root substitutes for the controlling repeatable.
