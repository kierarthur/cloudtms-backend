# F-25 QUERY producer protocol schema review

9 October 2026. Independent source-only review of unlocked/uninstalled `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` against the transaction protocol and build-chunk designs, existing 2153/2225/2244 migrations, original Source receipt/core schema and the sealed current-contract schema evidence. No SQL, runtime, tests, fixtures, hosted reads or product edits were executed. The whole QUERY producer protocol and full F-25 remain incomplete.

Initial reviewed SHA-256: `56a1019d51f0d4db8dc8a701c3311567a33d102d9de5c6d4c3df5c48ab4a7467`. Fresh 186-line source after QP01/QP02 fixes: `332c4a33cf3a9eb0e2b91b4bc833f49fa2f9115ca27db4f8c5e7869ee3d82647`.

**The intermediate checkpoint required QP03 and QP04 corrections. All four findings are closed by the fresh finite schema verdict below.** INSERT/finalizer/receiver/enroller integration described below remains pending, as expected for this schema-only batch.

## QP01 — owner-stack root CHECK accepted an unknown result — closed

The original stack CHECK allowed a nonstandard lower bound, such as a one-element array indexed `[2:2]`. Its cardinality was one but `owner_stack[1]` was NULL, allowing the root equality to become UNKNOWN and pass CHECK. The fresh constraint at line 27 explicitly requires one dimension, lower bound one and `(owner_stack[1]=owner_kind) IS TRUE`, retaining the finite element allowlist and absence of NULL elements. The initial current-xid INSERT guard requires exactly one stack element. No further concrete stack-shape defect was found; finite nesting edges remain a helper obligation.

## QP02 — entered producer mode could change — closed

The original UPDATE immutable tuple omitted `mode`, permitting STANDALONE_QUERY to become SOURCE_PUBLICATION within the same entered owner. The fresh tuples at lines 137–138 include mode on both sides, so the original owner decision is retained. A recovered later qualification may receive its later metadata command under the finisher's genuine `is_fresh_receive` proof; it must not rewrite the entered mode.

## QP03 — prepared reservation can be reset after the clock

**P2, lines 136–142.** The operational guard pins identity and forbids phase regression, but it does not pin `reserved_range_start/end` or make `next_reserved_ordinal` monotone. A TAIL_PREPARED row can currently replace/clear all three fields or move its cursor backward while retaining TAIL_PREPARED, even when its parent transaction is CLOCK_SEALED. Each individual replacement still satisfies the table's range shape CHECK. This loses the preclock reservation certificate that is supposed to bound actual later completion events without updating the terminal transaction.

Allow the initial all-or-none reservation assignment only through the genuine current-xid preclock preparation state while the parent is OPEN. Once a range is present, retain its start/end and forbid clearing/backward cursor movement; cursor allocation must consume the exact next reserved ordinal under the owned producer. The later staging helper still has to validate the exact registered relation/row/callback manifest. This finding does not propose reopening or updating CLOCK_SEALED input_transaction.

## QP04 — Source member does not bind its original publication tuple

**P2, lines 78–88.** Separate Candidate, WORK, revision and composite command FKs prove that each referenced object exists, but permit a member to combine WORK A, revision B and command C from different real publications. The sealed current contract already exposes the exact unique key `private.bpay_next_publication(command_id,work_id,revision_id,candidate_id)`; accepted WORK input uses that same key. Bind the Source member's four columns with the corresponding composite FK. A `(work_id,revision_id)` FK to existing work_revision `(work_id,id)` is also available, though the publication tuple already provides that transitive exact relation.

The actual source-event/root/version/source-kind/receipt-array match, genuine all-or-none freshness probe, original deterministic command derivation, one/two member census, WORK-ID order and completion witness still require the planned owned INSERT/finalizer assertions. Adding this FK is a structural tuple guarantee, not permission to discover or lock an unrelated publication after the clock; its real targets must be preheld by the original owner.

## Confirmed schema boundaries

All seven new private tables enable RLS and revoke all direct PUBLIC/anon/authenticated/service-role rights. The owner-only operational producer rejects DELETE/TRUNCATE, cross-transaction mutation and any UPDATE once FINISHED, and retains monotone phases. The six durable association/header/group/member/manifest tables reject UPDATE/DELETE/TRUNCATE through the existing accepted-evidence immutable guard. There is no policy exposing them to browser/service roles. The new trigger function is SECURITY INVOKER with fixed `pg_catalog,private` search path and revoked direct execution.

The referenced PKs and composite keys exist: input_transaction xid8; query_input_version event_id; Source publication receipt id; candidates id; timesheets timesheet_id; WORK id; work_revision id; command `(id,agency_sequence,module_epoch)`; and the new producer/group composite keys. Source member root version is a retained positive bigint, while the original receipt's member-root versions are integer[]; comparison must preserve the exact original values rather than refresh today's mutable timesheet version.

Range nullability is explicitly all-null or all-present with bounded ordered endpoints/cursor. Completion witness nullability is likewise all-null or all-present, with nonnull finite kind/UUID/32-byte digest. FINISHED iff finished timestamp is present is NULL-safe because phase is NOT NULL. Source group CURRENT_HEAD iff original receipt ID is present is NULL-safe because origin_kind is NOT NULL. Boolean old/new member scope flags are NOT NULL and require at least one true. Required count/ordinal/hash/key/enum fields are nonnull and bounded. Header actual count allows reservation gaps rather than inventing an event for every ordinal.

No finance counter, money/hold/net journal, personal prose/contact/credential payload, historical backfill, business-row mutation, agency switch or command-clock allocation is introduced. The migration creates no members/jobs/workers/publications or metadata command. A nonempty header is represented structurally, but zero-job command/enrollment behavior is not implemented or proven here.

## Explicit remaining integration dependencies

The batch does not yet supply entry/nesting/stage/finish, actual original Source group producer, query hooks, typed INSERT validators, deferred completeness assertion, QUERY_INPUT_VERSION receiver or zero-member enrollment. Those owners must enforce event→producer exact current xid/epoch, event-source association→same producer, copied header cause/witness→original producer, actual Source member count and exact retained source-event/root proof, mode-dependent original Source versus later metadata anchoring, same-transaction event→receipt completeness, terminal parent behavior and canonical typed hash verification. The present individual FKs and digest lengths do not prove those semantic equalities or actual preheld locksets.

In particular event_source_member currently has separate event/member FKs without producer equality, and event_owner has separate event/producer FKs without same-xid equality. Close those in the planned exact INSERT/deferred guards or add typed composite FK columns before treating them as retained protocol proof. Do not interpret this schema checkpoint as a finished producer, source-clock closure, installation, zero-job enrollment or full F-25 PASS.

## QP03/QP04 correction and fresh finite verdict

**PASS for the additive schema subset at saved SHA-256 `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1`.** QP01–QP04 are closed; no remaining concrete schema defect was found in this finite review.

QP03 is closed by lines 137–144: initial reservation assignment requires old BUSINESS_OPEN, new TAIL_PREPARED and the exact parent xid/epoch still OPEN. Once present, endpoints are immutable and the cursor cannot become NULL or move backward. The table's all-or-none and bounded-cursor CHECKs remain in force. Exact cursor consumption and registered target coverage still belong to the upcoming owned staging helper; the parent CLOCK_SEALED latch is not reopened or modified by this migration.

QP04 is closed by the new line 86 composite FK to the existing publication `(command_id,work_id,revision_id,candidate_id)` key, which also binds the WORK/revision pair transitively through the original publication FKs. The separate composite command FK retains exact sequence/epoch and the group FK retains exact epoch. Actual original source-event/root/member-array/certificate/census proof remains an explicitly pending owned validator, not something inferred from UUID existence.

Fresh stack shape, entered mode immutability, range and publication changes were reread from saved source and the fresh raw hash recorded. All previously reviewed private ACL/RLS/append-only/current-xid/no-history/no-financial-job schema boundaries remain the finite reviewed scope. No SQL, runtime or tests were executed. This PASS does not close the future INSERT/finalizer/receiver/enroller dependencies above, the whole query producer protocol or full F-25, and does not certify installation or business behavior.
