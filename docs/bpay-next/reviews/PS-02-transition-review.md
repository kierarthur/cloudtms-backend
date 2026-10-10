# PS-02 stored business-date and producer-reuse review

10 October 2026. **PASS for the saved D01 schema, D02/D04 source reuse and D05 authored acceptance subset; no remaining concrete defect found.** Read against the full PS-02 card/checklist, current final-definer registry, predecessor core/internal schemas, actual settlement/return/internal/cancel/reissue source and BD032. No SQL, runtime, test, business function, fixture, database or hosted call invoked. This verdict does not establish installation or app/queue/financial acceptance.

The new `10102026_0544_banking_pay_next_ps_c02_stored_business_dates_v1.sql` adds only `business_date` to the two actual immutable fact tables: `private.bpay_next_transfer_outcome` and `private.bpay_next_internal_receipt`. Both are GENERATED ALWAYS, STORED, NOT NULL and finite DATEs computed from that row's own occurrence through explicit `Europe/London`. They do not depend on session TimeZone, ingestion, run pay_date, a mutable source, economic-effect time or request/job completion. NULL cannot pass the finite check because the column also has NOT NULL. No new RPC, trigger, grant, producer, finance counter or money calculation is introduced.

BD032 correctly identifies this as a calendar projection of the already admitted occurrence, rather than a new independently supplied bank-date operand. Existing rows already contain their actual immutable occurrence; calculating its fixed London calendar day neither manufactures an event nor changes their occurrence/ingestion. An invalid historical occurrence fails finite-date validation rather than receiving a guessed date. This resolves the convention left open by the investigation; its optional nullable/no-backfill alternative is not a remaining requirement for this accurately derived column.

For actual SETTLED/RETURNED outcomes, occurrence owns the reported paid/return day; server-default received_at_utc independently records ingestion. Internal-zero receipt occurrence/date is accounting acceptance and is explicitly not a bank payment date. Existing outcome UPDATE/DELETE denial and internal INSERT-only receipt guard protect the generated fields alongside their occurrence. Explicit generated-column value injection is unavailable. Private table ownership, RLS and existing revocations carry through these additions. Server ingestion through current receive paths is already sufficient: payloads cannot supply received_at_utc, and INSERT omits it. This review does not claim a stronger unrestricted owner-write protocol.

## Reused owners and facts

The entire current files retain the exact investigation hashes, and Git reports no changes to them:

- Settlement receive and bounded post remain final2018; CLAIM remains0200. Receive writes the immutable SETTLED fact/request and transfer state atomically. Exact receipt replay retains its original occurrence/ingestion/identity and returns the original request; conflicts refuse. Later posting and completion do not overwrite its date.
- Return0230 atomically retains its separate RETURNED fact and reported occurrence, preserves the real SETTLED predecessor, and posts/reopens the existing cash obligation in command order. Reissue construction retains original cash lineage; only its own eventual settlement gives that leg a paid date, with no second earnings event.
- Internal0490 atomically inserts the genuine server-clock receipt/request and INTERNAL_PROCESSING transition. Its original guard enforces both exact server timestamps and immutable receipt identity; bounded posting later completes INTERNAL_SETTLED without creating an external outcome.
- Current cancellation owners do not produce a CANCELLED outcome. BLOCKED request completion does not cancel cash. Simple/Case cancellation can change the transfer before later release completion, so request-finish timestamps must never substitute for the event. EX-16/27 ownership remains explicit.
- Current CSV issue retains immutable instruction/issued time, not a claimed CSV_ISSUED transfer_outcome producer. Provider SCHEDULED/SUBMITTED/UNKNOWN/REFUSED remain unavailable until their actual owners exist. The new column adds no such producer or enum availability.

The exact D05 note keeps all installed/app/business/Worker/queue/minute clauses NOT_RUN. It covers distinct yesterday-occurrence/today-ingestion, UK midnight/BST and session independence, immutable replay/conflict/rollback, separate return and reissue dates, internal-zero semantics, blocked cancellation, absent provider producers and generated-column/ACL refusals. Future EX cancellation schema and facts are not marked delivered. No manufactured acceptance receipt or additional local scenario programme appears.

## SHA-256 reviewed

| Source | SHA-256 |
| --- | --- |
| new0544 migration | `7537d120fb13c61b78b06a77e5697065152529f4c7ad911bdf7e83653fd5ce5b` |
| unchanged final2018 settlement receive/post/intake | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| unchanged0200 CLAIM / outcome immutability attachment | `fa6bac8c799387f91717375219af885f65b334274bdb139a048f46e90e3a98fc` |
| unchanged0230 return | `0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d` |
| unchanged0490 internal receipt/settlement | `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c` |
| `PS-02-SCOPE.md` | `5adddf2a3a6d8b36bf02e704277585cab1a64f4f11674e178373c2efbfdea1d9` |
| `PS-02-JOINED-ACCEPTANCE.md` | `3e2ad439bb3371b92edc997e6e025c3626ea5734cfb77a5e028b84426dd9a29d` |
| registry | `83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc` |
