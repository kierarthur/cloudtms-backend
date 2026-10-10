# PS-02 occurrence/business-date investigation

10 October 2026. Read full plan06 PS-02:1353–1375, exact checklist records, approved policy PSP-15/18, current schema/guards and final producer authorities. Source-only; no SQL, tests, fixtures, runtime, network, secrets or product edits.

The proposed two stored generated dates fit PS-C02's finite schema change. They preserve the actual UTC occurrence and independent ingestion clocks; they add no public input, provider state, payment date inference or money owner.

## Exact objects

D01 is one new scaffolded business-date migration on the existing facts, not the entire generic 1324 table inventory. D02 confirms/reuses settlement receive/post and claim; D04 retains its installed predecessor provenance while confirming current final source. D03 is historical core CONTEXT only. D05 is the saved future joined checklist, not an executed scenario. Dependency F-05 remains; Worker/screen changes are absent.

## Existing clock authority and producers

| Existing actual path | Fact and atomic boundary |
|---|---|
| `private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamptz,uuid)`; final 2018:2191 | Validates finite reported occurrence and exact positive amount; INSERTs immutable SETTLED outcome with `occurred_at_utc=p_occurred_at`, omits ingestion so the server transaction default applies, then creates linked outcome request and updates transfer SETTLED in the same function transaction. Exact receipt replay compares transfer/kind/amount/original occurrence and returns the existing fact. |
| `private.bpay_next_receive_simple_csv_return_v1(uuid,uuid,text,numeric,timestamptz,uuid)`; final 0230 | Validates finite occurrence and earlier SETTLED predecessor; INSERTs distinct RETURNED outcome with reported occurrence/server ingestion, linked return request and RETURNED status atomically. Receipt replay retains both original clocks. Later return-cash posting is not the return's bank date. |
| `private.bpay_next_receive_internal_settlement_v1(uuid,uuid,uuid)`; final 0490 | INSERTs unique zero-cash internal receipt with both clocks defaulting to server transaction time, links request and updates INTERNAL_PROCESSING atomically. Guard requires both clocks to equal transaction time. Later bounded INTERNAL_SETTLED completion does not rewrite the receipt's acceptance occurrence or create a bank outcome. |
| `private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)`; final 10102026_0316:341, build final 0240 | Accepts original return-cash lineage once and builds a cash-only leg; creation/request completion is not paid. Its actual subsequent SETTLED/RETURNED outcomes own their own dates, using the same outcome table. No new earnings event or original paid-date replacement is introduced. |
| Existing cancellation lifecycle 0260/0460/0355 | REQUESTED/CANCELLING/CANCELLED/BLOCKED request/status progress is not a bank CANCELLED cash fact. EX-16 owns the combined SCHEDULED/CANCELLED schema; EX-27 owns the named `PS_C02_BANK_CANCEL_TRANSITION_V1` producer. These future owners are explicitly deferred, not inferred from a request finish timestamp. |

The current final settlement post function is 2018, while claim remains 0200. Its bounded posting and immutable economic effects may occur later than bank occurrence. That is already correct; no 0200 successor body is needed merely to add a generated date. The actual transfer-outcome INSERT census currently consists of settlement and return writers (0200's historical definition is superseded by 2018). SUBMITTED/UNKNOWN/REFUSED enum entries and later SCHEDULED/CANCELLED schema are not producer delivery. CSV issuance is a retained instruction with its own issue clock, not a paid outcome.

## Stored date expression and PostgreSQL scope

Use `business_date date generated always as ((occurred_at_utc at time zone 'Europe/London')::date) stored`. The input is already a stored timestamptz. The fixed-zone timestamptz-to-timestamp conversion and timestamp-to-date cast are immutable PostgreSQL operations appropriate for a stored generated expression; there is no session-TimeZone dependence, clock call, text timestamp parsing, table read or caller-supplied date. The resulting UK calendar date handles midnight/BST through the named zone and leaves the stored UTC timestamp unchanged. The reviewer did not run a physical PG17 catalog/apply probe; root's permitted schema compile remains the installation evidence.

PSP-15 makes SETTLED occurrence the paid date and RETURNED occurrence the return date, never received time or financial-effect time. PSP-18 requires UK local display and preservation of stored UTC. The generated date is a persisted projection of that original authority, not a new business event. The internal receipt's date is zero-cash accounting occurrence and must never be described as a bank paid date.

## Schema/security/contract implications

1324 already gives transfer outcomes NOT NULL occurrence/ingestion and server-owned ingestion default; supported writers omit ingestion and validate finite reported occurrence. Its receipt uniqueness and append-only 0200 trigger preserve replay facts. The core outcome occurrence itself has no finite CHECK, so the proposed finite generated-date CHECK is narrow protection rather than a new timestamp input. 0480 internal receipt already requires both clocks finite, non-NULL and received>=occurred; 0490 rejects all UPDATE/DELETE and clock tampering at INSERT.

Adding generated NOT NULL finite dates preserves those guards: they reject all fact UPDATE/DELETE regardless of column list. No guard reads the generated field in BEFORE INSERT, when it is not yet computed. No trigger change is needed. Outcome/private owner and application-role revokes are already present in 1324; internal receipt additionally explicitly enables RLS in 0480. Preserve existing security state; do not claim that a new date column itself establishes RLS or installation parity.

All current fact writers use explicit INSERT lists, so generated values are computed without owner-body changes. Current history sections explicitly enumerate occurrence/received fields rather than serializing a whole table row. Closed public request/reply shapes, source/money terms and cursor identities remain unchanged. Later Payment Summary readers can select the new date under their own scoped cards. A stored column may materialize from old immutable occurrence during migration; this is not a guessed backfill from mutable state.

No missing clock in the supported SETTLED/RETURNED/internal fact paths requires a new producer. Future cancellation/provider-state completeness remains with its named cards and PS-12 producer register. This investigation confirms current supported facts and honest future scope, not all enum channels or every request lifecycle as append-only bank events.

Key source SHA-256: 1324 `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c`; 0480 `032fcf4d3456b66c67c49a6399ee6a13207e223cb37b5d5fff0de3439e70f70b`; 0200 `fa6bac8c799387f91717375219af885f65b334274bdb139a048f46e90e3a98fc`; 0230 `0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d`; 0490 `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c`; current 2018 `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`; current 0316 `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6`; policy `65f31c6726926ec6f68141d438c3f690433a767f7ce1f6a8818054f045349497`.

Only this investigation document was written by the reviewer.
