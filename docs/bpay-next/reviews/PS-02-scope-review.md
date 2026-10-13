# PS-02 independent scope/source review

**PASS for D01, D02/D04 exact current reuse and authored D05 future joined checklist.** Reviewed static saved source on10 October2026. No product edits, SQL/runtime/tests/fixtures, hosted reads or execution. All actual installed/app/full Worker/queue/minute/business acceptance remains NOT_RUN.

Binding card: normal R3 `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1353–1371`; exact current PS-02-D01/D02/D04/D05 and shared D03 context; PSP-15/18 in approved R3 check/addendum Payment Summary policy. Reviewed builder decision BD032 in `design/PS-02-SCOPE.md`, current0544 migration and final readable `PS-02-JOINED-ACCEPTANCE.md`. Required scope is the existing supported transition-fact/date confirmation; no Worker/Screen or newly invented producer belongs to this card.

| Required row | Independent result |
|---|---|
| D01 new business-date migration | PASS:0544 adds native DATE `business_date GENERATED ALWAYS AS ((occurred_at_utc AT TIME ZONE 'Europe/London')::date) STORED NOT NULL`, with finite-date CHECK, only on actual immutable transfer_outcome/internal_receipt tables. The migration retains UTC occurrence/ingestion fields, original defaults, identities and all financial schema. Table comments distinguish SETTLED paid date, RETURNED return date and zero-cash accounting date. |
| D02 current0200 receive/claim/post reuse | PASS: exact actual receive and post winners remain2018, claim remains0200. Generated metadata needs no receiver API argument, redundant successor, new command kind, callback or posting calculation. Original transaction/replay and financial behavior stays intact. |
| D04 installed baseline context and preservation | PASS: historical0200 source remains unchanged and retains outcome immutable trigger attachment; the source reuse correctly identifies2018 successors rather than reinstalling/restoring old0200 receive/post bodies. Current source delivery does not claim an updated hosted installation. |
| D05 authored future joined acceptance | PASS: final seven clauses cover yesterday's reported occurrence/today's received time, London midnight/BST/session independence, separate economic posting clock, immutable exact replay/conflict, transaction rollback, return/original/reissue separate dates, genuine internal receipt and zero-cash semantics, blocked request vs cancelled cash, EX-16/27 and unavailable provider-state boundaries, generated-field immutability/explicit injection refusal and security. Every real joined clause remains NOT_RUN. |

## Date meaning and generated-field safety

PSP-15 explicitly defines paid date from the SETTLED outcome occurrence and return date from RETURNED occurrence, with received time restricted to History and no use of financial_effect occurrence as bank date. PSP-18 chooses UK local calendar display while retaining stored UTC. BD032 therefore consistently stores the London calendar projection of each already retained immutable occurrence. It does not claim an independent reported date operand that the current command/RPC does not carry. The fixed zone, timestamptz→timestamp conversion and date cast do not depend on session TimeZone, ingestion, request/job completion, a later economic post or today's workflow.

Current outcome1324 stores nonnull occurred_at_utc and server transaction-default received_at_utc. Current settlement2018 and return0230 explicitly validate/report occurrence and omit received_at when inserting. Existing receipt replay compares the actual original occurrence/amount/identity and returns the existing fact; no clock is reassigned. The outcome immutable BEFORE UPDATE/DELETE trigger in0200:347–350 calls the existing effect_immutable guard, protecting all fields including the generated business date.

Internal0480 receipt clocks are both nonnull finite server transaction defaults.0490 insert guard additionally requires both equal the actual transaction timestamp and rejects any later UPDATE/DELETE. New date is derived from that actual receipt occurrence; it is a zero-cash accounting day and never a bank paid date. PostgreSQL's generated column is written from the immutable occurrence rather than a supplied new date value; existing guard predicates do not depend on reading this generated field before generation. No guard rewrite is necessary.

The generated stored date also projects pre-existing retained occurrence facts deterministically; this is not invented historical bank-state evidence or a new occurrence event. The added finite CHECK matches all actual current validated producers. Unknown historical nonfinite data would fail migration validation rather than receive a guessed date; source review does not establish current hosted contents or local compilation. No historical clock or financial datum is overwritten.

## All five producer areas, without availability overclaims

| Producer area | Actual current same-transaction/replay boundary |
|---|---|
| Settlement | Final2018 receive2191: SETTLED INSERT2277–2280, linked request and status SETTLED update2281–2285 in one transaction. Original receipt collision/replay2238–2250 retains both clocks. Current0200 claim and2018 post operate on that original request/fact; later financial posting cannot retime it. |
| Internal receipt |0490 receive86 creates one actual internal_receipt141–142 and linked request143–144 before INTERNAL_PROCESSING145 in the same transaction. Guard ensures genuine original zero-cash receipt and both server clocks; later complete_internal_zero154 validates actual finished member posting before INTERNAL_SETTLED238 and retains original occurrence. |
| Return |0230 receive5: original receipt replay44–52; RETURNED INSERT69–71, linked return request72–74 and transfer RETURNED75 atomically. Each return retains its own occurrence/day; original settlement paid day remains unchanged. |
| Reissue | Current0316 receive341 and0240 builder retain exact return/original lineage and produce one cash-only leg, with request replay. Creating/requesting/building that leg is not paid. Its own later supported SETTLED receipt uses the same atomic settlement fact/date authority. No new earning or payment event/date is manufactured at request completion. |
| Cancel | Current0260/0460/0355 own real request/progress/blocked dispositions. They do not INSERT the required append-only CANCELLED transfer_outcome. Card expressly assigns combined SCHEDULED/CANCELLED schema to EX-16 and actual PS_C02_BANK_CANCEL_TRANSITION_V1 to EX-27. PS02 records those pending owners and does not infer cancellation cash/date from request.finished_at. BLOCKED remains a request disposition, not cancelled cash. |

The source registry census has actual external outcome writers only settlement2018 and return0230 plus internal receipt0490. This is five investigated producer **areas**, not five currently completed outcome producers. SUBMITTED/SCHEDULED/REFUSED/UNKNOWN enums remain unavailable until their true producers are delivered and tested. CSV issued retains instruction issue time and no paid date. The future combined outcome CHECK and cancellation producer are neither replaced nor prematurely built by0544. No additional producers are warranted in PS02.

## Source hashes

| Build repository relative source | Raw SHA-256 |
|---|---|
| supabase/migrations/10102026_0544_banking_pay_next_ps_c02_stored_business_dates_v1.sql | 7537d120fb13c61b78b06a77e5697065152529f4c7ad911bdf7e83653fd5ce5b |
| docs/bpay-next/design/PS-02-SCOPE.md | 5adddf2a3a6d8b36bf02e704277585cab1a64f4f11674e178373c2efbfdea1d9 |
| docs/bpay-next/PS-02-JOINED-ACCEPTANCE.md | 3e2ad439bb3371b92edc997e6e025c3626ea5734cfb77a5e028b84426dd9a29d |
| supabase/migrations/03102026_1324_banking_pay_next_core.sql | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| supabase/migrations/04102026_0480_banking_pay_next_internal_zero_execution.sql | 032fcf4d3456b66c67c49a6399ee6a13207e223cb37b5d5fff0de3439e70f70b |
| supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| supabase/repeatable/04102026_0200_banking_pay_next_simple_csv_settlement_v1.sql | fa6bac8c799387f91717375219af885f65b334274bdb139a048f46e90e3a98fc |
| supabase/repeatable/04102026_0230_banking_pay_next_simple_csv_return_v1.sql | 0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d |
| supabase/repeatable/04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql | 88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c |
| supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql | 98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6 |
| supabase/repeatable/04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql | 5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714 |
| supabase/repeatable/04102026_0260_banking_pay_next_simple_cancel_v1.sql | d10f3468e3fa6f39bc20b9e792f1d228259e5d234d433397cf5f552b9588135f |
| supabase/repeatable/04102026_0355_banking_pay_next_reissue_cancel_v1.sql | 3f31e48aef1629bac6dea37cd2a40a1e2dd0b5806031971b0c5e8334f4873c55 |
| supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql | 4417b25f96d6caefb627eae013c0c956268356104be15e2411dead56cc60bcf5 |
| docs/bpay-next/final-definer-closures.json | 83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc |
