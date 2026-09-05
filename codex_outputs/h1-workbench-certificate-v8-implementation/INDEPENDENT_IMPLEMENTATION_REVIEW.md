# H1 Workbench recovery and settled certificate V8

**LOCAL IMPLEMENTATION CANDIDATE — NOT INSTALLED — NOT DEPLOYED — NOT A WORKBENCH RECOVERY PASS**

## Review target

Review backend implementation commit `f6abc37d71879e204ea8dba11900c385c07c2959`, tree `80b3b08aafc0a424e84712fa561a4196ef781a02`, which contains exact shared TEST base `16b299bf03c3d752f6dbd7891c0d23044e88e890`. Review frontend commit `6ec5f56cf236c43ad1864ae44514d1d634230aae`, tree `af79cbbe4cdf433d569a9a85f987160a41460377`, which contains exact frontend base `278dcbd007bb3cb6a13d09b5fe0d8aaf5132a5be`.

The H1 implementation has two responsibilities:

1. preserve and complete the already-proved Workbench recovery rules; and
2. build a normalized, bounded, server-owned settled Workbench certificate after the accepted Workbench session is ready.

H1 does not calculate Draft outputs. It supplies immutable selected constituent, partition, source and expected pre-Draft facts. H2 remains responsible for the Create Draft route, frozen Draft staging, readiness phases, economic-owner calls, cleanup, terminal publication and constituent parity.

## Policy boundary

There is no payment-policy change. Eligibility, selection, identity, amount, sign, pay method, gross/net treatment, tax, VAT, payee, recovery, headroom, reservations, cancellation, provider, settlement and final payment outcome remain owned by the existing routines. Policy X is preserved. The new operational safety gate temporarily disables Create Draft only while the exact current certificate is absent or still building; missing, stale, failed or tampered evidence cannot start a Draft.

## Exact local evidence

- V8 function catalogue: 39/39 exact definitions, owners, ACLs, security mode and configuration.
- Merged generated contract: `fcf3895ad455d3e5fff66476c45542171261a9edf053670dfe22a617525aea70`; file SHA-256 `711c9caf6cef0951ce0a2e1c0ba35fadff790239c34f22af9ae8106fad34714f`.
- Contract difference against the pre-H1 merged database was exactly four routines: changed build-start and operation-admit plus new status and due-claim functions. No relation, enum, trigger, policy, ACL, signature, owner or security delta appeared.
- `db:check`: 212 migrations and 518 repeatables PASS.
- PostgreSQL 17 current merged contract comparison PASS and rollback-contained V8 first use PASS.
- PostgreSQL 18 exact H1 changed-definition identities and direct runtime fixture PASS. The final H1/H2 combined NEW/UPGRADE replay remains an H2-L16 release gate rather than being inferred from earlier evidence.
- H1 backend source suite: 18/18 PASS. Merged frontend boundary: 6/6 PASS. Newly merged Candidate boundary: 151/151 PASS. Exact H2 control audit: 10/10 PASS.
- The actual H1 Worker producer through local PostgREST sealed 5,000 constituents in 79 append RPCs and 131 durable queue deliveries in 160,591 ms. It ended `SEALED_CURRENT`, left no successor message and suffered no timeout or lost continuation. The allowed maximum remains 50,000; 5,000 is only the user-selected practical pressure size, and 50,001 remains a deterministic pre-write rejection.
- H1 recovery mutations: 18/18 killed, zero survivors.

No repeat of the 5,000-row pressure run was performed after the final status/manifest-only edits because those edits cannot enter the producer continuation path. Instead, the changed status source, PostgreSQL 17/18 definition hashes, 39-function catalogue and affected frontend/backend tests were rerun. This is an explicit impact-based decision, not missing evidence.

## Complete H2 lifecycle audit

`H1_CREATE_DRAFT_LIFECYCLE_PRODUCER_AUDIT_V1.json` assesses the exact current H2-L01 through H2-L16 checklist. It is bound to:

- H2 lifecycle contract `b13b5be38eeee54c587d26605c86015b08fbac7a39124b826c3136798a336727`;
- H2 lifecycle test `210898e32078b1c80dc8095ede6dd0fc0e5c1c3b34b4f805881a3a85672ad87c`;
- policy contract `3952c019426334a6c04b568226d019fc915f635d2411b7a295229c188beef42c` covering 15 families, 88 finite classes and 17 stages.

Every H1 producer boundary is examined. H1 source/direct-RPC evidence closes its own admission, compact projection, certificate build/page/seal, channel partition and durable producer responsibilities. It does not falsely close H2 consumer work.

## Known H2 consumer failures independently reproduced

The current H2 source is not ready to consume this producer:

1. its V8 helper is not activated by the Worker dispatcher;
2. it does not page distinct Timesheet IDs from frozen typed rows and therefore cannot safely run `DRAIN_TSFIN` for more than 100 Timesheets;
3. it omits `ENSURE_PAYEE_READINESS`;
4. it does not consume typed empty-shell, failure and terminal results through the existing owners;
5. its parity routine serializes but does not enforce several expected allocation, item and reservation facts, and can demand a reservation before respecting deliberate `NOT_APPLICABLE` policy;
6. it has not yet proved the complete Draft continuation and all downstream artifacts for the 15-family/88-class contract.

These are H2-owned fixes. H1 will not silently edit them or alter payment policy. H2 must correct them, then H1 and H2 must each rerun the same shared boundaries.

## Current installed boundary

Fresh read-only H2 connector evidence identifies Miget TEST `cloudtms_test_clone`, PostgreSQL 17.11, latest release `20260822-test-authority-upgrade-16b299bf03c3`. The H1 causal-recovery closure is installed, but V8 is not. The local producer is therefore not yet available to normal TEST, no populated certificate instance exists, and no installed Worker/frontend identity can be claimed.

## Verdict requested

Approve or reject the H1 producer only. If approved, H1 can be published and installed to Miget TEST under the user's existing TEST authority; a fresh Workbench session must then be forced and monitored through scope build, certificate sealing and browser adoption. H2 may begin installed integration only after that succeeds and H2 has corrected the consumer failures above.

Current mandatory statement: **HANDOVER 2 DEPENDENCY REMAINS UNAVAILABLE**.
