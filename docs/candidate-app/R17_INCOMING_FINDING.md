# Incoming R16 independent finding — controlling R17 assignment

Date received: 18 August 2026

## Independent disposition

The independent R16 review returned:

> **NO-GO — bounded authority-transition integration defect**

The review accepted that R16 correctly closed all six R15 findings: normalized active-CID1 uniqueness, all-history source-HMAC ownership, transaction-safe privileged installation, Master top-level identity conflict, a genuine ordinary controlled dual-consumer transition and a PostgreSQL 17.6/18.1 pre-migration workflow gate.

It found two related blockers in the one pre-existing supported source-link writer:

```text
public.candidate_daily_authority_transition_atomic_v1
```

### Finding 1 — expected identity conflict escaped the item boundary

The R16 history guard deliberately raises `IDENTITY_LINK_CONFLICT`. The authority-transition function's expected item-error catalogue did not contain that code. A protected-history conflict therefore entered `WHEN OTHERS`, missed the closed expected catalogue and was re-raised. That could roll back:

- the whole transition statement;
- otherwise valid sibling items;
- the batch receipt;
- transition, entitlement and scope changes that should have remained committed for valid items;
- the exact durable terminal replay body.

The required result is a completed durable batch with an indexed item outcome:

```json
{
  "status": "REJECTED",
  "error_code": "IDENTITY_LINK_CONFLICT"
}
```

### Finding 2 — generation and transition used opposite lock order

R16 first generation effectively used:

```text
SOURCE advisory lock
→ Candidate authority scope
```

The pre-existing Office transition effectively used:

```text
Candidate authority scope
→ source-link INSERT
→ R16 history trigger
→ SOURCE advisory lock
```

Concurrent generation and authority transition for the same Candidate/source could therefore form:

```text
generation owns SOURCE and waits for SCOPE
transition owns SCOPE and waits for SOURCE
```

PostgreSQL could choose a victim and raise `40P01`, leaving the result dependent on database deadlock selection instead of the durable product contract.

## Exact bounded correction required

The independent review required one later effective complete definition of the existing authority-transition function that:

1. preserves the eight-argument signature, security attributes, return type, batch receipts, transition policy, entitlement policy, independent approval, generation/cursor/readiness checks and grants;
2. scans transition items before scope locking;
3. derives only syntactically safe source identities;
4. deduplicates and deterministically sorts those identities;
5. acquires the exact R16 `SOURCE` transaction advisory locks first;
6. then acquires Candidate scope locks in Candidate UUID order;
7. never casts malformed source key-version text in the pre-lock scan;
8. adds `IDENTITY_LINK_CONFLICT` to the expected per-item catalogue;
9. completes and durably replays mixed committed/rejected batches;
10. changes no Worker, Google source, frontend, schema, finance or Banking Pay owner.

## Mandatory tests required

- single-item source-history conflict;
- valid then conflict mixed cohort;
- conflict then valid mixed cohort;
- exact idempotent replay;
- malformed source link remains indexed `VALIDATION_FAILED`;
- no-source-link transition regression;
- actual generation RPC versus actual authority-transition RPC under forced overlap;
- two authority-transition batches containing the same two source identities in opposite item order;
- PostgreSQL 17.6 and 18.1 exact ordered matrix;
- workflow proof that TEST migration remains dependent on both engines.

## Required no-change boundary

No change was requested or authorised to Candidate creation, the R16 indexes/trigger/binder/generation RPC, Candidate Daily HTTP routes or Worker adapters, Candidate or Office frontend, Availability source, Master legacy `Code.gs`, Google triggers, authentication, ordinary timesheet workflows, Emergency/providers, finance, invoices, Banking Pay, payments, Policy X or production.

## Immediate operational disposition inherited by R17

R16/R17 must remain unpublished and uninstalled until the correction passes independent review and the exact PostgreSQL 17.6/18.1 gates. Both Google bridge flags remain false. No source-link bootstrap, entitlement, Candidate feature enablement or Google deployment is authorised.
