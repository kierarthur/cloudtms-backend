# Independent review brief — Candidate Daily Phase 3 R17

Perform a fresh bounded software-assurance review of the exact packaged R17 source. Live Google Apps Script access is not required. Treat packaged Google version/trigger/disabled-switch facts as operator evidence and the complete unredacted Apps Script files as proposed source authority.

## Mandatory review questions

1. Does the effective later repeatable own exactly the existing eight-argument `candidate_daily_authority_transition_atomic_v1` signature and preserve its security, result, receipt, transition, entitlement, approval, generation, cursor and grant contracts?
2. Before any Candidate authority-scope row lock, does it collect only syntactically valid source identities, deduplicate them, sort them globally and acquire the exact R16 `environment:SOURCE:keyVersion:hmac` advisory lock namespace?
3. Is the source-key version range checked as text before any cast, so malformed input remains an indexed `VALIDATION_FAILED` item rather than a whole-batch error?
4. Does `IDENTITY_LINK_CONFLICT` become an indexed per-item `REJECTED` outcome without rolling back valid siblings or the batch receipt?
5. Does a completed mixed result store one terminal body/hash and return the same body with `_idempotent_replay=true` on exact replay?
6. Do changed-content reuses of the idempotency key still conflict?
7. Does the actual generation function racing the actual transition function avoid `40P01`, uncontrolled abort and partial data under forced overlap?
8. Do opposite-order multi-item transition batches avoid deadlock because source locks are sorted independently of request order?
9. Are existing no-source, ordinary cutover, rollback, reconciliation and independent-approval rules unchanged?
10. Does the exact ordered chain run on both PostgreSQL 17.6 and 18.1 before the migration job is permitted to start?
11. Are R16 normalized CID1 ownership, all-history source-HMAC ownership, first-generation atomicity, R14 aggregate response handling, disabled bridge inertness and no-manual-bootstrap retained?
12. Is the no-change boundary respected: no Worker/route, Google source, frontend, finance, Banking Pay, payment, provider or production change?

## Mandatory adversarial journeys

- single transition item rejected by protected source history;
- valid then conflict mixed cohort;
- conflict then valid mixed cohort;
- exact same-key replay and changed-content conflict;
- malformed source-link key version and HMAC;
- no-source-link transition;
- actual generation versus actual transition with the same Candidate/source/scope and forced overlap;
- two transition batches with the same two source identities in opposite input order;
- complete inherited R16 identity and ordinary controlled dual-consumer journeys;
- exact PostgreSQL 17.6 and 18.1 ordered chains.

## Evidence rule

The pack records exact R17 PostgreSQL 17.6 and 18.1 success. Independently inspect the engine proofs and rerun them where available; do not infer success merely from the summary. A final technical GO for publication requires both exact R17 engine chains to remain green after any rebase or source change.

## Requested disposition

Issue GO only if no concrete supported blocker remains and all mandatory engine/journey gates pass. GO would authorise only a later separately approved, collision-checked TEST publication/install and disabled Google qualification. It would not enable either bridge, enable Candidate Daily, create an entitlement outside the accepted transition, authorise Phase 4 or permit production.

If a defect remains, return one complete bounded correction handover rather than disconnected addenda.
