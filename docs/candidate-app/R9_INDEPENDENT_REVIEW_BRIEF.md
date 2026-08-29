# Independent Review Brief - Candidate Daily Phase 2/1B R9

## Verdict requested

Issue one bounded operation-level verdict for the R9 correction to `public.candidate_daily_authority_transition_atomic_v1`, then state whether the outstanding Phase 2 authority may receive GO while the existing R8 Phase 1B transport GO remains in force.

Do not reopen already accepted Phase 0/R7/R8 families unless the R9 diff concretely changes them. Do not infer feature activation, Google editing, full-app completion, production authority or legacy retirement from a GO.

## Intake and provenance

1. Verify `MANIFEST.sha256` and `MANIFEST.sizes` before reading conclusions.
2. Verify the GitHub branch/head, implementation/evidence commits, workflow runs and deployed TEST Worker versions independently.
3. Confirm the pack contains no secrets, local-machine-only path dependency or unavailable chat-only authority.
4. Treat R5/R7/R8 documents as cumulative authority and Sections 80-84 / AV-229-AV-244 as later-controlling for transition proof.
5. Compare the packaged complete repository with the stated changed-file inventory and current GitHub source.

## Mandatory source review

Inspect:

- `supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql`;
- `tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql`;
- `tests/candidate-daily-authority-transition-concurrency.integration.js`;
- `tests/candidate-daily-phase2-source-contract.test.js`;
- `.github/workflows/candidate-db-runtime.yml`;
- the R9 correction authority, closure matrix, living decision matrix and current decisions PDF.

Confirm:

- one transition owner and no duplicate qualified definition later in replay order;
- unchanged signature, return type, security-definer state, closed search path and service-role-only ACL;
- no table/RPC/HTTP/Google/frontend/finance expansion;
- deterministic cohort scope lock order;
- every per-item record/scalar is reset;
- expected business errors become explicit item rejection while unexpected errors still abort;
- immutable ledger facts come from locked database rows.

## Mandatory disposable PostgreSQL journeys

Run the exact install/workflow on PostgreSQL 17.6 and 18.1. Then independently invoke the transition RPC for:

1. valid dark `GOOGLE_PRIMARY -> SUPABASE_PRIMARY` with entitlement false;
2. global-off rejection when enabling entitlement;
3. global-on exact entitlement enablement;
4. `SUPABASE_PRIMARY -> ROLLBACK_PENDING` only after global disable and entitlement disable;
5. `ROLLBACK_PENDING -> GOOGLE_PRIMARY` only with full parity proof;
6. exact no-op with no ledger mutation;
7. missing scope with no implicit creation;
8. missing, BUILDING/partial, stale, wrong-ID and wrong-version generation;
9. missing, ambiguous and expired/disabled source identity;
10. missing sync, non-READY sync and accepted/required/effective cursor lag;
11. reconciliation timestamp older than generation, availability or projection facts;
12. invalid deferred overlay and exact valid current overlay;
13. PENDING, CLAIMED, RETRY and TERMINAL projection work;
14. an in-progress command;
15. another in-progress batch containing the Candidate;
16. IN_PROGRESS and UNKNOWN external effect receipts;
17. a falsified caller `DRAINED` assertion;
18. caller `CANCELLED` assertion;
19. exact same-key replay;
20. changed factual reuse of the same key;
21. simultaneous exact same-key calls from two database sessions;
22. simultaneous different-key cutovers from the same expected prior state;
23. a partial cohort with one valid and one invalid item;
24. post-operation proof that no scope remains `transition_in_progress=true`.

The expected concurrency result is one durable exact replay for journey 21, and one commit plus one explicit stale-precondition rejection for journey 22. Deadlock, two commits, two independently generated results or a stuck transition fence is NO-GO.

## Mandatory installed TEST review

Read-only against `test-cloudtms` only:

1. verify project and PostgreSQL identity;
2. verify the repeatable ledger SHA-256 equals packaged/published source;
3. compare the installed canonical function-definition hash with evidence;
4. confirm one overload and unchanged argument/result signature;
5. confirm `SECURITY DEFINER`, owner, search path and grants;
6. confirm exactly twelve Daily tables and thirteen public Daily RPCs;
7. confirm all Candidate feature flags are false;
8. confirm all seven Candidate core tables, twelve Daily tables, Candidate-bound mail and effect data remain empty;
9. confirm no Google, finance, Banking Pay or production mutation occurred.

Do not create a real Candidate scope, source, entitlement, generation or transition in TEST for this audit. Use disposable PostgreSQL for business journeys unless separately authorised.

## Mandatory preserved-boundary regression

Confirm the complete Candidate database workflow remains green, including authentication/session concurrency, Candidate Office, QR/electronic lifecycle, Current/History, manager mail, PAPER/QR execution and Phase 1B contracts. Confirm all Worker dry builds and harmless deployed health/readiness/unauthenticated route checks.

No Playwright UI journey is required for this database-only correction because R9 changes no frontend asset or public presentation. Do not invent a UI claim.

## Stop rule

If every mandatory journey passes and no supported concrete blocker remains, issue GO for R9 and the outstanding Phase 2 authority, and confirm the existing Phase 1B transport GO remains in force. The next planned product phase remains Phase 3 and still requires its own Google edit/deployment authority.

If NO-GO remains, give one bounded finding with exact reproduction, affected transition family, required correction and executable regression. Do not broaden into legacy browser modernisation, Office, finance, Banking Pay, provider or unrelated architecture.
