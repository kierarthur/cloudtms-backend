# Independent Review Brief - Candidate Daily Phase 2 and Phase 1B R8

## Verdict requested

Issue separate evidence-backed verdicts for:

1. Phase 2 source design;
2. Phase 2 TEST installation;
3. Phase 1B private Worker integration;
4. Phase 1B public broker integration;
5. preservation of accepted Phase 0/R7 and existing Candidate core/Office boundaries;
6. readiness to begin Phase 3 only.

Do not infer feature activation, app-UI completion, production authority, real communications/effects or legacy retirement from a GO.

## Pack intake

1. Verify `MANIFEST.sha256` and `MANIFEST.sizes` before reading conclusions.
2. Confirm `PROVENANCE.md` GitHub branch heads and deployed Worker versions independently.
3. Treat the R5/R6/R7 materials as historical/cumulative authority and R8 documents/PDF sections as later-controlling only where explicit.
4. Confirm no pack document depends on a local filesystem path or unavailable chat context.

## Mandatory source review

Inspect at minimum:

- `supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql`;
- `supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql`;
- the amended bootstrap in `07082026_2108_candidate_app_read_and_missing_week_rpcs_v1.sql`;
- `broker/src/candidate-daily-phase1b.js`;
- `broker/src/candidate-daily-contract-v1.js`;
- private/public dispatch changes;
- merged R8 OpenAPI;
- the Phase 2 PostgreSQL runtime suite;
- complete/focused JavaScript evidence.

Search the repository for duplicate qualified Daily table/function/cursor/mode owners. There must be one authority, not test-only or dead-code duplicates.

## Mandatory database verification

Against exact TEST, read-only:

1. verify the project identity and PostgreSQL version;
2. verify the migration/repeatable ledgers and source hashes;
3. enumerate all twelve Daily tables, RLS, policies and direct grants;
4. enumerate all thirteen Daily RPC signatures, security-definer state, search path and role grants;
5. verify the single bootstrap overload and Daily capability helper binding;
6. verify all thirteen Candidate flags are false;
7. verify all seven Candidate core and twelve Daily tables are empty;
8. verify no Candidate-bound mail/effect/business row exists.

Do not mutate rows to prove these points. Use disposable PostgreSQL for execution paths unless a new explicit TEST-mutation scope is granted.

## Mandatory adversarial journeys

Independently exercise on PostgreSQL 17.6 and 18.1:

- source-link exact mapping, missing/disabled/ambiguous/mismatched source;
- Candidate identity cannot be request selected;
- publish a complete fourteen-day generation and reject partial/stale/mismatched generation;
- exact Candidate command replay and changed-input conflict;
- exact legacy command replay/status and changed-input conflict;
- batch publication replay/conflict;
- parallel projection claims, lease mismatch and retry/park terminality;
- durable/effective cursor divergence, exact deferred overlay proof and retreat/requeue;
- all valid and invalid authority mode transitions;
- effect claim/concurrent lease/complete/status/lost-response replay;
- Candidate flag/entitlement/mode/generation/freshness gate;
- signed-system continuity independence from the Candidate product flag;
- strict public response allowlisting and internal-field leakage resistance;
- bootstrap deep baseline preservation.

## Google/minimal legacy review

Confirm R8 contains no Google write. Re-read the Google Evidence Gate and verify the later Phase 3 plan remains minimal:

- preserve the legacy browser/UI/login and `msisdn` behaviour;
- add only a server-side signed compatibility adapter;
- never expose CloudTMS/Supabase/HMAC authority to the browser;
- retain mixed per-date legacy response compatibility;
- add Master Rota dual publication without removing its Availability publication;
- keep Emergency/specialist functions compatible with both clients;
- do not equate legacy browser retirement with Availability/Emergency/Master retirement.

## Shared-state qualification

Workflow `31981114093` must not be reported as a complete safe-migration PASS. Confirm from raw evidence that it:

1. applied the Candidate prerequisite migration;
2. applied the Candidate Daily repeatable;
3. stopped only at the three declared pre-existing James definition hashes.

Do not repair, revert or catalogue-normalise James/Banking Pay inside a Candidate audit. Instead, independently validate the Candidate installed subset and report the shared-workflow qualification precisely.

## Stop rule

If all mandatory journeys pass and no concrete supported blocker remains, issue GO for Phase 2 and Phase 1B R8 and stop. The next authorised implementation phase would be Phase 3, not feature activation.

If NO-GO remains, provide one complete bounded finding with exact reproduction, affected operation family, required correction and mandatory regression proof. Do not broaden into legacy modernisation, finance, Office, Banking Pay or unrelated architecture.
