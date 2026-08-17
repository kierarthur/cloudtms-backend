# Candidate Daily Phase 3 R12 independent review brief

## Required disposition

Perform a fresh, complete operation-level re-audit of the R12 Phase 3 coexistence boundary. Do not limit review to the three R11 findings, but do not broaden into unrelated Candidate Office, finance, Invoice, Banking Pay, Policy X or production.

Issue one of:

- `GO for the controlled Phase 3 enabled TEST proving gate`; or
- one bounded `NO-GO` with reproducible evidence and one complete targeted correction handover.

Do not infer Candidate feature activation, Phase 4 authority or production authority from a Phase 3 GO.

## Read order

1. `00_HANDOVER.md`;
2. `01_INDEPENDENT_REVIEW_BRIEF.md`;
3. `02_CURRENT_STATE.md`;
4. `03_VERIFICATION_SUMMARY.md`;
5. `PROVENANCE.json`;
6. the 109-page Decisions PDF;
7. Phase 3 authority, compliance matrix, installation and rollback runbooks;
8. complete Availability and Master source plus both certified rollback files;
9. the self-contained Phase 3 test and canonical HMAC fixture;
10. raw focused/complete/dry-build/live-install evidence;
11. the complete incoming R11 independent review artifacts.

Verify both manifests before trusting any payload.

## Mandatory R11 closure reproduction

### Deferred write

Prove the busy/deferred browser branch makes no CloudTMS call, creates no bridge operation and preserves its existing response. Then prove the flush owner:

- revalidates the rows;
- writes Google values/backgrounds first;
- mirrors only successfully written rows;
- releases the legacy lock before network work;
- never mirrors rejected, superseded or failed rows.

### Mixed result

Run all accepted, all rejected and mixed accepted/booked/blocked/outside-window/validation cases. The signed factual body must equal only the durable accepted non-deferred legacy subset. All rejected must perform zero identity, state, log and network work.

### Response disposition

Independently exercise:

```text
COMMAND_IN_PROGRESS / STATUS_CHECK
SOURCE_IDENTITY_NOT_READY / STATUS_CHECK
IDENTITY_LINK_MISSING / STATUS_CHECK
AVAILABILITY_VERSION_CONFLICT / REFRESH
IDEMPOTENCY_KEY_REUSED / DO_NOT_RETRY
malformed or unknown 409
```

Every non-terminal or malformed result must preserve the exact operation. Approved terminal triples may clear only as documented.

### Lost response

Run:

```text
execute uncertain
status exact 404
one exact retry uncertain
status uncertain
three later status refreshes
```

Assert the entire request body, request UUID, key, correlation and source HMAC are identical and the total execute count never exceeds two.

## Mandatory broader Phase 3 review

- missing/false flag hard no-op at every source seam;
- legacy browser/login/msisdn and response-shape containment;
- HMAC vector and environment-bound source identity;
- exact nine public broker routes and private service-binding translation;
- Availability tile overlay retains legacy-only and Emergency facts and fails open;
- Master publication remains legacy-first, update-end-only, fourteen days and maximum fifty candidates per batch;
- projection defers booked/system-blocked overlays;
- effect helpers remain unwired primitives;
- no new trigger or `ai_startDailyPings` owner;
- complete rollback and retained previous Google deployments;
- no secret values in source/evidence;
- no database/financial/production drift.

## Mandatory self-contained pack test

Extract the archive to a new empty directory and run:

```text
node --test tests/candidate-daily-phase3-apps-script.test.js
```

Required result:

```text
18 passed
0 failed
```

The test must use only packaged files and the packaged canonicalization fixture.

Also rerun against the full repository:

```text
node --test tests/candidate-daily-phase1a-contract.test.js tests/candidate-daily-phase1b-contract.test.js tests/candidate-daily-phase2-source-contract.test.js tests/candidate-daily-phase3-apps-script.test.js
npm test
```

Supplied baseline:

```text
Focused 54/54
Complete 632/632
```

## Installed-state qualification

Confirm the active Google versions and source identities without changing them. Confirm the bridge property is false by presence/state evidence only; never read the two secret values. Confirm prior deployments remain available for rollback and no trigger, manifest or scope changed.

Do not send a signed route, create a source link, mutate Candidate data or enable any cohort during source re-audit. Those actions belong to the separately approved enabled proving gate after GO.

## Stop rule

If all mandatory journeys pass and no concrete supported blocker remains, issue GO and stop. Do not gold-plate the temporary legacy app or require a legacy browser authentication redesign.
