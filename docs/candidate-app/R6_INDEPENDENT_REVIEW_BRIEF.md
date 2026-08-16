# Independent Review Brief — Candidate Daily Phase 1A R6

## Review question

Determine whether the R6 source implements the complete accepted Phase 1A dark transport and policy authority without changing accepted Candidate business authority, Google behaviour, financial authority or production state.

This is not a request to approve activation or the completed Candidate App.

## Integrity first

1. Extract the archive into a new directory.
2. Run `tools/validate_candidate_daily_r6_pack.py <package-root>`.
3. Independently verify both top-level manifests.
4. Confirm the nested `baseline_r5/` manifests still validate.
5. Verify the 70-page Decisions PDF is readable and contains R5 plus R6 Sections 57–64.
6. Verify the merged OpenAPI and HMAC fixture hashes recorded in the handover.

## Controlling authority order

Where interpretation differs, apply this order:

1. the combined R6 Current Decisions PDF;
2. the R6 AV-155–AV-180 matrix;
3. the complete accepted R5 decision ledger/matrix under `baseline_r5/`;
4. the sole merged R5 OpenAPI contract;
5. the implementation authority and Google Evidence Gate;
6. source and executable tests.

Do not use superseded legacy implementation behaviour to override the settled decisions.

## Mandatory source checks

Independently inspect the production modules, not merely string-presence tests:

- route catalogue is exactly 24 new operations, with exact methods, paths, access policy and operation IDs;
- existing bootstrap members survive unchanged and only Daily capability is added;
- unlisted Daily paths and wrong methods fail closed;
- Candidate routes are reachable only after normal Candidate authentication and remain disabled;
- signed system routes reject Origin/Cookie/Authorization and cannot acquire browser authority;
- public broker never sees an HMAC secret or owns replay state;
- signed method/path/query/headers/raw body reach the private verifier without semantic rewriting;
- HMAC canonicalization matches every accepted vector, including negative raw-parser cases;
- only PRIMARY/OVERLAP key slots are accepted and misconfiguration fails generically;
- timestamp, body digest, signature and nonce checks occur in the settled order;
- nonce key path, atomic create-if-absent behaviour, retention and replay result are exact;
- Candidate correlation may be valid-or-generated; signed correlation is mandatory and never replaced;
- reads forbid idempotency, commands require it and bodies cannot supply a second key;
- body limits/framing/content-type/encoding/BOM/JSON-object rules are exact;
- rates are keyed by Candidate session or system key ID as appropriate;
- 10/12/20-second service deadlines are actually applied;
- no isolate-local counter is falsely presented as distributed in-flight authority;
- all business routes remain dark and perform no business mutation/effect.

## Mandatory regression checks

Confirm the complete 576-test backend suite remains green and inspect the focused 13 tests for false positives. Re-run the exact accepted R5 HMAC and source-identity Node/Python vectors. Re-run the R5 pack validator and runtime/OpenAPI parity check. Re-run the three Worker dry builds from the supplied source boundary.

## Google/legacy review boundary

The Google gate is evidence, not a Google patch. Check that:

- the legacy browser/UI/login/`msisdn` contract is not changed;
- no HMAC/Candidate/Supabase authority reaches the browser;
- the later compatibility adapter is server-side and minimal;
- Master Rota deployed-vs-Head drift is explicit and not silently normalized;
- `ai_startDailyPings` is not revived;
- Emergency/Master consumers remain protected as coexistence dependencies;
- unredacted source and property values are absent from the package.

## No-change boundary

Reject scope drift into:

```text
existing Candidate authentication/session/workflow/Office business RPCs
Candidate frontend or native apps
Google Apps Script source, browser, Sheets, triggers, OAuth or deployments
DAILY/WEEKLY financial calculations
rates, pay, charge, VAT, ERNI, margin, TSFIN
Process, Authorise, invoices, payments
Banking Pay, Policy X, provider, settlement, remittances
production
```

## Required reviewer report

Return:

1. package integrity result;
2. source baseline and changed-file verification;
3. decision-by-decision disposition for AV-155–AV-180;
4. any regression against the 154 R5 decisions;
5. route-by-route and policy-by-policy result;
6. HMAC/nonce/correlation/idempotency/limit result;
7. Google/minimal-legacy boundary result;
8. test/build reproduction result and any limitations;
9. explicit safety/no-change confirmation;
10. final **GO for Phase 1A source only** or bounded **NO-GO**.

If GO, the next authorised activity is Phase 2 source authoring and disposable PostgreSQL verification only. TEST SQL installation, Phase 1B, Google editing/deployment, UI work, feature enablement and production remain separately gated.
