# Independent Review Brief - Candidate Daily Phase 1A R7

## Review question

Does R7 close all nine bounded findings from the independent R6 review while preserving the accepted R5 API/decisions, dark Phase 1A boundary and all no-change domains?

## Mandatory source review

Read in this order:

1. `R7_HANDOVER.md`;
2. the independent R6 audit in `audit_inputs/`;
3. the 76-page R7 Decisions PDF;
4. the R7 compliance matrix;
5. the merged R5 OpenAPI and R5 decision matrix;
6. all seven changed production/config files;
7. the focused tests and shared Node/Python vectors;
8. raw evidence logs and deployment evidence;
9. manifests and package validator output.

## Mandatory adversarial journeys

- Exact 25-operation error matrix including bootstrap.
- Missing `message`, untyped details and unknown private fields.
- Private 4xx/5xx drift and unexpected success schema.
- Origin/native/preflight 403 paths.
- Rotating unknown HMAC key IDs against pre-auth throttling.
- Nonce age at 599/600 seconds under positive and negative accepted skew.
- Missing, invalid and valid signed correlation IDs.
- Candidate and system declared/actual byte mismatch.
- Eight query canonicalisation vectors in Node and Python.
- Duplicate/ambiguous headers through a real Fetch Request/Headers boundary.
- Regression of all accepted R5 HMAC/source-identity/route-policy rules.

## Required no-change proof

Confirm no change to Candidate business tables/RPCs, Office Candidate, frontend, Google Apps Script, Sheet data, triggers, finance, invoice, payment, Banking Pay, Policy X, provider, settlement, remittance or production. Confirm Candidate Daily remains dark and Phase 2 has not started.

## Stop rule

If every mandatory journey passes and no concrete supported blocker remains, issue GO for corrected Phase 1A and stop. A GO is not Phase 2 or later-phase authority. If a blocker remains, give one bounded complete correction handover; do not broaden into unrelated architecture or legacy modernisation.
