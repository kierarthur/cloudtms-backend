# Source package v3 — joined TEST release authority

This release adopts the independently qualified pre-channel Source package agreed with HANDOVER 2. It does not activate Banking Pay NEXT, create a banking draft, execute payment/provider operations, or publish LIVE.

## Exact reviewed database authority

- Release: `20261006-source-banking-compatible-package-v3`.
- Canonical contract: `9497e4b8e2c614ee093906ca3c2d434dab1c572408dca4885e158cf70b1a9cb2`.
- Managed execution manifest: `0b2a2c22008f13e9cdd4bc6c413b593f776cb6aae72abb70df01c6b4eb31875f`.
- Inventory: 293 immutable migrations, 787 repeatables, 149 UPGRADE verifiers, 150 NEW verifiers.
- Frozen complete-input inventory raw SHA256: `3d3967ac478e59406542b68b11e0727f165d891188295de80eeb171330e79a18`.
- Qualification checks all 1,279 sealed inputs, not a moving Banking worktree. Both channel additions dated `06102026_0236` remain excluded. The original installed-writer census remains unchanged.

The only dependency-lock successor is the separately human-approved `source-map-js` 1.2.1 to 1.2.2 security correction in predecessor commit `72476632ebbb53ebc15b5eefbed12cf5675c9816`. All other frozen dependency entries and package metadata are unchanged.

## Source runtime boundary

Protected-hour Save completes the CloudTMS-owned durable local decision. It does not require an external Banking C1 START/status/publish handshake. A lost response retries the same receipt and immutable request rather than inventing another shift. The established calculator remains the money/rate/hour authority; the additive Source component projection associates exact work-event identities without repricing.

Manual queries, retained approved positions, source reconciliation, invoice isolation, and the existing Processing Delayed/Withdrawn presentation are governed by the sealed SQL. Approved Candidate display admits only positively certified HEAD, INITIAL_AUTHORISED_TSFIN_V1, or SAVED_UNAUTHORISED_LOCAL_V1 evidence. The optional NEXT paid-information consumer is read-only and refuses malformed or unqualified evidence; it is not an authorisation or Save dependency.

## Local gates at preparation

- Complete-input frozen qualifier and its two offline regression tests: PASS.
- Release-owned complete-input/manifest qualification: PASS, exact manifest above.
- Source regressions: 370 PASS, zero failures, 13 explicit environment-dependent skips.
- Backend JavaScript regressions: 1,520 PASS, zero failures, three explicit existing skips.
- Security toolchain, provenance, source credential scan and production/full dependency audits: PASS, zero vulnerabilities.

The restricted local PostgreSQL replay supplied by HANDOVER 2 passed all 149 UPGRADE verifiers and the exact full contract. This is local proof only. Hosted installation must use the canonical protected `npm run release:test` workflow, retain LEGACY activation, run the full hosted verifier/contract gates, and publish runtime targets in dependency order. Office remains held until actual Kier protected-pay Save acceptance passes.

Neither local tests nor this document claim hosted installation or browser acceptance. The coordinator and protected workflow receipts provide those exact commit-bound results separately.
