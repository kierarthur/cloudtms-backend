# Candidate Daily Phase 3 R13 independent review brief

## Required disposition

Perform a fresh bounded operation-level review of the R13 Master Rota generation durability/quota correction and preserved Availability behavior.

Issue one of:

- `GO to deploy R13 and proceed to controlled population-wide Phase 3 TEST proving`; or
- one bounded NO-GO with reproducible evidence and a complete targeted correction handover.

Do not infer bridge enablement, Candidate feature activation, Phase 4 authority or production authority from a GO.

## Later-controlling product decision

The independent R12 review recommended a technical one-candidate/source-HMAC allowlist. The product owner explicitly superseded that recommendation: the first enabled TEST exercise is population-wide. Kier Arthur is the first observational phone-app journey only and must not be hard-coded.

Do not reject R13 merely because it lacks an allowlist. Instead verify:

- no candidate-specific identity exists in source/properties;
- all otherwise eligible fixture rows are handled consistently;
- the installation/proving runbook requires source-link readiness for the eligible population before enablement;
- missing/ambiguous database links still fail closed at the existing signed authority.

## Read order

1. `00_HANDOVER.md`;
2. this brief;
3. `02_CURRENT_STATE.md`;
4. `03_VERIFICATION_SUMMARY.md`;
5. `PROVENANCE.json`;
6. the R13 Decisions PDF;
7. Phase 3 authority/compliance/runbooks;
8. complete Availability and Master sources plus rollbacks;
9. both self-contained Phase 3 tests and canonical HMAC fixture;
10. raw focused/complete/dry-build/source/Google/DB evidence;
11. the complete incoming R12 independent review.

Verify both manifests before trusting any payload.

## Mandatory reproduction

### Closed response ownership

Run the exact Master helper with `BATCH_IN_PROGRESS / STATUS_CHECK`. Prove that the manifest, body chunks, batch ID, idempotency key and correlation remain unchanged and no completion log is emitted.

Run exact terminal conflict/incomplete triples and prove explicit terminal rejection without a false completion log. Run malformed/unknown 4xx, 429, 5xx and transport failure and prove retention.

### Exact pending recovery

Create an uncertain event, then invoke a later accepted legacy event. Prove the first event is replayed byte-for-byte before any new generation can be built. Repeat uncertainty at least three times and across a timestamp older than seven days.

### Multi-batch authority

Force more than one frozen batch. Prove early success plus later uncertainty does not complete the event. Prove overall completion appears only after every batch succeeds.

### Quota/state authority

Persist four and fifty minimal items. Verify no single value exceeds 7,000 UTF-8 bytes; the manifest body byte count and SHA match exact reconstruction; the complete store preflight remains below 480,000 bytes; and no POST occurs when capacity is insufficient.

Create an oversized candidate set and prove request rechunking keeps each request below 245,760 bytes and 50 items.

Corrupt a chunk/manifest/index and prove fail-closed behavior without replacement identity.

### Population-wide product decision

Use at least two eligible fixture candidates and prove both are included, no raw identity is transmitted, and no candidate-specific source or property exists. Treat Kier only as a later real-world observational journey.

### Existing global key versus Daily source link

Independently verify both facts without disclosing either identity: Kier's existing global Candidate key is present and matches the operator-supplied mapping, while no active `private.candidate_daily_source_links` row currently exists for Kier. Confirm that the generation resolver uses only the non-reversible `GOOGLE_CREDENTIALLY_PUBLIC_ID` HMAC catalogue and never treats `public.candidates.key_norm` as a substitute. Confirm bootstrap binds the HMAC to the existing Candidate UUID and cannot create a replacement Candidate.

Also verify the app-only onboarding case. A candidate who never used the legacy browser must be linkable by the admin-entered global Candidate key to exactly one existing CloudTMS Candidate UUID, after which the separate source HMAC is attached to that same UUID. Prove no duplicate Candidate can be created and no ambiguous/global-key conflict can silently select a person.

### Real TEST negative runtime

Repeat or inspect the structurally valid invalid-authority broker probe. It must fail with HTTP 401 `SYSTEM_AUTH_FAILED / DO_NOT_RETRY` and cause no database mutation. Do not mistake this for positive signed-publication proof.

## Google deployment status

The R13 helper is saved to NEW MASTER ROTA Head but deliberately undeployed. Active Master remains version 102 and Availability version 216. Independent GO is required before creating/updating a Google deployment. The flag remains false.

## No-change boundary

Do not broaden into database schema/RPC changes, Worker runtime redesign, Candidate Office, finance, Invoice, Banking Pay, Policy X, Emergency/provider execution, production or Candidate feature enablement.
