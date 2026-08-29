# CloudTMS Candidate App - Candidate Daily Phase 3 R11 handover

Date: 17 August 2026

Environment: TEST preparation only

## 1. Executive result

This package implements the complete Phase 3 source boundary for temporary coexistence between:

```text
the certified Availability API Apps Script project
the certified NEW MASTER ROTA Apps Script project
the frozen Phase 1B signed CloudTMS system routes
the frozen Phase 2 Candidate Daily database authority
```

It deliberately does not modernise the temporary legacy browser. It contains that browser behind the existing Apps Script projects and adds the smallest signed server-to-server compatibility boundary.

The source is complete, unredacted and copy/paste-ready. No live Google project, trigger, Sheet, CloudTMS database, Worker or feature flag was changed by this R11 build.

## 2. Binding false/true behaviour

### Flag missing or false

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED = false
```

means:

- legacy tiles continue to come from the same Master Rota-backed Availability/EmailHistory data and cache;
- legacy availability changes continue through the same queue and Sheet logic;
- Master Rota continues its existing Availability publication;
- Emergency and retained specialist behaviour remains unchanged;
- no CloudTMS request, retry, status lookup, bridge log, bridge state or bridge-owned Sheet write occurs;
- every existing legacy response object is returned unchanged.

The only enabling value is case-insensitive `true` after trimming.

### Flag true

For a tile read, Apps Script first creates the exact legacy envelope, then makes a signed server-to-server call to the CloudTMS Worker. The Worker validates the signed legacy source and reads canonical tiles from TEST Supabase through the frozen Phase 2 authority. Apps Script overlays those canonical date facts while retaining legacy-only presentation and Emergency fields. On failure it returns the original legacy envelope.

For an availability change, the existing legacy write completes first and remains the browser result. The helper mirrors the same factual change to CloudTMS with one durable operation identity and status-first lost-response recovery.

For Master Rota, the existing Availability update remains first. A successful update-end additionally publishes the complete fourteen-day generation to CloudTMS. This dual publication remains necessary after the temporary legacy browser retires because Availability and Emergency still consume Master Rota truth until separately migrated and accepted.

## 3. Minimal-change legacy boundary

Unchanged:

```text
legacy browser UI
legacy login and msisdn resolution
existing Apps Script business logic
Sheet/cache shapes
manifest and OAuth scopes
all installed triggers
existing Emergency and specialist journeys
legacy response contracts
```

Added:

```text
four guarded Availability return seams
one guarded Master Rota post-publication seam
one complete helper file in each project
```

Each seam is guarded with `typeof` so the certified path still returns normally if the helper has not yet been pasted.

## 4. Complete revised source and rollback

The archive contains:

```text
source/availability-api/Code.gs
source/availability-api/CloudTMSCandidateBridge.gs
source/availability-api/rollback/Code.gs

source/master-rota/Code.gs
source/master-rota/CloudTMSCandidateBridge.gs
source/master-rota/rollback/Code.gs
```

The revised `Code.gs` files are full project files, not patches or excerpts. The rollback files are byte-identical to the two user-certified inputs.

Source hashes and current Google evidence are in `02_CURRENT_STATE.md` and `03_VERIFICATION_SUMMARY.md`.

## 5. Security authority

The legacy browser never receives or sends:

```text
CloudTMS session credentials
Supabase credentials
HMAC secrets
canonical Candidate UUIDs
raw Credentially public IDs
```

Apps Script resolves the candidate through the existing msisdn/Candidate List path, derives an environment-bound non-reversible source HMAC with a dedicated source secret, then signs the exact UTF-8 body with the frozen `CLOUDTMS-HMAC-V1` protocol.

CloudTMS independently maps the signed source identity to the exact allowed Candidate source link. The browser cannot nominate another Candidate.

The supplied helpers call the already-implemented Candidate broker routes exactly:

```text
POST /candidate-system/v1/google-availability/legacy/tiles
POST /candidate-system/v1/google-availability/legacy/availability
POST /candidate-system/v1/google-availability/legacy/availability-status
POST /candidate-system/v1/google-availability/rota-generations
POST /candidate-system/v1/google-availability/projection/claim
POST /candidate-system/v1/google-availability/projection/complete
POST /candidate-system/v1/google-availability/effects/claim
POST /candidate-system/v1/google-availability/effects/complete
POST /candidate-system/v1/google-availability/effects/status
```

The public broker maps this signed system family through its private service binding. The private Worker applies the frozen Phase 1B body validators and maps the accepted operation to the existing Phase 2 RPC owner. Source and tests prove the route/method/header/body agreement; the live Google-to-broker round trip remains a mandatory controlled installation gate.

These are configured as Google Apps Script **Project Script Properties**, not JavaScript globals in `Code.gs`. Both projects require:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false
CLOUDTMS_CANDIDATE_BASE_URL=https://test-cloudtms-candidate-broker.kier-88a.workers.dev
CLOUDTMS_CANDIDATE_ENVIRONMENT=TEST
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID=candidate-daily-google-test-v1
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET=<new signing secret generated after Phase 3 source GO>
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET=<different new source-identity secret generated after Phase 3 source GO>
```

Availability API additionally requires:

```text
CLOUDTMS_CANDIDATE_EXECUTOR_ID=availability-api-google-test
```

Read-only Cloudflare control-plane inspection of the deployed TEST public Candidate broker and private Candidate Worker on 17 August 2026 found no Candidate Daily Google primary key ID, signing secret, accepted-key catalogue, overlap key or source-HMAC secret. That is the expected dark state. The key ID and both secrets must be newly created after source GO. The exact catalogue, Worker ownership and safe configuration order are in `source/SCRIPT_PROPERTIES.md`. Installation-only secret values are deliberately absent from source and the pack.

## 6. Stable replay and recovery

Availability changes and Master generations persist their frozen operation/batch facts under Script Lock. After uncertainty:

1. status is read first;
2. a durable result is returned if found;
3. the first authoritative not-found may consume one exact retry;
4. continued uncertainty is status-only;
5. the operation is cleared only after an authoritative terminal result or stable rejection.

No replacement UUID or idempotency key is invented because a response was lost.

## 7. Projection and Emergency compatibility

The explicit projection drain claims and completes the frozen Phase 2 outbox contract. It writes only through the existing Availability mapping and returns `DEFERRED_OVERLAY` instead of overwriting booked or system-blocked legacy truth.

The helper includes external-effect receipt claim/complete/status primitives, but Phase 3 does not wire them into providers. Existing Emergency, cannot-attend, leave-early, running-late, DNA, messaging and specialist services therefore continue unchanged. Their complete dual-path acceptance remains Phase 6.

## 8. Decisions authority

The current Decisions PDF preserves accepted Sections 1-88 and adds Sections 89-94 and decisions AV-250 through AV-270. The later-controlling Phase 3 decisions cover:

- strict disabled-state equivalence;
- minimal/no legacy client change;
- Apps Script-to-Worker authority;
- source HMAC and HMAC v1 signing;
- legacy-first/fail-open behaviour;
- canonical tile merge;
- Master Rota dual publication and fourteen-day generations;
- stable replay;
- projection overlay safety;
- no new triggers or revival of `ai_startDailyPings`;
- Availability/Emergency/Master survival after browser retirement;
- complete rollback and later live-Google proving gates;
- no feature activation or financial drift.

## 9. Verification result

```text
Focused retained + Phase 3 tests: 48 passed, 0 failed
Complete backend JavaScript:      625 passed, 0 failed
```

The package includes raw TAP for both commands. It also includes the Phase 3 test, frozen HMAC vector and incoming R10 independent GO evidence.

## 10. Installation status and required next gate

This R11 is a source handover. It has not changed either live Apps Script project.

After independent source acceptance, follow `CANDIDATE_DAILY_PHASE3_INSTALLATION_RUNBOOK.md`. The first manual installation must save with the bridge flag false, prove exact legacy parity and retain the existing deployment versions for rollback. Only then may one approved TEST cohort be enabled for signed route, generation, recovery, projection, quota and outage proving.

The correct independent disposition is therefore either:

```text
GO for Phase 3 controlled Google installation/proving gate
```

or one bounded evidence-backed source blocker. It is not GO for Candidate Daily cutover, public feature enablement, Phase 4 or production.

## 11. Full implementation after Phase 3

| Phase | Remaining outcome |
| --- | --- |
| Phase 4 | Complete Daily responsive web/iOS/Android journeys, shadow parity and retained specialist interfaces |
| Phase 5 | Controlled TEST cutover with identity/parity/soak/error-budget and rollback proof |
| Phase 6 | Complete Emergency, cannot-attend, leave-early, running-late, DNA, messages/content, Past Shifts, DAILY signing and EMAIL/PHONE acceptance across old/new paths |
| Phase 7 | Gradual entitled rollout, monitoring and separately authorised retirement of the temporary browser/compatibility adapter |

Availability, Emergency and Master Rota publication remain operational after temporary browser retirement until each is separately migrated and accepted.

## 12. No-change statement

R11 changes no Candidate core/Office route, database function, financial calculation, rate, Process/Authorise rule, invoice, payment, Banking Pay, Policy X, provider, settlement, remittance or production resource.

No secret, raw candidate identity, local filesystem path, screenshot or authentication artifact is included in the handover.
