# CloudTMS Candidate App - master project-continuation handover

Date: 18 August 2026

Purpose: give a completely new reviewer or implementation chat enough context to continue the entire CloudTMS Candidate App programme without relying on the earlier conversation.

This is a whole-project continuation handover. It is not a claim that the complete Candidate App is finished, and it is not another narrow Phase 3 assurance verdict. The narrow current gate is R17; the full programme continues through Phases 4-7 after Phase 3 is independently accepted.

## 1. Read this status first

The current status is:

> Candidate Daily Phase 3 R17 is an unpublished, uninstalled and undeployed review candidate. It corrects the bounded R16 authority-transition/source-lock integration NO-GO. The exact PostgreSQL 17.6 and 18.1 source candidates passed locally, and independent review returned GO for the later collision-checked TEST publication/install sequence. Google installation and bridge enablement remain separate operational gates.

At the time this handover was sealed:

```text
Backend GitHub repository: kierarthur/cloudtms-backend
Backend branch:            test
Backend origin/test:       37b7e5d140cfb97d8b32b8b4b727b470ff545134

Frontend GitHub repository: kierarthur/TEST-Frontend
Frontend branch:            main
Frontend origin/main:       2ffc071bda5ab2447f303336daa49db531ea626d

R17 commit:                 none
R17 push:                   none
R17 Supabase installation:  none
R17 Worker deployment:      none
R17 Google installation:    none
R17 Google deployment:      none
R17 bridge enablement:       none
```

The complete R16+R17 implementation exists only as saved local proposed source, tests, workflow changes, decisions, evidence and assurance material. It must not be mistaken for current GitHub or installed TEST authority.

The packaged Google operator baseline remains:

```text
Availability API active version:    216
Availability API rollback version:  215
NEW MASTER ROTA active version:      102
NEW MASTER ROTA rollback version:    101
Availability bridge switch:          false
Master Rota bridge switch:           false
Google triggers:                     unchanged
```

No R16/R17 source has been pasted, versioned or deployed in either Google Apps Script project. R17 changes no Google source. The receiving chat does not need live Google Apps Script access to review R17 source correctness. It must treat the packaged versions, triggers, disabled switches and saved-source facts as operator evidence, and use the complete unredacted source copies in this archive as the proposed source authority. Actual Google installation or enabled proving is a later, separately authorised operational step.

No Candidate feature flag, entitlement, Candidate row, source-link row, generation or business row was changed by R17. Production is not in scope.

## 2. The project is the complete Candidate App, not a Daily-only project

The programme must deliver one complete Candidate experience across responsive web, iOS and Android, backed by the same CloudTMS authorities already used by Office. Candidate Daily and the temporary Google coexistence bridge are only part of that programme.

The full product includes, at minimum:

- Candidate account activation, login, password and session lifecycle;
- secure access/refresh rotation, logout, device and push identity;
- Current and History timesheet lists using the server-owned partition;
- authoritative timesheet detail and server-provided action hub;
- WEEKLY and DAILY factual entry, break facts, additional units and expenses;
- exact evidence categories, evidence upload and immutable lineage;
- missing-week handling, no-work and recovery actions;
- ELECTRONIC manager approval by EMAIL or PHONE, with manager-owned approval decisions;
- QR Pack issue, signed return, classification, rejection and resubmission;
- notifications, status, refusal and rejection reasons;
- Candidate Daily tiles, availability changes and canonical rota generations;
- Emergency, cannot-attend, leave-early, running-late, DNA and retained specialist journeys;
- messages/content and Past Shifts;
- accessibility, secure local storage, offline-safe drafts and idempotent resume;
- an app-ready CloudTMS Office experience using the same canonical backend/RPC truth.

The full Candidate App is not complete until all of those journeys, the later cutover and gradual rollout have passed their own gates.

## 3. Decision hierarchy

Use this order when sources appear to disagree:

1. The latest explicit user decision recorded in the current Decisions PDF.
2. Later-numbered decisions in that PDF override earlier decisions only for the boundary they explicitly replace.
3. The latest phase implementation authority and compliance matrix for that boundary.
4. The current installed database/Worker/API contract after the required read-only rebaseline.
5. Earlier handovers and historical review findings as provenance, not as permission to revive superseded behaviour.

The controlling full decisions authority in this pack is:

```text
CloudTMS_Candidate_App_Current_Decisions_20260818_Phase3_R17.pdf
```

It contains the complete inherited decisions plus the R17 authority-transition integration addendum and decisions through AV-333. It is the complete decisions document, not an R17-only appendix. The Markdown implementation authorities and matrices are included to make exact source ownership and executable proof easier to follow.

Never implement from one historical handover in isolation. Historical packs explain why a rule exists; the current PDF and later-controlling authority documents decide what applies now.

## 4. Non-negotiable architecture

### 4.1 CloudTMS owns business truth

CloudTMS remains the sole owner of timesheet, financial and lifecycle truth. Candidate clients submit factual inputs. They do not calculate pay, charge, VAT, ERNI, margin, invoice breakdown or TSFIN and do not infer route, eligibility, lifecycle or approval authority from labels.

The existing canonical DAILY and WEEKLY financial owners, Process, Authorise, invoice, payment and official-document authorities remain in control. Candidate work must compose them, not create parallel engines.

### 4.2 Public and private Candidate boundary

The public Candidate broker is the only Candidate/manager internet-facing boundary. It calls a service-authenticated private CloudTMS Candidate API. The public broker does not access Supabase or R2 directly, does not carry Supabase service credentials and does not reconstruct official documents.

The private API composes database/RPC, R2, mail, renderer and retained system authorities. Candidate sessions can never invoke Office authority.

### 4.3 Database/RPC limits

The accepted core Candidate architecture contains exactly seven Candidate App core tables and fourteen public service-role Candidate business RPCs. Candidate Daily later adds its separately accepted twelve-table and thirteen-RPC authority. Do not introduce an extra core Candidate table or business RPC merely to work around an existing authority.

### 4.4 Financial and Banking Pay isolation

Finance, invoice economics, payments, settlement, remittance, Banking Pay and Policy X are out of Candidate scope unless an explicit later decision says otherwise. Do not change them while completing Candidate work.

### 4.5 Server-owned capability and presentation

Clients consume server-owned statuses, capabilities, actions, document names, warnings, exact identities and typed error/retry classes. Clients must not:

- invent an action;
- turn one action into another;
- infer an enabled action from a display status;
- use a raw backend state as user-facing wording;
- reconstruct official PDFs;
- calculate Candidate eligibility in the browser;
- use a generic fallback that widens authority.

## 5. Completed and accepted foundations

The following are established programme foundations. They remain regression responsibilities in every later phase.

### 5.1 Candidate core DB/RPC/backend/API

The core Candidate DB/RPC/backend/API freeze has independent GO. This includes:

- activation, login, password, refresh-family rotation, replay revocation and logout;
- Candidate-safe bootstrap, Current/History, detail and missing-week projections;
- canonical route family and import-authoritative view-only enforcement;
- evidence, expenses, workflow/component lineage and one-claim concurrency;
- manager EMAIL/PHONE request, approval and finalisation authority;
- QR Pack generation, return, complete-pack and immutable document ownership;
- rejection, refusal, resubmission and no-work authority;
- request idempotency, mutation receipts and mixed-version concurrency corrections;
- public/private broker separation, rate limits, CORS, tokens, uploads and stable public errors;
- official renderers, R2 ownership, mail/provider permits and retry-safe delivery.

Do not reopen this architecture without a concrete independently reproduced defect.

### 5.2 CloudTMS Office Candidate compatibility

The Office Candidate implementation is app-ready and uses the same canonical contracts. It remains a regression boundary for later full-app work.

Controlling Office rules include:

- Office staff never perform manager PHONE/EMAIL approval and never enter a manager's approval reason;
- Office may reject a Candidate submission only before Authorisation and outside protected financial states;
- an authorised timesheet cannot show `Reject Candidate Submission`; Unauthorise must happen first where permitted;
- Simple Timesheet retains Overview, Lines, Expenses, Evidence, Issues, Finance and Audit;
- evidence is displayed in the Evidence tab, with server-owned classifications;
- the combined issued PDF appears as audit-only `Unsigned QR Pack` and cannot make the record authorisable;
- returned signed documents remain separate and classified by type;
- Office wording is `QR Pack` or `QR Timesheet`, never the internal term PAPER;
- PHONE/EMAIL mechanics are hidden from Office status wording;
- QR progress is mutually exclusive: only the furthest applicable QR state appears;
- terminal Candidate status outranks retained historical QR facts;
- no Candidate status is presented on routes that do not support Candidate submission, including manual non-QR and import-authoritative routes;
- Processing Status is only the processing/invoice lifecycle, not legacy QR/electronic wording;
- all Candidate actions render only when the server explicitly enables them;
- every mutation uses the styled CloudTMS confirmation component, never a native browser dialog;
- material modal layout, appearance, wording or interaction decisions not fixed by the decisions authority must be agreed with the user before implementation, preferably as one consolidated question block per modal;
- all new modal/button changes require desktop and narrow Playwright visual and functional proof.

The full Office status catalogue and every approved button/bulk rule are in the Decisions PDF and implementation plan. Do not rely on this summary instead of those authorities.

### 5.3 Candidate Daily Phase 0

Phase 0 froze the Daily/Emergency coexistence decisions, operation/error matrix, minimal-change legacy rule, identity direction and phased delivery. It has independent GO.

### 5.4 Candidate Daily Phase 1A

Corrected Phase 1A has independent GO. It provides the dark, closed HMAC transport boundary and twenty-four Candidate/system Daily routes, including:

- exact signing/canonicalisation;
- request framing and correlation;
- nonce/replay controls;
- closed public errors and retry classes;
- public/private service separation;
- pre-auth throttling;
- no database or Google business effect while routes remain dark.

### 5.5 Candidate Daily Phase 1B and Phase 2

Phase 1B maps the accepted Daily operations into strict Worker-to-RPC composition. Phase 2 owns canonical source links, generations, day facts, availability, command/batch receipts, projection state, authority transitions and external-effect receipts.

The Phase 2/1B R8-R10 chain corrected cutover/rollback barriers and received the gate needed to proceed to Phase 3. It remains installed disabled and must be regression-tested. It does not itself enable Candidate Daily, create a real entitlement or complete a Candidate UI.

## 6. Current Phase 3 purpose

Phase 3 is a temporary coexistence bridge. Its governing principle is:

> Do not modernise the legacy app. Contain it.

The existing legacy browser, login, `msisdn` lookup, Google Sheets behaviour and most Apps Script logic remain unchanged. The secure boundary is the trusted server-to-server Apps Script-to-CloudTMS call, not the legacy browser.

### 6.1 Disabled behaviour

When `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` is missing, blank, false, `FALSE`, `0` or anything other than case-insensitive `true`:

- the legacy Availability app behaves exactly as before;
- Master Rota publishes exactly as before;
- no CloudTMS bridge HTTP request is made;
- no bridge retry/status request is made;
- no bridge-owned property or Sheet state is written;
- no bridge log is emitted;
- the legacy result/envelope is returned unchanged.

This exact equivalence is a hard safety and rollback invariant.

### 6.2 Enabled Master Rota flow

When the bridge is later explicitly enabled in TEST:

1. NEW MASTER ROTA performs its existing Availability publication first.
2. Only an accepted `AVAILABILITY_UPDATE_END` is eligible for mirroring.
3. Master Rota builds the complete fourteen-day factual generation from its trusted booking/time facts.
4. It creates the controlled CID1 global Candidate key from `Public ID - Credentially` and a separate versioned source HMAC from that source identity.
5. It signs the closed generation request and publishes it through the Candidate broker/private Worker boundary.
6. The database resolves exactly one pre-existing active Candidate by normalized CID1.
7. On the first valid generation, the database atomically binds the separate source HMAC, creates the initial Google-primary authority scope and publishes the generation.
8. It never creates a Candidate, entitlement, feature flag or cutover.
9. Missing, ambiguous, conflicting or historically reassigned identity fails closed.
10. The complete immutable body, batch ID, idempotency key and correlation identity are retained across uncertainty.

There is no manual source-link bootstrap. New candidates who never used the legacy browser are supported when an administrator has entered the generated CID1 global key on the one canonical Candidate record. Their first valid generation creates the source link automatically.

### 6.3 Enabled Availability flow

When the bridge is later explicitly enabled in TEST:

1. Availability performs its existing login and trusted legacy candidate resolution.
2. It builds the existing legacy tile envelope first.
3. It derives the source HMAC without exposing the raw public ID, mobile number, email, Candidate UUID or secret to the browser.
4. It calls the signed Candidate system route.
5. The Worker/database resolve the exact canonical source link and return canonical tile truth.
6. Apps Script merges canonical by-date fields into the existing envelope while retaining legacy-only cohorts, welcome and Emergency presentation fields.
7. Failure returns the unchanged legacy envelope.
8. Availability changes dual-write under a persisted factual operation identity with status-first recovery and one exact not-found retry.

The old browser receives no CloudTMS access token, HMAC secret, Candidate UUID, Supabase access or power to nominate an arbitrary Candidate.

### 6.4 Retained systems

Master Rota must continue publishing to Availability even after the temporary legacy browser is retired. Availability, Emergency and retained specialist consumers still need current work truth until each has been separately migrated and accepted.

Retiring the temporary browser later does not retire:

- the Availability Sheet/Apps Script service;
- Emergency functions;
- Master Rota publication;
- signed synchronisation;
- projection and freshness owners;
- retained specialist services.

## 7. R11-R17 history and current R17 correction

Phase 3 evolved through narrow assurance rounds:

- R11 prepared the minimal coexistence source and decisions.
- R12 corrected Availability durable accepted-subset/recovery behaviour and installed the disabled Google baseline.
- R13 corrected Master Rota durable generation recovery and quota-safe persistent ownership.
- R14 corrected aggregate HTTP-200 item-outcome handling so `REJECTED` items cannot be mistaken for complete success.
- R15 replaced manual source-link bootstrap with automatic first-valid-generation binding to an existing exact CID1 Candidate.
- R16 corrected the R15 identity-integrity, ACL, Master-response, dual-consumer and workflow-gate findings.
- R17 is the current bounded correction after the independent R16 NO-GO found one incomplete integration with the pre-existing Office transition writer.

R16 addresses six bounded findings:

1. database-enforced uniqueness of normalized active CID1 ownership;
2. immutable one-Candidate ownership of a source HMAC across all history, including retired, rejected, expired and future-valid rows;
3. atomic installation and immediate ACL closure for the private `SECURITY DEFINER` first-generation helper;
4. terminal handling of top-level `409 / IDENTITY_LINK_CONFLICT / DO_NOT_RETRY` in Master recovery;
5. a real dual-consumer proof through reconciliation and `candidate_daily_authority_transition_atomic_v1`, not direct scope/entitlement updates;
6. a hard workflow dependency preventing TEST migration until the exact commit passes PostgreSQL 17.6 and 18.1.

The proposed R16 local verification reports:

```text
Candidate Daily JavaScript:                 102 passed, 0 failed
Complete backend JavaScript:                680 passed, 0 failed
PostgreSQL 17.6 Candidate chain:             45 suites passed
PostgreSQL 18.1 Candidate chain:             45 suites passed
Authentication chain:                       3 passed per engine
Mixed-version authentication:               7 passed per engine
Authority-transition concurrency:           2 passed per engine
First-generation concurrency:               1 passed per engine
R16 identity-integrity concurrency:          3 passed per engine
Normal/public/private Worker dry builds:     PASS
Workflow YAML parse:                         PASS
```

The independent review accepted those R16 boundaries but found that `candidate_daily_authority_transition_atomic_v1` did not contain the new history conflict and acquired Candidate scopes before the new SOURCE lock.

R17 adds one later effective complete definition of that existing function. It acquires sorted distinct SOURCE locks before sorted Candidate scope rows, adds `IDENTITY_LINK_CONFLICT` to the per-item expected-error catalogue and preserves malformed input as indexed `VALIDATION_FAILED` without pre-lock casts. The HTTP/Worker/Google/frontend contracts are unchanged.

Current R17 local evidence is:

```text
Focused Candidate Daily JavaScript:        72 passed, 0 failed
Complete backend JavaScript:               686 passed, 0 failed
PostgreSQL 18.1 Candidate chain:            46 suites passed
PostgreSQL 18.1 R17 concurrency:            2 passed, 0 failed
PostgreSQL 17.6 Candidate chain:             46 suites passed
```

Both engines also pass the public-auth, mixed-version, authority-transition, first-generation, R16 identity and R17 cross-writer concurrency suites. These results are implementation evidence, not self-approval. Independent review and exact-commit CI remain publication gates.

## 8. What the independent R17 reviewer must decide

The reviewer must independently test, not merely restate the packaged green evidence, that:

- the effective later repeatable preserves the full eight-argument transition contract and is installed after the earlier Phase 2/R16 owners;
- every syntactically safe source identity is deduplicated and sorted before Candidate scope locking using the exact R16 lock namespace;
- malformed source values are never cast during pre-lock and remain indexed `VALIDATION_FAILED`;
- `IDENTITY_LINK_CONFLICT` rejects only its item and does not erase valid siblings or the durable terminal receipt;
- exact replay returns the stored mixed result and changed-content key reuse still conflicts;
- the actual generation and transition functions cannot deadlock under forced overlap;
- opposite-order transition batches cannot deadlock;
- the complete inherited R16 identity/ACL/Master/dual-consumer/workflow protections remain intact;
- PostgreSQL 17.6 and 18.1 execute the exact same R17 chain before migration.

The exact mandatory adversarial journeys are in `r17-current-gate/01_INDEPENDENT_REVIEW_BRIEF.md`.

### If R17 receives NO-GO

Do not push, install, deploy, paste into Google or enable anything. Produce one complete, bounded later correction:

- reproduce every remaining defect;
- identify exact source/authority ownership;
- preserve all accepted behaviour;
- update current decisions, compliance matrices, runbooks, evidence and PDF;
- rerun the complete two-engine and JavaScript gates;
- request another independent verdict.

Do not create disconnected addenda or broaden into Phase 4.

### If R17 receives GO

The GO is narrow. It means the complete R16+R17 candidate is fit to enter later publication/install and disabled Google qualification. It is not yet final Phase 3 acceptance and does not authorise Phase 4.

The next sequence is:

1. Perform a fresh collision check with every active CloudTMS task.
2. Re-read repository instructions and rebaseline current backend `origin/test`, current TEST installed Candidate definitions and current Worker identities.
3. Rebase/merge the complete R16+R17 source without altering unrelated Banking Pay, James, finance, invoice, payment, provider or frontend work.
4. If the base changed, rerun the complete focused/full JavaScript gates and exact PostgreSQL 17.6/18.1 chains.
5. Review the exact staged diff, workflow scope, secrets exclusions and source hashes.
6. Commit and push one coherent R16+R17 publication to backend `test` only when explicitly authorised.
7. Require the exact-commit two-engine Candidate matrix to pass.
8. Allow the dependent safe migration only after those jobs pass.
9. Read-verify the installed CID1 index, source-history guard/index, trigger, functions, canonical definitions and ACLs.
10. Dry-build and deploy only the explicitly authorised TEST Candidate Worker targets; never production.
11. Keep every Candidate feature flag, every real Candidate entitlement and both Google bridge flags false.
12. Re-export/recheck effective Google source, active/rollback deployment IDs and triggers immediately before any Google edit.
13. With separate Google authority, install the exact accepted source while still disabled and create a rollback-safe version.
14. Prove disabled equivalence: the legacy app, Master Rota, Availability and Emergency work exactly as before with zero bridge side effect.
15. Request separate authority for the enabled TEST proving window.
16. Turn on only the agreed TEST bridge switches and observe the ordinary eligible population. Kier Arthur may be the first observed phone journey but must not be a code allowlist or the only supported Candidate.
17. Prove first-generation automatic binding for exact existing CID1 Candidates, exact replay, later generation, identity conflicts, outage/recovery, quota, lock contention and projection overlay behaviour.
18. Prove the old Availability app reads the canonical generation by source HMAC and preserves its legacy envelope/Emergency fields.
19. Prove the Candidate UUID path reaches the same generation only through the real controlled readiness/transition gates. Do not bypass them.
20. Turn the bridge off immediately on any unsupported defect; legacy remains the rollback path.
21. Issue a separate final Phase 3 GO only when enabled coexistence evidence passes and no supported blocker remains.

## 9. Phase 4 - full Candidate App implementation

Phase 4 starts only after a separate final Phase 3 GO. It is the full Candidate App implementation, not merely a Daily screen.

### 9.1 Clients

Build and prove the same contract-driven experience for:

- responsive web;
- iOS;
- Android.

The clients must share generated contracts or an equivalently strict single contract source. Platform-specific presentation must not create platform-specific business rules.

### 9.2 Authentication and device journeys

Implement and test:

- account activation/challenge;
- login, locked/expired credentials and password recovery;
- access and refresh rotation;
- exact replay/lost-response behaviour;
- logout and session invalidation;
- secure native/browser token storage;
- device/push identity and notification feed;
- no secrets or service credentials in clients.

### 9.3 Timesheet journeys

Implement and test:

- Current as default and History as the server-disjoint paid partition;
- exact week-ending labels and ordering;
- card tap to authoritative detail;
- the server-provided primary action and complete detail action hub;
- WEEKLY and DAILY factual entry;
- break intervals/minutes and explicit no-break;
- additional units;
- expenses and exact evidence categories;
- missing weeks and add-missing authority;
- no-work;
- draft/resume and idempotent submit;
- immutable official documents;
- refusal/rejection reasons and precise recovery action;
- resubmission with correct replacement lineage;
- QR Pack download, signed return and status;
- manager EMAIL/PHONE approval without exposing manager-only decisions to Office or Candidate;
- finalisation retry where and only where server-enabled.

### 9.4 Candidate Daily journeys

Implement and test:

- fourteen-day tiles from canonical CloudTMS truth;
- availability changes and their durable status/replay handling;
- booked/unavailable/system-blocked states;
- freshness and stale presentation;
- safe retry and offline-safe local drafts;
- same-generation parity with the legacy Availability facade and projection;
- retained specialist entry points without executing an unaccepted provider path.

### 9.5 UI decision and quality rule

If any material layout, appearance, wording or interaction is not already settled in the Decisions PDF, pause before implementing that material decision and ask the user. Gather all likely decisions for one screen/modal into one consolidated question block with clear recommendations and options, to minimize stop/start cycles.

For every new action button or modal change:

- identify it to the user before implementation if the decision is not already recorded;
- explain in plain English when it appears and exactly what it does;
- do not omit a required action merely because approval is needed;
- render it only when the server says it is applicable;
- use a styled in-product confirmation for mutations;
- run Playwright functional and visual checks at desktop and narrow sizes;
- reject messy, clipped, misaligned, overflowing or unprofessional layouts;
- prove no native `alert`, `confirm` or `prompt` is used.

### 9.6 Phase 4 exit evidence

Require:

- contract/source parity across all three clients;
- accessibility checks;
- auth/session/adversarial tests;
- server-action gating tests;
- patched/deployed-asset browser proof;
- desktop and narrow visual review;
- offline/reconnect and idempotency evidence;
- no direct Google/Supabase access from clients;
- shadow parity among Candidate client, CloudTMS DB, legacy facade and Sheet projection;
- an updated complete decisions matrix and independent GO.

## 10. Phase 5 - controlled TEST cutover

Phase 5 changes authority only through the installed transition owner. It is not a flag flip performed by UI or Apps Script.

Required work:

1. Choose explicitly approved TEST cohorts/entitlements.
2. Prove exact identity/source links and complete current generation.
3. Drain/reconcile commands, batches, projections and effects.
4. Prove accepted/required/effective cursors and freshness.
5. Reach the required READY sync state with no unresolved work.
6. Use independent Office actor and approver where required.
7. Call the real `candidate_daily_authority_transition_atomic_v1` with exact expected facts.
8. Enable only the agreed TEST global flag and entitlements.
9. Run shadow parity, latency, availability, error-budget and soak gates.
10. Rehearse and prove rollback to Google primary with the global flag disabled first.
11. Monitor old and new paths and stop on divergence.
12. Issue a separate cutover GO; do not infer production authority.

## 11. Phase 6 - full specialist and workflow acceptance

Phase 6 completes every retained operational journey that the earlier phases deliberately did not execute.

It includes:

- Emergency;
- cannot attend;
- leave early;
- running late;
- DNA;
- acknowledgement/escalation;
- messages and content;
- Past Shifts;
- concrete specialist/provider adapters through durable effect claim/lease/completion/status receipts;
- DAILY signing and manager EMAIL/PHONE approval acceptance;
- both legacy and new-client entry paths during coexistence;
- exact replay, lost-response and provider-acceptance evidence;
- proof that no legacy capability has silently disappeared.

The existing provider or specialist system must not be called until its exact effect receipt and idempotency authority are proven. A missing dependency fails closed; it is not treated as success.

## 12. Phase 7 - gradual rollout and retirement

Phase 7 is controlled operational rollout, not a code-complete shortcut.

Required work:

- gradual entitled cohorts;
- telemetry, support and error-budget monitoring;
- rollback readiness;
- native/web release management;
- real-world accessibility/device acceptance;
- explicit acceptance for every retained specialist journey;
- separately approved retirement of the temporary legacy browser and compatibility adapter only after parity is proven.

Legacy-browser retirement does not automatically retire Availability, Emergency or Master Rota. Each retained service needs its own later migration and acceptance before it can be removed.

Production requires separate explicit authority and is outside every current pack.

## 13. Identity model

The identity direction is intentionally strict:

```text
trusted Google Public ID - Credentially
  -> normalized source value
  -> generated CID1 global Candidate key
  -> administrator places CID1 on the one existing Candidate record
  -> first valid signed generation carries CID1 plus separate source HMAC
  -> database resolves one active normalized CID1 owner
  -> database atomically binds source HMAC to that Candidate
  -> old Availability path reads by source HMAC
  -> new Candidate app reads by authenticated Candidate UUID after cutover gates
```

Rules:

- never create a Candidate automatically;
- never match by name, email, phone or fuzzy logic;
- never send the raw public ID to CloudTMS in the generation body;
- never allow a source HMAC to change Candidate owner across history;
- never silently reactivate a retired/rejected source identity;
- never use a Candidate-specific allowlist;
- never require legacy-app participation for a new-app Candidate;
- no manual source-link bootstrap is part of the ordinary flow.

CID1 is identity-equivalent controlled data, not anonymised data. Do not print full CID1 values or source HMACs in logs or handovers.

## 14. Google configuration and secrets

The property names are documented; their values are deliberately not in this archive.

Both Google projects use:

```text
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED
CLOUDTMS_CANDIDATE_BASE_URL
CLOUDTMS_CANDIDATE_ENVIRONMENT
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET
```

Availability API alone also uses:

```text
CLOUDTMS_CANDIDATE_EXECUTOR_ID
```

The public Candidate broker holds only the accepted/current key identifiers. The private Candidate Worker holds the corresponding transport signing secret. The separate source-identity secret remains in the trusted Google projects and is not a public-browser secret.

Never put secret values in source, tests, screenshots, logs, reports or a handover. Verify names and presence only.

## 15. Testing strategy through completion

Every phase must use evidence proportionate to its boundary.

### Source and static checks

- closed request/response schemas;
- exact allowlists and stable errors;
- source hashes and Git blob parity;
- no secret material;
- no local-machine paths;
- no unintended Candidate/finance/Banking Pay overlap;
- workflow dependency and migration ordering.

### Database tests

- PostgreSQL 17.6 and 18.1 exact ordered chains;
- runtime, ACL, RLS/grant and feature-off checks;
- same-key replay and different-key concurrency;
- mixed-version and lock-order tests;
- identity insert/update/activation races;
- real authority-transition and rollback barriers;
- no direct mutation of protected state in tests counted as authority proof.

### Worker/API tests

- public/private separation;
- HMAC framing/canonicalisation/replay;
- strict request/response reconstruction;
- exact status/retry/error mapping;
- aggregate item outcomes;
- request size and quota bounds;
- disabled/feature-off behaviour;
- dry builds and exact deployed-runtime smoke proof when deployment is authorised.

### Google coexistence tests

- bridge false: old app and Master Rota exactly unchanged;
- bridge true: old app still works while canonical data travels through the new path;
- Availability-first publication remains;
- automatic first-generation binding;
- no manual bootstrap;
- lost response, status recovery and one exact retry;
- outage fallback to unchanged legacy envelope;
- property quota, body chunking and corrupt-state fail-closed;
- old/source-HMAC and new/Candidate-UUID reads converge on the same generation;
- Emergency and retained legacy-only fields survive.

### Frontend and app tests

- server-owned capability rendering;
- no actions on unsupported routes;
- desktop and narrow Playwright;
- professional modal layout;
- all buttons wired and confirmed;
- no native dialogs;
- loading, stale/conflict and recovery-only states;
- offline/reconnect/idempotency;
- accessibility;
- deployed/patched asset identity proof.

### Safety snapshot

Before and after any authorised TEST mutation, capture:

- exact target environment;
- feature/entitlement state;
- bounded row counts;
- exact operation identities without exposing sensitive values;
- Worker/version identity;
- final state and rollback outcome.

## 16. Common mistakes the receiving chat must avoid

Do not:

- declare the whole project complete when R17 or Phase 3 passes;
- publish R17 before exact PostgreSQL 17.6/18.1 evidence and the independent verdict;
- enable bridges merely because source has passed static tests;
- create source links manually for every Candidate;
- create or deduplicate Candidate rows automatically;
- use Kier Arthur as a code allowlist;
- replace the existing Availability publication with CloudTMS publication;
- retire Availability/Emergency/Master Rota with the browser;
- modify legacy UI/login/msisdn behaviour unnecessarily;
- expose secrets or identities;
- bypass the authority-transition RPC in an acceptance test;
- accept HTTP 2xx/`ok:true` without validating every item outcome;
- clear an uncertain operation and invent a new idempotency identity;
- introduce a parallel financial, Process, Authorise, invoice or payment engine;
- let Candidate status leak onto manual non-QR or import-authoritative timesheets;
- show more than the furthest QR lifecycle status;
- let Office approve on behalf of a manager;
- show rejection on an authorised timesheet;
- make unagreed material UI decisions;
- claim Playwright verification when the browser did not load the patched/deployed asset;
- access or deploy production.

## 17. Repository and operational discipline

Before any work:

1. Read the workspace and nearest repository `AGENTS.md` files.
2. Re-read saved disk source immediately before editing.
3. Preserve unrelated dirty files and other chats' work.
4. Perform an immediate cross-chat collision check before every push, migration, Worker/Pages deployment, Google deployment or live TEST mutation window.
5. Confirm the exact TEST project/Worker/repository branch.
6. Never print secrets or auth artifacts.
7. Use current installed definitions and current origin heads, not this handover's historical snapshot, as runtime baseline.
8. Keep publication/install/deploy windows serialized.
9. Close the shared window explicitly with final commits, workflows, installed hashes and Worker identities.

## 18. What the receiving chat should do first

If the receiving chat is asked to review the current state:

1. Read `00_READ_THIS_FIRST.md`.
2. Read this master handover.
3. Read the full Decisions PDF.
4. Read `02_PHASE_COMPLETION_MATRIX.md` and `03_PROJECT_AUTHORITY_INDEX.md`.
5. Read every file in `r17-current-gate/`.
6. Independently compare packaged R17 source with current GitHub and TEST installed authority.
7. Inspect the exact R17 PostgreSQL 17.6 and 18.1 evidence and reproduce the bounded checks required by the current assignment.
8. Treat the recorded independent R17 verdict as GO for collision-checked TEST publication/install only; do not infer Google enablement or Phase 4 authority.
9. State explicitly that a technical R17 GO is not final Phase 3 GO and not full-project completion.

If the receiving chat is asked to continue implementation after an R17 GO, follow the publication and proving sequence in Section 8, then obtain separate Phase 3 acceptance before starting Phase 4.

## 19. Final current disposition

```text
Candidate core DB/RPC/backend/API:       GO remains in force
CloudTMS Office Candidate integration:  implemented/app-ready regression boundary
Candidate Daily Phase 0:                GO
Candidate Daily Phase 1A:               GO
Candidate Daily Phase 1B:               GO
Candidate Daily Phase 2:                GO/installed disabled; retain regression
Candidate Daily Phase 3 R17:            INDEPENDENT GO FOR COLLISION-CHECKED TEST PUBLICATION/INSTALL
Phase 3 publication/install:             NOT PERFORMED
Phase 3 Google source installation:      NOT PERFORMED
Phase 3 bridge enablement/proving:        NOT PERFORMED
Phase 4 full Candidate clients:          NOT STARTED
Phase 5 controlled TEST cutover:         NOT STARTED
Phase 6 full specialist acceptance:      NOT STARTED
Phase 7 rollout/legacy retirement:       NOT STARTED
Production:                              NOT AUTHORISED
```

The correct immediate action is the collision-checked TEST publication, exact-commit two-engine gate, disabled database/Worker installation and verification. Google source installation and every bridge-enabled proving step remain separately controlled.
