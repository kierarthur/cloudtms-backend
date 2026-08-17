# Candidate Daily Phase 2 and Phase 1B Implementation Authority - R8

Date: 17 August 2026

Environment: TEST only

Runtime source commit: `1823403f33fc6e3741c435dce5b2b3a6340db1de`
Product state: installed and deployed, but globally disabled and unentitled

## 1. Scope and controlling outcome

R8 implements the complete accepted Phase 2 additive database/RPC authority and Phase 1B public-broker/private-Worker integration for Candidate Daily availability. It follows the independently accepted Phase 0 R5 specification and corrected Phase 1A R7 transport boundary.

R8 is not a claim that the full Candidate application is complete. It deliberately does not implement or activate:

- the Phase 3 Google coexistence adapter;
- any Availability or NEW MASTER ROTA Apps Script edit or deployment;
- the Phase 4 Candidate Daily user interface in responsive web, iOS or Android;
- TEST authority cutover, real entitlements, real Candidate Daily data or communications;
- full specialist journey acceptance;
- production rollout or legacy-browser retirement.

The only permitted current verdict is an independent GO or bounded NO-GO for Phase 2 and Phase 1B R8. Feature activation and later phases remain separate decisions.

## 2. Single-owner architecture

```text
Candidate app / browser
        |
        v
public Candidate broker
  public access token, origin, abuse, schema and safe-response boundary
        |
        v  Cloudflare service binding
private Candidate Worker
  service authentication, Candidate session, HMAC/nonce, RPC composition
        |
        v
TEST Supabase
  exact source link + entitlement + generation + availability + receipts
  + projection cursor + transition + external-effect authority

Temporary legacy browser
        |
        v
existing Availability Apps Script (unchanged in R8)
        |
        v  future Phase 3 narrow signed server adapter
same private Candidate Worker and same TEST Supabase authority
```

No browser receives Supabase credentials, a Google-system HMAC secret, a canonical source-link mapping or permission to nominate an arbitrary Candidate UUID. The public broker cannot access Supabase. The private Worker cannot invent a second business state; it adapts strict factual inputs to one installed RPC owner.

## 3. Exact Phase 2 database surface

The schema migration `17082026_0010_candidate_daily_phase2_authority_schema.sql` creates exactly twelve additive tables and adds one disabled global flag. It does not add a Candidate business table outside this approved domain.

| Schema.table | Sole purpose | Direct client DML |
| --- | --- | --- |
| `private.candidate_daily_authority_scopes` | One environment authority mode and transition fence | none |
| `private.candidate_daily_entitlements` | Exact Candidate/cohort entitlement and bounds | none |
| `private.candidate_daily_source_links` | Approved legacy source identity to canonical Candidate mapping, using digests rather than raw tokens | none |
| `public.candidate_daily_command_receipts` | Durable factual command key/request/result ownership | none |
| `private.candidate_daily_batch_receipts` | Durable generation/batch request ownership | none |
| `public.candidate_daily_rota_generations` | Immutable versioned generation header and completeness state | none |
| `public.candidate_daily_rota_days` | Canonical generation-bound fourteen-day rota/day facts | none |
| `public.candidate_daily_availability_days` | Canonical Candidate/day availability state and generation binding | none |
| `public.candidate_daily_sheet_projection_outbox` | Lease, retry, park and projection state for Google compatibility | none |
| `private.candidate_daily_sync_state` | Per-target durable and effective cursor/freshness authority | none |
| `private.candidate_daily_authority_transitions` | Immutable transition/reconciliation audit owner | none |
| `private.candidate_daily_external_effect_receipts` | Claim/lease/completion/result owner for Emergency and retained specialist effects | none |

All twelve tables have RLS enabled with no permissive policy. `anon`, `authenticated` and `service_role` have no direct SELECT/INSERT/UPDATE/DELETE authority. Business access is only through closed security-definer RPCs. The transition ledger is append-only through an immutable trigger.

The migration adds `candidate_daily_enabled=false` only if absent. TEST now has thirteen Candidate flags, all false. It does not create an entitlement, source link, generation, day, availability, receipt, outbox, transition or effect row.

## 4. Exact Phase 2 RPC surface

The repeatable `17082026_0015_candidate_daily_phase2_rpcs_v1.sql` owns exactly thirteen public service-role RPCs. Every RPC is `SECURITY DEFINER`, has a closed `search_path`, grants execution only to `service_role`, and denies `anon` and `authenticated`.

| RPC | Authority and result |
| --- | --- |
| `candidate_daily_legacy_availability_apply_atomic_v1` | Applies one legacy factual availability mutation through the approved source link and durable command receipt |
| `candidate_daily_legacy_availability_status_get_v1` | Returns exact durable legacy command status without repeating mutation |
| `candidate_daily_rota_generation_publish_atomic_v1` | Publishes one canonical complete generation under batch idempotency and source/version checks |
| `candidate_daily_projection_claim_v1` | Claims one due projection item under a bounded lease |
| `candidate_daily_projection_complete_atomic_v1` | Completes, retries or parks the exact claimed projection and advances durable/effective cursors only when proved |
| `candidate_daily_sync_status_get_v1` | Reads current mode, generation, cursors, lag and freshness for the signed system |
| `candidate_daily_reconciliation_apply_atomic_v1` | Applies one bounded sheet/reconciliation event through one durable receipt owner |
| `candidate_daily_authority_transition_atomic_v1` | Performs the only GOOGLE_PRIMARY / ROLLBACK_PENDING / SUPABASE_PRIMARY transition |
| `candidate_daily_external_effect_claim_v1` | Claims one retained specialist/Emergency effect under lease and receipt fencing |
| `candidate_daily_external_effect_complete_v1` | Completes or retries the exact effect claim with durable response ownership |
| `candidate_daily_external_effect_status_get_v1` | Returns exact effect status/result for lost-response recovery |
| `candidate_daily_tiles_get_v1` | Returns server-composed Candidate Daily tiles and complete route/action targets |
| `candidate_daily_availability_apply_atomic_v1` | Applies the new Candidate factual availability command through exact session/entitlement/generation checks |

Private helpers own canonical JSON hashing, source-link resolution, flag lookup, current authority mode, complete-generation proof, capability composition and receipt replay. There is no public direct helper authority.

## 5. Identity and source-link authority

The legacy browser continues its current `msisdn` lookup inside Apps Script during coexistence. CloudTMS does not trust a browser-supplied Candidate UUID. The later Phase 3 Apps Script adapter supplies a bounded legacy source identity and signature; the database resolves it through `private.candidate_daily_source_links` to one canonical Candidate account.

The source-link row stores only bounded identity facts and keyed digests required for matching/audit. It does not store an Apps Script bearer secret, Candidate password, browser session or raw mobile token. A disabled, ambiguous, environment-mismatched or unmapped source fails closed.

New Candidate calls derive their Candidate identity from the authenticated private Candidate session, not from request JSON. Legacy and new calls therefore converge on the same canonical Candidate UUID without sharing browser authentication models.

## 6. Generation and availability invariants

One current generation is authoritative per environment and source. A generation becomes active only when:

- its canonical generation identity/version is valid;
- its request hash and batch receipt match;
- all required day facts are present;
- the exact fourteen-day window is complete;
- it is not superseded or internally inconsistent.

Availability rows are generation-bound. Candidate-facing reads and writes fail closed if the current generation is missing, incomplete, stale, mismatched or outside entitlement. The Worker does not synthesize missing days or reuse an older generation merely to produce a response.

The availability command receipt binds the factual request, Candidate identity, generation, date set, values and idempotency key. Exact replay returns the same durable result. Changed factual input under the same key conflicts. Generated response metadata is not allowed to change semantic identity.

## 7. Projection, cursor and deferred-overlay authority

The outbox is the only owner of Google Sheet projection work. Claiming a row creates a bounded lease; completion requires the exact lease token. Failed work receives a bounded retry or a terminal/parked state. Parallel Workers cannot advance the same projection independently.

`private.candidate_daily_sync_state` owns both:

- the durable cursor: what has been durably written/acknowledged; and
- the effective visible cursor: what may currently be represented to coexistence consumers.

`DEFERRED_OVERLAY` is allowed only when the exact current generation/hash proof is present. If an overlay is removed, changed or no longer matches the current generation, effective visibility retreats and parked work becomes eligible for reprocessing. No browser or Apps Script memory owns this decision.

Candidate-facing reads require the configured freshness/generation proof. Signed-system continuity/status/projection routes remain callable under their HMAC authority even when the Candidate product flag is false; otherwise the legacy/Emergency path could be disabled by a future app switch.

## 8. Authority transition and rollback

The only modes are:

```text
GOOGLE_PRIMARY
ROLLBACK_PENDING
SUPABASE_PRIMARY
```

There is one current scope row and one immutable transition ledger. A transition requires the exact expected prior mode/version, current generation and reconciliation/freshness barriers. `ROLLBACK_PENDING` is a real fenced mode, not a UI label. It prevents an unproved direct flip between sources.

The implementation preserves the accepted rollback principle: a failed cutover may return to Google ownership only through the explicit transition/reconciliation authority. It does not delete Supabase audit/receipt facts or silently reuse stale projection state.

## 9. External effects and Emergency compatibility

Emergency and retained specialist actions use a durable effect receipt with:

- one factual effect key;
- canonical Candidate/source/work-date/action identity;
- request hash;
- lease owner/token/expiry;
- attempt count and retry state;
- exact terminal result or failure.

The first accepted executor owns the effect. Exact replay reads the same result. A lost response is recovered through status without inventing another key. The database prevents concurrent claim owners and changed-input key reuse.

R8 provides this authority and strict adapter seam, but it does not execute a real Emergency/specialist effect. The concrete service adapters remain a Phase 3/6 dependency and fail closed as unavailable until supplied. This is a dependency boundary, not permission to omit those journeys from the full app.

## 10. Phase 1B private Worker integration

`broker/src/candidate-daily-phase1b.js` is the single Phase 1B composition module. The private Worker maps the frozen operation catalogue to the installed RPCs and validates every dependency result. It does not issue arbitrary SQL, trust request-owned identity, or forward unknown database fields.

The integration covers:

- Candidate tiles and availability read/write;
- legacy availability apply/status;
- generation publish;
- projection claim/complete;
- sync status;
- sheet edit/reconciliation;
- authority transition;
- external effect claim/complete/status;
- booked-tile action target composition through existing Candidate DAILY routes;
- bootstrap capability sourced from the same database helper.

Database replay results carry an internal `_idempotent_replay` marker. The private adapter uses it to preserve factual replay semantics and removes internal execution detail from the public contract.

## 11. Phase 1B public broker integration

The public broker remains the only browser/native entry. It:

- enforces the accepted route catalogue, origin/native policy, body ceilings and rate classes;
- unwraps only supported Candidate credentials;
- forwards system calls without gaining the Google HMAC secret;
- rebuilds every success and failure from closed response schemas;
- removes internal receipt, version, lease, hash, stack and dependency details;
- fails closed when private success/error shape drifts.

`candidate-broker/wrangler.jsonc` contains separate rate-limit namespaces for Daily reads, commands, external effects and signed-system calls. Public Candidate/manager credentials remain separate from Google system authentication.

## 12. Capability and feature gating

`candidate_app_bootstrap_v1` now derives `capabilities.daily_availability` from the same database-owned helper used by business routes. It does not infer capability from the mere presence of Electronic/DAILY data.

Candidate Daily availability requires all of:

1. `candidate_daily_enabled=true`;
2. exact active Candidate entitlement;
3. authenticated Candidate identity;
4. permitted authority mode;
5. complete current fourteen-day generation;
6. freshness/cursor barriers;
7. operation-specific version/idempotency preconditions.

TEST has none of the activation facts: the flag is false, the entitlement table is empty, and all Daily business tables are empty. Therefore deployed transport and installed SQL cannot expose Candidate Daily business functionality.

Signed-system continuity does not consult `candidate_daily_enabled`. It still requires a valid service binding, HMAC version/key, timestamp, nonce, body digest, correlation and route-specific database authority.

## 13. Legacy minimal-change rule

R8 changes no Availability browser, login, HTML, Sheet data, trigger, deployment or Apps Script source. It changes no NEW MASTER ROTA code or deployment.

Phase 3 is constrained to the smallest server-side change:

```text
existing legacy browser
 -> existing Apps Script handler and msisdn lookup
 -> narrow source-link resolution / stable event identity / HMAC request
 -> CloudTMS private boundary
 -> translate response back into the existing legacy response shape
```

The old browser must remain functional throughout coexistence. It must not receive CloudTMS access tokens, HMAC secrets, Supabase credentials or arbitrary Candidate identity authority.

NEW MASTER ROTA must continue publishing the working rota to Availability and, after Phase 3, the signed CloudTMS generation boundary. Retirement of the old browser does not remove the Availability service, Emergency logic, Master Rota publication or specialist integrations.

## 14. Verification and installed TEST evidence

| Gate | Result |
| --- | --- |
| PostgreSQL 17.6 | 42 Candidate suites PASS |
| PostgreSQL 18.1 | 42 Candidate suites PASS |
| Complete backend JavaScript | 605/605 PASS |
| Focused Phase 1A/1B/2 contracts | 35/35 PASS |
| OpenAPI | 62 paths; semantic validation PASS |
| Candidate private Worker dry build | PASS |
| Candidate public broker dry build | PASS |
| TEST database | PostgreSQL 17.6; exact migration/repeatables installed |
| Daily tables | 12/12 present, RLS on, no direct role DML |
| Daily RPCs | 13/13 present, service-role-only, fixed definer boundary |
| Candidate flags | 0 enabled of 13; Daily false |
| Candidate core/Daily rows | all zero |
| Candidate private Worker | `689bbe95-bf31-4f91-8e5a-40289558cefa`, 100% |
| Candidate public broker | `18f67f8e-3ca2-46ad-9599-8512894de6c3`, 100% |
| Broker health/readiness | HTTP 200 / HTTP 200 |
| Unsigned Daily route | HTTP 401; no business action |

Safe migration workflow `31981114093` successfully installed the Candidate prerequisite migration and new repeatable, then stopped in an existing Banking Pay catalogue verifier on exactly three declared out-of-band James definition hashes. It did not report Candidate drift. Those James definitions were not changed, reinstalled or reverted. The Candidate install was independently verified read-only through TEST catalogue, ledger, ACL, definition-hash, flag and row-count checks.

## 15. No-change boundary

R8 does not change:

- legacy Google browser UI/login/`msisdn` behaviour;
- Availability or NEW MASTER ROTA Apps Script source, deployments, triggers, properties, Sheets or data;
- existing Candidate authentication/session/workflow/Office owners except the additive bootstrap Daily capability read;
- Candidate Office frontend;
- Candidate responsive web, iOS or Android UI;
- financial calculations, rate/pay/charge/VAT/ERNI/margin/TSFIN;
- Process/Authorise/invoice/payment authority;
- Banking Pay, Policy X, provider, settlement or remittance;
- production.

No real email, push, R2 business write, external effect or Candidate business mutation was executed.

## 16. Remaining full-app phases

| Phase | Remaining outcome |
| --- | --- |
| Independent R8 gate | Prove Phase 2/1B against source, installed TEST and adversarial integration journeys |
| Phase 3 | Minimal Availability Apps Script adapter, Master Rota signed publisher, projection/effect adapter, coexistence outage/recovery proof |
| Phase 4 | Complete Daily UI in responsive web/iOS/Android, specialist journeys and shadow parity |
| Phase 5 | Controlled TEST cutover with identity, parity, soak, error-budget and rollback evidence |
| Phase 6 | Full Emergency/cannot-attend/leave-early/running-late/DNA/messages/content/Past Shifts and DAILY signing/EMAIL/PHONE acceptance through old and new paths |
| Phase 7 | Gradual entitled rollout, monitoring, then separately authorised legacy-browser/compatibility-adapter retirement |

The full Candidate App also retains all previously accepted non-Daily Candidate/Office/authentication/workflow functionality. R8 does not narrow the project to Daily availability.

## 17. Later-controlling R9 authority-transition correction

R8 remains the installed Phase 2/Phase 1B architecture authority except for the transition-proof statements in Sections 8 and 14. The independent R8 audit found that the original implementation did not execute enough of the accepted cutover/rollback proof inside the locked database owner. The later-controlling R9 correction therefore replaces any R8 inference that a caller-supplied generation snapshot or in-flight disposition was sufficient.

R9 changes only `public.candidate_daily_authority_transition_atomic_v1` and its executable verification. It does not add a table, public RPC, HTTP route, Google adapter, UI, entitlement, source link, Candidate row, external effect or financial authority.

For every authority-changing item, the corrected database owner now:

1. takes the existing environment/batch receipt lock and owns exact replay/conflict;
2. locks existing Candidate scope rows in deterministic Candidate order and rejects a missing scope without creating one;
3. locks the global Candidate feature configuration and exact Candidate entitlement;
4. locks and proves one current active PRIMARY legacy source identity;
5. locks and proves the exact active fourteen-day generation, its identity/version, completion, publication and 120-second freshness;
6. locks and proves the Candidate sync row, exact accepted/required/effective cursors and a reconciliation watermark not older than generation, availability or outbox facts;
7. validates any deferred overlay against the exact active generation/date/source-row hash;
8. locks and classifies projection rows, command receipts, other transition batches and external-effect receipts;
9. derives `NONE`, `RECONCILED` or `DRAINED` from those locked rows and rejects a contrary caller assertion;
10. freezes the database-winning generation, sync and derived-disposition facts into the immutable transition ledger;
11. clears the transition fence on every expected per-item rejection through a subtransaction, while unexpected failures abort the batch;
12. returns one durable result for exact replay and one stale-precondition rejection for a losing different-key concurrent cutover.

The corrected owner deliberately never derives `CANCELLED`. A caller may not use `CANCELLED` to bypass unresolved work. `PENDING`, `CLAIMED`, `RETRY`, `TERMINAL`, in-progress command/batch state and `IN_PROGRESS` or `UNKNOWN` external effects all prevent a strict switch. A valid current-generation `DEFERRED_OVERLAY` is the only state that derives `RECONCILED`; otherwise fully settled state derives `DRAINED`.

The supported forward/rollback sequence is now:

```text
GOOGLE_PRIMARY
  -> SUPABASE_PRIMARY
     requires exact source/generation/cursor/freshness/in-flight proof

SUPABASE_PRIMARY
  -> ROLLBACK_PENDING
     requires the global Candidate Daily switch and entitlement off

ROLLBACK_PENDING
  -> GOOGLE_PRIMARY
     requires the same exact source/generation/cursor/freshness/in-flight proof
```

An exact no-op does not add a transition row. A missing scope cannot bootstrap itself through the transition RPC. A partial cohort may commit valid items and reject invalid items, but no item may retain `transition_in_progress=true` after completion.

The executable R9 authority is:

```text
supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql
tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql
tests/candidate-daily-authority-transition-concurrency.integration.js
tests/candidate-daily-phase2-source-contract.test.js
```

`CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md`, `R9_FINDING_CLOSURE_MATRIX.md`, current Decisions PDF Sections 80-84 and decisions AV-229 through AV-244 are later-controlling wherever an earlier R8 statement conflicts. Publication, installed TEST hashes, workflow identities and deployed Worker versions are recorded in the final R9 current-state and verification documents rather than retroactively altering historical R8 evidence.
