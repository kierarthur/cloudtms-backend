# Candidate Daily Phase 2 and Phase 1B R8 Decision Compliance Matrix

## Complete decision inventory

The complete current decision inventory is cumulative and consecutive:

- AV-001 through AV-154: accepted R5 `02_DECISION_LEDGER.md` and `03_DECISION_COMPLIANCE_MATRIX.md`;
- AV-155 through AV-192: R6/R7 `CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md`;
- AV-193 through AV-228: the original R8 implementation matrix;
- AV-229 through AV-244: the later-controlling R9 database-owned transition-barrier correction;
- AV-245 through AV-249: the later-controlling R10 first-rollback unresolved-work correction.

The sealed R8 handover includes all three layers. Earlier rows remain controlling unless a later row expressly records a supersession. Historical R6/R7 status documents are evidence of their phase and are not current-state authority.

Status meanings:

- **PASS**: implemented and evidenced in the current source/installed TEST/deployed runtime boundary;
- **PRESERVED**: earlier accepted authority was regression-checked and not changed;
- **DISABLED**: implementation exists but product prerequisites deliberately make it unavailable;
- **DEFERRED BY DESIGN**: named later phase, not omitted R8 work;
- **NOT AUTHORISED**: deliberately not performed.

## R8 decisions AV-193 through AV-228

| ID | Atomic decision | R8 compliance | Evidence owner | Status |
| --- | --- | --- | --- | --- |
| AV-193 | Phase 2 adds exactly twelve Daily tables and no thirteenth table. | Migration creates the exact approved table list; catalogue reports 12/12. | schema migration, source-contract and TEST catalogue | PASS |
| AV-194 | Phase 2 adds exactly thirteen public Daily RPC owners and no fourteenth RPC. | Repeatable exposes the exact approved function list. | repeatable, source-contract and TEST catalogue | PASS |
| AV-195 | All Daily tables are closed against direct `anon`, `authenticated` and `service_role` DML. | RLS enabled; no permissive policies; direct DML privileges false for all three roles. | post-install ACL query | PASS |
| AV-196 | All public Daily RPCs are security-definer, closed-search-path and executable only by `service_role`. | 13/13 installed functions have the expected ACL and configuration. | post-install function catalogue | PASS |
| AV-197 | `candidate_daily_enabled` is additive, defaults false and is never enabled merely by migration/deployment. | TEST flag value is false; all 13 Candidate flags are false. | migration and TEST snapshot | PASS / DISABLED |
| AV-198 | Signed-system continuity routes do not consult the Candidate product flag. | HMAC/system handlers dispatch to RPC authority independently; session/global switch is not a signed-system prerequisite. | production module and focused tests | PASS |
| AV-199 | Candidate Daily reads/writes require flag, exact entitlement, Candidate identity, authority mode, complete generation and freshness. | One DB capability/helper and operation-specific RPC guards own the complete conjunction. | helper/RPC runtime tests | PASS / DISABLED |
| AV-200 | The existing bootstrap is additive and reads Daily capability from the same database owner as business routes. | `candidate_app_bootstrap_v1` calls the Daily capability helper and retains its baseline fields. | amended bootstrap repeatable, baseline preservation tests, installed definition hash | PASS |
| AV-201 | The temporary legacy browser receives no CloudTMS access/session/HMAC/Supabase authority and cannot nominate a Candidate UUID. | No browser/Google source changed; future seam requires signed server source-link resolution. | no-change boundary and source-link RPC design | PASS |
| AV-202 | Legacy source identity resolves only through the approved environment/source mapping to one canonical Candidate. | Private source-link table/helper rejects absent, disabled, ambiguous or mismatched mapping. | schema/RPC runtime tests | PASS |
| AV-203 | Source mapping stores bounded digests/identity facts, never raw browser tokens, passwords or CloudTMS credentials. | Column/contract inspection and negative source tests. | migration and source contract | PASS |
| AV-204 | One immutable generation header and exact day rows own current rota truth. | Batch receipt plus generation/day constraints and publish RPC implement this authority. | SQL runtime matrix | PASS |
| AV-205 | Candidate Daily requires one complete current fourteen-day generation; partial/missing/stale generations fail closed. | Publish/read/write tests exercise completeness and mismatch failures. | PostgreSQL runtime test | PASS |
| AV-206 | Command semantic identity excludes generated transport detail and exact replay returns the durable database result. | Request hashes bind factual identity; receipt returns stored result with internal replay marker. | command receipt tests and Phase 1B tests | PASS |
| AV-207 | Same key plus changed factual request conflicts across Candidate and legacy command paths. | Receipt request-hash comparison under shared lock. | concurrency/idempotency SQL tests | PASS |
| AV-208 | Generation/batch publication has a separate durable batch receipt and does not reuse browser/session time as identity. | Batch table and publish RPC own request hash and result. | SQL runtime tests | PASS |
| AV-209 | Google projection work is owned by one durable outbox with bounded leases, retries and terminal/parked states. | Claim/complete RPCs fence lease token and attempt state. | PostgreSQL runtime tests | PASS |
| AV-210 | Durable cursor and effective-visible cursor have one owner in `candidate_daily_sync_state`. | No duplicate cursor column/owner exists elsewhere. | schema/source duplication checks | PASS |
| AV-211 | `DEFERRED_OVERLAY` requires the exact current generation/hash proof. | Completion rejects stale or mismatched overlay proof. | SQL runtime tests | PASS |
| AV-212 | Removing/changing an overlay retreats effective visibility and requeues eligible parked projection work. | Reconciliation/transition logic and runtime test cover retreat/requeue. | SQL runtime matrix | PASS |
| AV-213 | Authority mode is exactly GOOGLE_PRIMARY, ROLLBACK_PENDING or SUPABASE_PRIMARY. | Scope/transition constraints and closed response enum enforce only these values. | schema/OpenAPI/contract tests | PASS |
| AV-214 | Mode transition is fenced by expected mode/version, current generation, cursor/freshness and reconciliation barriers. | R8 source did not fully implement this claim. R9 makes every authority-changing commit depend on locked database-owned generation, source, cursor, reconciliation and in-flight facts. | R9 direct SQL and parallel runtime suites | PASS IN R9 |
| AV-215 | Rollback is explicit and auditable; it never deletes receipts or silently makes stale Google projection authoritative. | R8 source did not fully implement this claim. R9 requires the product switch off before rollback starts, derives in-flight disposition inside PostgreSQL, and requires exact current generation/cursor/parity proof before Google becomes primary again. | R9 direct SQL and parallel runtime suites | PASS IN R9 |
| AV-216 | External Emergency/specialist effects use one claim/lease/completion/status receipt and exact replay. | Three effect RPCs plus private receipt table own the lifecycle. | SQL and Phase 1B contract tests | PASS |
| AV-217 | R8 executes no real external effect and does not pretend a missing specialist dependency is success. | Specialist seam returns typed dependency unavailable until Phase 3/6 adapter is supplied. | strict fake/dependency tests and safety record | PASS / DEFERRED BY DESIGN |
| AV-218 | Phase 1B maps every accepted Candidate/system operation to a closed installed RPC or typed specialist seam. | `candidate-daily-phase1b.js` owns one operation catalogue; route parity tests pass. | source and focused tests | PASS |
| AV-219 | The private Worker derives Candidate identity from the authenticated session and never trusts a request-owned Candidate UUID. | Authenticated dependency/RPC args are server selected. | private handler tests | PASS |
| AV-220 | The public broker rebuilds strict allowlisted success/error bodies and never forwards DB/internal receipt fields. | Response validators/rebuilders reject drift and remove internal replay/lease/hash/version detail. | focused adversarial tests | PASS |
| AV-221 | Database idempotent replay remains distinct from transport-nonce replay. | DB response uses an internal marker; each HTTP retry still needs a valid fresh transport nonce. | integration/source tests | PASS |
| AV-222 | Candidate Daily read/command/effect/system rate classes remain separate. | Dedicated Worker rate-limit bindings and route policy mapping exist. | Wrangler configuration and tests | PASS |
| AV-223 | TEST installation is harmless: all Candidate flags false and all seven core plus twelve Daily tables empty. | Read-only post-install snapshot reports zero rows everywhere. | Supabase post-install evidence | PASS |
| AV-224 | R8 changes no Google Apps Script, Sheets, trigger, property, web deployment or legacy browser behaviour. | Changed-file inventory contains no Google source; no Google write occurred. | Git/safety evidence | PASS |
| AV-225 | Active NEW MASTER ROTA v101 remains the certified current source fact, but Phase 3 must recheck both effective Google projects immediately before editing. | No R8 Google assumption substitutes for a fresh Phase 3 gate. | R7 evidence and R8 handover | PRESERVED / RECHECK |
| AV-226 | Retiring the old browser later does not retire Availability, Emergency, Master Rota publication, projection/freshness or specialist services. | Explicit lifecycle rule retained in plan, authority and PDF. | documentation review | PASS |
| AV-227 | Phase 2/1B completion does not mean the full Candidate App is complete. Phase 3–7 remain mandatory. | Current-state/handover name each remaining gate and prohibit feature activation. | handover and PDF | PASS |
| AV-228 | No finance, Process/Authorise, invoice, payment, Banking Pay, Policy X, provider, settlement, remittance or production owner may drift in R8. | Changed-file inventory/tests show Candidate-only boundary; production untouched. | Git diff and safety statement | PASS |

## R9 decisions AV-229 through AV-244

These decisions close the single bounded R8 independent finding. They supersede any R8 wording that implied an authority switch could rely on caller assertions or incomplete database facts.

| ID | Atomic decision | R9 compliance | Evidence owner | Status |
| --- | --- | --- | --- | --- |
| AV-229 | An authority transition never creates a missing Candidate authority scope or silently repairs a missing prerequisite. | The transition locks and requires the pre-existing exact scope; a missing scope produces an item-level fail-closed result and no scope/entitlement/transition row. | `candidate_daily_authority_transition_atomic_v1`, direct SQL suite | PASS |
| AV-230 | All current facts that can invalidate cutover are locked and evaluated inside one database transaction. | PostgreSQL locks the global feature row, deterministic cohort scopes, entitlement, source links, active generation, sync row, commands, other batches, effects and projection outbox before commit. | repeatable source, static source contract, parallel suite | PASS |
| AV-231 | `in_flight_disposition` is a caller assertion only; PostgreSQL derives the actual result and requires equality. | The only derived outcomes are `DRAINED`, `RECONCILED` or `NONE`; caller `CANCELLED` can never manufacture authority. | repeatable and falsified-disposition runtime cases | PASS |
| AV-232 | A forward cutover and completed rollback require exactly one current PRIMARY legacy source in exactly one active link group. | Missing, expired/disabled, ambiguous and cross-group source authority fail closed under locked source-link rows. | direct SQL source-state matrix | PASS |
| AV-233 | An authority switch requires the exact expected active generation, version and complete fourteen-day published generation. | Missing, BUILDING/partial, stale, wrong-ID, wrong-version or incomplete-day generations are rejected. | direct SQL generation matrix | PASS |
| AV-234 | The accepted, required-visible and effective-visible cursors must all equal the locked scope canonical version and the caller's exact expectations. | Missing sync, lagging/elevated cursor, non-READY state, pending/retry/terminal counts or missing source revision/reconciliation time fail closed. | direct SQL cursor/freshness cases | PASS |
| AV-235 | Reconciliation evidence must be at least as new as every relevant generation, availability and projection fact. | PostgreSQL computes the latest locked fact timestamp and refuses an older reconciliation watermark. | repeatable and runtime freshness proof | PASS |
| AV-236 | `DEFERRED_OVERLAY` proves visibility only when generation ID/version, date and source-row hash match the exact active generation. | Invalid overlay rejects; an exact current overlay is derived as `RECONCILED` and is frozen in the transition. | direct SQL invalid/valid overlay cases | PASS |
| AV-237 | Pending, claimed, retry, terminal, other-batch, command and in-progress/unknown effect work blocks an authority switch. | Every listed owner is counted under row locks; a terminal projection is never treated as drained and UNKNOWN effect truth never becomes success. | direct SQL in-flight matrix | PASS |
| AV-238 | Entitlement and mode changes remain closed and feature-controlled. | Entitlement cannot be true in Google or rollback mode; enabling requires the global switch; rollback from Supabase requires the global switch disabled first. | direct forward/flag/rollback cases | PASS |
| AV-239 | Rollback is two-stage: disable/stop Supabase authority, enter `ROLLBACK_PENDING`, then prove current Google parity before completing. | The first stage cannot carry an enabled entitlement; the second stage uses the same strict source/generation/cursor/reconciliation barrier as forward cutover. | direct rollback sequence | PASS |
| AV-240 | Every authority-changing committed transition freezes database-winner generation, sync and derived in-flight facts in the immutable ledger. | Transition snapshots are populated from locked rows, not copied from request JSON, and immutable trigger authority is unchanged. | ledger assertions in direct SQL suite | PASS |
| AV-241 | A cohort may be partially accepted only as explicit isolated item outcomes; one failed item cannot leak variables or a transition fence into another. | Per-item subtransactions, complete scalar resets and postcondition tests produce one explicit COMMITTED/REJECTED set with no stuck fence. | direct partial-cohort suite | PASS |
| AV-242 | Same-key concurrency returns one durable replay, while different-key simultaneous authority attempts produce one winner and one stale-precondition rejection. | Independent database sessions prove batch-lock replay and deterministic scope-lock single-winner behaviour. | `candidate-daily-authority-transition-concurrency.integration.js` on PostgreSQL 17.6/18.1 | PASS |
| AV-243 | An exact no-op is observable but creates no transition or authority mutation. | Same mode, entitlement and no source change returns `NO_CHANGE`; non-`NONE` disposition is rejected. | direct SQL no-op/replay cases | PASS |
| AV-244 | The corrected barrier must remain executable on both supported PostgreSQL engines and in the complete Candidate regression workflow. | 43 SQL suites, real-chain authentication, mixed-version tests and transition concurrency pass on PostgreSQL 17.6 and 18.1; the rebased complete JavaScript suite is 613/613. | workflow, local matrix and handover evidence | PASS |

## R10 decisions AV-245 through AV-249

These decisions close the single bounded R9 independent finding. They supersede any earlier wording that allowed database-derived `NONE` to satisfy a mode-changing transition merely because the caller reported it truthfully.

| ID | Atomic decision | R10 compliance | Evidence owner | Status |
| --- | --- | --- | --- | --- |
| AV-245 | Database-derived `NONE` means unresolved work exists and can never authorise an authority-mode change. | After caller/database disposition equality, every item with a changed mode and derived `NONE` returns `CANDIDATE_DAILY_NOT_READY`. | transition repeatable, source-contract test | PASS IN R10 |
| AV-246 | The first rollback edge from `SUPABASE_PRIMARY` to `ROLLBACK_PENDING` is blocked by every unresolved projection, command, other batch and external-effect owner. | Direct SQL covers PENDING, CLAIMED, RETRY, TERMINAL, IN_PROGRESS command, other IN_PROGRESS batch, IN_PROGRESS effect and UNKNOWN effect. | PostgreSQL 17.6/18.1 direct runtime suite | PASS IN R10 |
| AV-247 | A falsified `DRAINED` claim and a truthful unresolved `NONE` claim have distinct stable outcomes. | Mismatch remains `SEMANTIC_REJECTION`; truthful unresolved mode change returns `CANDIDATE_DAILY_NOT_READY`. | direct SQL matrix | PASS IN R10 |
| AV-248 | A rejected first rollback must change no authority, entitlement, transition ledger or residual fence, including under concurrent different-key execution. | Direct SQL asserts all four postconditions; two real sessions race the first rollback and both fail closed with zero transition rows. | SQL suite and Node/PostgreSQL concurrency test | PASS IN R10 |
| AV-249 | R10 preserves valid `DRAINED`/exact `RECONCILED` transitions, exact no-op semantics and every R9 forward/final-rollback barrier. | The existing complete R9 suite remains in the workflow and passes unchanged alongside the added first-rollback matrix. | full Candidate workflow and focused regression | PASS IN R10 |

## Complete R5 decision-family compliance in R8

This summary is a navigation aid; it does not replace the 154-row accepted R5 matrix included in the pack.

| R5 decision family | R8 implementation result |
| --- | --- |
| Product scope/single authority AV-001–AV-018 | One Supabase authority; no browser/Google second database owner added. |
| Version/generation/cursor AV-019–AV-038 | Implemented by generation/day, sync-state, outbox and transition owners. |
| HMAC/nonce/correlation/errors AV-039–AV-046 and AV-100–AV-106 | Corrected R7 transport retained; Phase 1B calls it rather than bypassing it. |
| Availability/rota/projection AV-047–AV-073 | Implemented in Phase 2 RPCs with completeness, lease, overlay and cursor proofs. |
| Audit/rollout/TEST controls AV-074–AV-088 | Receipts/transitions/effects installed; flags/entitlements/data remain disabled/empty. |
| Limits/performance/concurrency AV-089–AV-094 | Durable locks/receipts/leases implement distributed ownership; local matrices pass PG17/18. |
| Entitlement/feature gate AV-095–AV-100 | Database helper/entitlement/flag/mode/generation/freshness conjunction implemented; disabled in TEST. |
| External effects/Emergency AV-107–AV-123 | Durable effect lifecycle installed and wired; concrete effect executor remains later-gated and no effect ran. |
| API/source/legacy authority AV-124–AV-154 | R8 OpenAPI, minimal legacy containment, source mapping and no browser credential boundary preserved. |

## Surface-by-surface decision compliance

| Surface | Implemented now | Still later-gated |
| --- | --- | --- |
| TEST Supabase | complete twelve-table/thirteen-RPC authority | real source links, entitlements, generations and availability data |
| Candidate private Worker | complete RPC composition and strict dependency validation | concrete Google/specialist adapter dependencies |
| Candidate public broker | complete strict routing/rates/public response boundary | enabled Candidate access after rollout prerequisites |
| Existing Candidate bootstrap | Daily capability reads canonical DB helper | capability remains false in TEST |
| Legacy Availability browser | unchanged and functional by design | retirement after accepted rollout |
| Availability Apps Script | unchanged | Phase 3 narrow signed compatibility adapter/projection/effect work |
| NEW MASTER ROTA | active v101 unchanged | Phase 3 signed generation publisher/dual publication proof |
| Emergency/specialists | durable CloudTMS receipt seam exists | Phase 3/6 concrete adapters and end-to-end effects |
| Candidate Daily UI | no R8 UI change | Phase 4 full responsive web/iOS/Android UI and parity |
| Office/finance/Banking Pay | deliberately unchanged | none within Candidate Daily scope |

## Zero-drift confirmation

| Protected boundary | R8 result |
| --- | --- |
| Existing seven Candidate business tables / fourteen Candidate business RPCs | preserved; the Daily additive domain is separately named and scoped |
| Candidate auth/session/workflow/Office contract | preserved; bootstrap only gains database-owned Daily capability |
| Candidate Office frontend | unchanged |
| Availability and Master Rota Apps Script/source/data/deployments | unchanged |
| Legacy browser UI/login/identity behaviour | unchanged |
| Emergency and specialist external systems | no effect invoked or altered |
| Financial calculations and Office Process/Authorise | unchanged |
| Invoice/payment/Banking Pay/Policy X/provider/settlement/remittance | unchanged |
| Production | not accessed or deployed |

## Phase 3 later-controlling status update

The R8/R10 statements above that mark Google source as unchanged remain historically correct for those packages. Phase 3 R12 now supplies, installs and versions independently reviewable source in the live TEST Google projects while the bridge remains false.

| Surface | Phase 3 source result | Live authority |
| --- | --- | --- |
| Legacy Availability browser | no client/login/msisdn/UI change | unchanged and active |
| Availability Apps Script | full revised Code plus additive signed bridge helper installed | deployed version 216; bridge false |
| NEW MASTER ROTA | minimal existing-post seam plus additive dual-publication helper installed | deployed version 102; operator setup helper preserved; bridge false |
| Trigger inventory | no new/removed trigger in source | no trigger edit was performed; re-prove before enablement |
| `ai_startDailyPings` | orphan reference left untouched; no declaration/trigger added | unchanged |
| CloudTMS source links/entitlements/data | source-HMAC derivation frozen; no bootstrap/mutation | absent/disabled until separate gate |
| Emergency/specialist effects | receipt primitives prepared only | existing legacy behaviour unchanged; Phase 6 acceptance pending |

Phase 3 decisions AV-250 through AV-280 are recorded in `CANDIDATE_DAILY_PHASE3_DECISION_COMPLIANCE_MATRIX.md` and the later-controlling Decisions PDF addendum.
