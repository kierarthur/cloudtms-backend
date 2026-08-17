# Candidate Daily Phase 2 and Phase 1B R8 Decision Compliance Matrix

## Complete decision inventory

The complete current decision inventory is cumulative and consecutive:

- AV-001 through AV-154: accepted R5 `02_DECISION_LEDGER.md` and `03_DECISION_COMPLIANCE_MATRIX.md`;
- AV-155 through AV-192: R6/R7 `CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md`;
- AV-193 through AV-228: this R8 matrix.

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
| AV-214 | Mode transition is fenced by expected mode/version, current generation, cursor/freshness and reconciliation barriers. | One atomic transition RPC and immutable audit row own the change. | SQL runtime tests | PASS |
| AV-215 | Rollback is explicit and auditable; it never deletes receipts or silently makes stale Google projection authoritative. | ROLLBACK_PENDING plus transition audit/cursor checks. | transition tests and schema | PASS |
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
