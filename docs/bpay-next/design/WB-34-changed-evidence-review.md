# WB-34 changed-evidence proposal review

13 October 2026. Independent read-only review of Root's exact pre-product proposal and BD176. **DESIGN PASS for the bounded wrapper attachment described below.** No implementation, SQL compile, first use, installed runtime, A4, timing or application acceptance is reviewed or claimed. Only this requested review document was written; no product, SQL/RPC, database data, tests, network, rendering, Git or deployment operation ran.

## Proposal reviewed

The existing public W2 SAVE/CLEAR branch adds `{request_id,candidate_id,state,mappings,facts,unavailable_code}` as `changed_evidence` only after one of the three named `23514` stale refusals. It reads the exact original request's at-most100 rows through original-line/current-same-Work component-key mapping and the current native explicit RESOLUTION_CONTEXT with null intents. Evidence is all-or-none, with honest unavailable fallback. Escaped native DETAIL is bounded to120,000 bytes and the complete normalized command fault to125,000 bytes, below the existing131,072-byte read caps. No new public entry point, table, money owner or calculator is added.

Current source reviewed: public wrapper `13102026_0721_banking_pay_next_frozen_decisions_clear_v1.sql:539–562`; private writer/context `13102026_0537_banking_pay_next_saved_resolution_clear_review_v1.sql:191–369,373–687`; immutable scope validator `12102026_2241_banking_pay_next_resolution_context_v1.sql:8–96`; native basis/active mapping `12102026_2100_banking_pay_next_resolution_remaining_quote_v1.sql:13–62,189–225`; immutable line schema `03102026_1324_banking_pay_next_core.sql:188–208`; broker native-fault classifier `broker/src/bpay-next-resolution.js:407–414`; Worker dispatcher `broker/src/bpay-next-workbench.js:892–903,931`; equivalent FE fault/controller owner `js/bpay-next-resolution.js:408–489`.

## Authorization and transaction boundary

Public W2 already checks service role and active admin **before** its SAVE/CLEAR delegate. Preserve that complete0721 body and all other branches. Catch only SQLSTATE23514 plus exact STALE/POSITION_BASIS_CHANGED/BASIS_CHANGED names around the delegate; never globally catch every check violation, permission failure, replay conflict or unknown outcome. Preserve original SQLSTATE, message, status/retryability and known-refusal classification. The unchanged0537 money subtransaction must unwind before reading detail. Existing accepted replay and absent readback receipts remain their original immutable outcomes and gain no fresh stale detail.

The evidence builder independently validates exact original scope and context bindings: authenticated actor, original args Candidate, actual agency identity and current NEXT owner epoch. This is necessary because0537 can raise STALE at423/441 before comparing supplied context at520–522. Verify every original Work belongs to that Candidate, original revision belongs to that Work and original approved-line PK belongs to that exact revision before deriving any component key. No cross-Candidate, foreign original revision, guessed actor or changed module epoch may expose CURRENT evidence. Missing/inactive/adoption/authority is UNAVAILABLE.

A private helper, if Root uses one to keep the wrapper legible, remains an invoker with fixed search_path and owner-only execution, not a new service/browser read surface. Its own validation cannot rely solely on callers having already reached the delegate. The current registry must identify its exact owner/signature as normal source discipline; this review does not approve a public RPC or a general arbitrary scope/evidence service.

## Exact evidence validation

CURRENT has exact six keys, correct original request_id/Candidate, null unavailable_code, and non-null mappings/facts. Mapping count equals the original aggregate requested row count, at most100. Every mapping identifies exactly one original Work/based-on revision/line/family/code, with no missing, duplicated or undisclosed linked row. Current line is selected only by the original qualified key and same Work's exact approved/applied current revision. STANDARD/ADDITIONAL/ADJUSTMENT compatibility remains native2100 authority. Source-component UUID is a fact, not a unique lookup or replacement fallback; date and metadata mismatch do not establish identity.

For each mapped explicit row require the native reply's Work/current revision/current line/family/code/component key to agree. Group mappings into the existing explicit reader input and reuse `validateResolutionResponse` with the authenticated actor. Both Worker and FE must bind that input to their already normalized **original** command args; the current classifier's existing single payload parameter alone cannot prove original scope. Require native facts actor/agency/Candidate/epoch equality and exact closed nested native shape, decimal/null distinctions and response limits. This validates actual native quote facts without creating a second DTO calculator.

Read with `intent:null` and `reason:null`: retained£18/reason/linked choice stay local unsaved input and are never previewed or submitted against new facts automatically. Current metadata/financial/target/date values may differ and are explicitly changed evidence. They must not overwrite original expected versions/request/context or become an automatic money retry. Preserve all native SOURCE/available schedule/saved TARGET/charge/residual distinctions. Valid native rows may contain null quotes with their existing unavailable codes; CURRENT means current native facts, not a certificate that every price is available. If the policy is to withhold the whole attachment when any needed quote is unavailable, emit UNAVAILABLE rather than fabricate or silently omit that row.

UNAVAILABLE also has exact six keys: original request_id/Candidate, mappings:null, facts:null and a finite permitted code. Prefer existing `BPAY_NEXT_RESOLUTION_CONTEXT_UNAVAILABLE` for failed/malformed/oversized detail; a specifically classified existing native read refusal may be retained. No arbitrary exception detail, query, hint, private row or diagnostics may be forwarded. No CURRENT subset is emitted after one requested counterpart disappears. Discovery's1000 physical sentinel does not expand the original100 admitted-row evidence scope.

## Specific failure fallback

| Failure | Required preserved result |
| --- | --- |
| Foreign actor/agency/Candidate/epoch or invalid original PK ownership | Original known stale refusal; UNAVAILABLE; no facts |
| Work withdrawn, current/apply mismatch, removed bucket/key or native basis ambiguity | Original known stale refusal; UNAVAILABLE; no substitute/newer financial retry |
| Native adoption/metadata/position/quote read failure | Original known stale refusal; native finite unavailable code or CONTEXT_UNAVAILABLE |
| Detail serialization exceeds escaped120,000-byte bound | Replace complete detail with bounded UNAVAILABLE; never truncate facts or raise wire limits |
| Normalized final command fault exceeds125,000 bytes | Replace attachment with bounded UNAVAILABLE; retain original fault code/retryability/outcome_unknown:false |
| Invalid JSON, foreign scope, unknown fields or wrong mapping in native DETAIL | Reject facts and use bounded UNAVAILABLE; preserve the already classified known stale fault |
| An actual command transport/native reply is missing or unknown | Existing unknown-outcome journal/recovery behavior; no invented stale evidence |

**The decoder order is material.** Classify the exact native23514/message pair first and mark the command result known refused. Then validate DETAIL inside its own bounded failure path. In the current Worker900, `attempted=false` occurs only after `resolutionCommandNativeFault` returns; if a new detail validator throws before returning, Worker931 would incorrectly set `outcome_unknown:true`. The attachment parser must therefore return the already classified original native fault with unavailable evidence on detail failure, or the dispatcher must set known refusal before parsing attachment. Malformed ancillary evidence must never erase a proven refusal or retain the request as unknown merely because detail decoding failed.

Measure the escaped DETAIL JSON string, including quoting/backslashes and UTF-8 bytes, with room for the known small PostgREST code/message/hint wrapper. Bound the complete normalized fault separately. A120,000-byte native W1 reply plus mapping can exceed either proposal budget; the finite unavailable fallback is part of correctness. Decoder caps and cumulative row limits remain strict. No raw diagnostic fallback is permitted.

## Scope of approval

The exact proposal preserves authorization, immutable original receipts and financial rollback, and provides genuine same-scope changed native evidence through existing owners. The stated validation and fallback rules are implementation requirements of this proposal, not a new owner gate or a separate programme. Root alone implements; the eventual changed wrapper/helper/decoders must be reviewed against these actual boundaries. Stale staff input, accepted-publication handoff and parent atomic completion remain separate WB-34 obligations. Rule B, Policy X, private0537 money/admission/replay behavior and all unrelated W2 branches stay unchanged.
