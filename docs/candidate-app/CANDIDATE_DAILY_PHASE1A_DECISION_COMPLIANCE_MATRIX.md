# Candidate Daily R6 Decision and Compliance Matrix

## How to read this matrix

The accepted R5 `02_DECISION_LEDGER.md` and `03_DECISION_COMPLIANCE_MATRIX.md` remain controlling for decisions **AV-001 through AV-154**. R6 does not rewrite or renumber them. The R6 package contains those complete documents unchanged under `baseline_r5/`.

This document records the newly evidenced/implemented decisions **AV-155 through AV-180**. Together, the R5 ledger/matrix plus this R6 matrix constitute the complete current decision inventory.

Status meanings:

- **PASS** — implemented or evidenced in the current Phase 1A source/evidence boundary;
- **PRESERVED** — existing accepted behaviour was checked and not changed;
- **DARK** — the contract exists but cannot perform the business operation until later gates;
- **DEFERRED BY DESIGN** — explicitly belongs to a named later phase and is not an incomplete Phase 1A mutation;
- **NOT AUTHORISED** — the action was deliberately not performed.

## R6 decisions

| ID | Decision | Current compliance | Proof owner | Status |
| --- | --- | --- | --- | --- |
| AV-155 | The authorised Google Evidence Gate covers only the Availability API and NEW MASTER ROTA systems needed for Candidate Daily/Emergency coexistence; unrelated Google functions stay out of scope. | Both projects inventoried read-only; scoped gate document excludes unrelated behaviour. | `GOOGLE_EVIDENCE_GATE_20260816.md` | PASS |
| AV-156 | Effective current `Code.gs` source must match the user-certified source before it may be used as patch authority. | Availability and Master Head sources matched their certified attachments byte-for-byte. | Google gate hashes and R5 certified-source authority | PASS |
| AV-157 | Unredacted Apps Script source and Script Property values must not enter the review handover. | Only hashes, structural facts and R5 sanitised sources are distributed. | package manifest/content inspection | PASS |
| AV-158 | The Availability API active web deployment is version 215 and matches current Head. | Effective deployment and Head hash compared in the authenticated read-only gate. | Google gate | PASS |
| AV-159 | Historical R6 fact: Master Rota active web version 100 differed from current Head while triggers executed Head. | Explicitly recorded and later superseded by AV-189 after the user deployed current Head as version 101. | Google gate plus user deployment evidence | SUPERSEDED |
| AV-160 | `ai_startDailyPings` is an orphaned legacy reference, not Candidate Daily scope, and must not be revived incidentally. | No declaration/trigger found; no code added or removed. | Google gate and certified-source search | PASS |
| AV-161 | Do not modernise the temporary legacy client; contain it behind the existing Apps Script server and the later narrow signed compatibility adapter. | No legacy browser/UI/auth change; Phase 1A exposes no system credential to it. | source diff/no-change boundary | PASS |
| AV-162 | Emergency and other retained Google consumers must work with both the temporary legacy app and the new Candidate App during coexistence, through one later CloudTMS business/effect authority. | Architecture preserved; no existing Emergency function changed. | R5 emergency mapping plus Google gate | PRESERVED |
| AV-163 | `CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml` remains the sole Phase 1A API authority. | Runtime catalogue compared exactly to 24 new operations; API SHA frozen. | route-parity evidence | PASS |
| AV-164 | Existing Candidate bootstrap is the 25th Daily-related merged operation and must remain additive/baseline compatible. | Existing bootstrap members are deep-preserved and only the Daily capability is added. | focused TAP | PASS |
| AV-165 | Phase 1A adds exactly 11 Candidate Daily routes and 13 trusted Google-system routes; unlisted paths/methods fail closed. | One closed catalogue and negative path/method tests. | runtime catalogue/focused TAP | PASS |
| AV-166 | All new routes remain unreachable for business use until later authority is installed and enabled. | Candidate routes return disabled; signed system routes verify transport then remain dependency-dark. | real public/private dispatch tests | DARK |
| AV-167 | Candidate Daily capability is `capabilities.daily_availability`; current value is disabled with `GLOBAL_DISABLED`, without removing existing bootstrap capabilities. | Additive capability composer proven against existing members. | focused TAP | PASS |
| AV-168 | Candidate correlation IDs may be accepted when valid or generated before processing; signed system correlation IDs are signed, mandatory and never replaced. | Candidate ULID middleware and signed-system verifier implemented. | runtime source/vector tests | PASS |
| AV-169 | `Idempotency-Key` is the sole transport command key; reads forbid it and bodies may not duplicate it. | Shared rule applied to Candidate and signed-system transport. | focused TAP/R5 vectors | PASS |
| AV-170 | Candidate/system body sizes are fixed at 32 KiB/256 KiB with strict content/framing/UTF-8 JSON-object rules. | Public and private boundaries enforce declared/actual size, JSON object, content type/encoding and BOM rules. | focused TAP/R5 raw-parser vectors | PASS |
| AV-171 | Route rates are 60/12/6 per minute for Candidate read/command/effect and 120 per minute per system key ID. | Four committed broker rate-limit bindings and route-class selection. | Wrangler dry build/config tests | PASS |
| AV-172 | R5 deadlines are explicit and applied to public-to-private Daily calls: 10 seconds DB/command, 12 seconds read/preview, 20 seconds external effect. | Route metadata and `AbortSignal.timeout` service-binding deadline. | source/test/build evidence | PASS |
| AV-173 | Distributed in-flight limits must never be faked with isolate-local counters; Phase 2 receipts/leases are the durable owner before activation. | Cardinalities frozen in route metadata; all routes dark; activation blocked pending Phase 1B integrated proof. | implementation authority | DEFERRED BY DESIGN |
| AV-174 | Google-system HMAC v1 signs the exact accepted R5 fields and raw bytes; the public broker owns neither keys nor replay storage. | Exact R5 fixture used by production modules; secrets referenced only in private Worker environment. | Node/Python vectors and end-to-end private test | PASS |
| AV-175 | The private Worker accepts PRIMARY and optional OVERLAP key slots, requires ±300 seconds, compares body digest without early-exit length drift and verifies HMAC with WebCrypto. | Implemented in `candidate-daily-hmac-v1.js`. | negative/rotation/config tests | PASS |
| AV-176 | A verified signed request atomically consumes one R2 nonce in the dedicated versioned namespace for at least ten minutes; replay fails generically. | Conditional `put`, dedicated key, scheduled cleanup and same-nonce test. | private verifier test | PASS |
| AV-177 | Transport nonce replay and later business idempotency are separate; legitimate business retry uses a fresh nonce and the same operation key. | Phase 1A owns only nonce authority; no substitute business receipt was added. | code/no-table boundary | PASS |
| AV-178 | Phase 1A may not author/install Daily SQL, edit/deploy Google, enable flags/entitlements or deploy Workers. | None performed. | Git/Google/TEST safety record | NOT AUTHORISED |
| AV-179 | Phase 2 remains twelve additive tables and thirteen service-only RPC owners, authored only after independent R6 review; TEST installation remains separate. | No SQL file created; R5 design retained. | changed-file inventory | DEFERRED BY DESIGN |
| AV-180 | Full completion still requires Phase 1B broker-to-RPC integration, Phase 3 minimal Google coexistence, Phase 4 app UI, controlled cutover, specialist acceptance, gradual rollout and later explicit legacy decommissioning. | R6 handover lists each gate and does not claim the full app is complete. | handover/Decisions PDF | DEFERRED BY DESIGN |
| AV-181 | Every Daily/bootstrap public error uses the exact R5 route/status/code/retry matrix, a fixed safe message, valid ULID and no details or one typed closed details variant. | Runtime loads and exhaustively tests the 25-operation error matrix. | focused R7 TAP | PASS |
| AV-182 | The public broker must reconstruct allowlisted Daily/bootstrap error fields and safe headers; it never forwards arbitrary private JSON or success drift. | Route-aware rebuilding and adversarial field/schema tests. | focused R7 TAP/source review | PASS |
| AV-183 | Origin, native-client and preflight/header-policy rejection remains public 403 `FORBIDDEN`/`DO_NOT_RETRY`. | All browser rejection paths asserted at 403. | focused R7 TAP | PASS |
| AV-184 | Signed-system pre-auth always consumes an IP bucket plus accepted-key-ID or shared-invalid-key bucket; attacker key-label rotation cannot mint buckets. | 150 rotating invalid key IDs cause only the bounded first 120 requests to reach private verification. | focused R7 TAP | PASS |
| AV-185 | Nonce retention age starts at successful server consumption, not the caller timestamp, and cleanup retains 599 seconds and permits cleanup at 600 seconds at both clock-skew edges. | Stored consumption epoch plus R2 upload fallback; boundary tests. | focused R7 TAP | PASS |
| AV-186 | Signed-system correlation is a valid supplied ULID or the request is rejected before private work with a newly generated valid response ULID. | Missing/invalid/preserved correlation tests. | focused R7 TAP | PASS |
| AV-187 | Supplied observable `Content-Length` must exactly equal the actual received bytes for Candidate and signed-system paths. | Declared shorter/longer and exact framing tests. | focused R7 TAP | PASS |
| AV-188 | Query canonicalisation uses explicit ASCII/code-unit tuple ordering in JavaScript and Python; Fetch-normalised header behaviour is the deployed claim. | Eight query vectors in Node/Python plus platform-level duplicate-header rejection test. | R7 vectors/focused TAP | PASS |
| AV-189 | User-deployed Master Rota active web version 101 represents the certified current Head as of 16 August 2026. | Operational fact recorded; Phase 3 fresh hash/deployment check still mandatory. | user deployment evidence | PASS / RECHECK BEFORE GOOGLE EDIT |
| AV-190 | Retiring the temporary legacy browser and `LEGACY_COMPAT` facade does not retire Availability, Emergency, Master Rota, signed synchronisation, projection/freshness or specialist owners. | Explicit lifecycle separation in Decisions PDF, implementation plan and Google gate. | documentation review | PASS |
| AV-191 | R7 may deploy only the dark Candidate public/private transport with flags false; it adds no SQL/RPC, Google edit, UI, entitlement or business effect. | Changed-file and deployment boundary. | Git/deployment/safety evidence | PASS |
| AV-192 | Phase 2 begins only after independent R7 GO; subsequent Phase 1B/3/4 and rollout gates remain separate. | No Phase 2 SQL/RPC exists in this correction. | source inventory/handover | DEFERRED BY DESIGN |

## R5 decision-family impact summary

This summary does not replace the 154-row R5 matrix; it records how the implementation affects each accepted family.

| R5 family | Phase 1A outcome |
| --- | --- |
| Product scope and single authority (AV-001–AV-018) | Preserved; dark transport only, no second business/data authority. |
| Canonical version/generation/cursor design (AV-019–AV-038) | Unchanged; Phase 2 owner remains pending. |
| HMAC, nonce, correlation and error authority (AV-039–AV-046, AV-100–AV-106) | Implemented and vector-verified at the public/private boundary. |
| Availability, rota and projection composition (AV-047–AV-073) | Contracts frozen; no data composition performed before Phase 2/1B. |
| Audit, rollout and TEST-only controls (AV-074–AV-088) | Preserved; no deployment/mutation/effect. |
| Limits, performance and concurrency (AV-089–AV-094) | Rates/body/deadlines implemented; durable distributed in-flight/receipt proof remains Phase 2/1B and routes stay dark. |
| Entitlement and feature gate (AV-095–AV-100) | Additive disabled capability implemented; no entitlement inferred or enabled. |
| External effects and emergency zero-loss (AV-107–AV-123) | Effect routes and transport/idempotency seams frozen; no effect executor exists or ran. |
| Merged API/source/legacy/browser authority (AV-124–AV-154) | Exact merged route parity, minimal legacy containment, certified-source gate and no-browser-credential boundary proven. |

## Zero-drift confirmations

| Boundary | Result |
| --- | --- |
| Existing Candidate auth/session/workflow/Office business functions | unchanged |
| Existing Candidate public/private routes | preserved |
| Candidate business tables/RPCs | unchanged |
| Availability browser/UI/login | unchanged |
| Availability Apps Script | unchanged |
| Master Rota Apps Script, triggers and Sheet data | unchanged |
| Emergency behaviour | unchanged |
| Finance, rates, pay, charge, VAT, ERNI, margin, invoices | unchanged |
| Banking Pay, Policy X, provider, settlement, remittance | unchanged |
| TEST feature flags/entitlements | unchanged |
| Production | untouched |
