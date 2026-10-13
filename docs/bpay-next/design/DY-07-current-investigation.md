# DY-07 current investigation — sequence94

11 October 2026. Read-only source investigation for the next scheduled item. No product, checklist, database, SQL execution, test, screen, fixture or hosted action. **Two raw-finaliser bypasses and exact20-second caller gaps remain; no implementation or app PASS claimed.**

## Authority / exact current parents

Full approved card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1041–1076`, checklist D01–D05 at `IMPLEMENTATION_CHECKLIST.md:2610–2626`, master sequence94 at `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:178`, F03 dependency and backend AGENTS single-flight contract govern. Preserve current Source/query admission and native permissions/replay semantics. No screen, new finalisation process, operational LEGACY parity gate or money owner is required.

| Exact owner/caller | Current source evidence | Remaining work |
| --- | --- | --- |
| public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) | Registered final physical/root `10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:1019–1124`; registry13030+ | Reuse unchanged. Do not reinstall obsolete04092026_1420 body. |
| public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamptz) | Sole saved complete definition `11082026_1832_cloudtms_office_candidate_adapter_v1.sql:1068–1637`, finalise branch1256–1284, ACL1638–1649; current-contract identity35601 | Complete exact-parent successor changing only nested finalise callee plus preserved closure/ACL. No registry entry for this exact adapter was found in the current final-definer file; record its source parent/final successor explicitly. |
| finaliseCandidateDailyThroughCanonicalAuthority | `broker/src/index.js:28286–28325`, injected as finaliseDaily198439 | Non-Office raw atomic call28324 must become existing single-flight RPC with same arguments/key and20s option; Office adapter invocation needs the same20s bound. |
| Candidate native finalise/replay funnel | `broker/src/candidate-app-backend.js:5934–6079,10856–10864` | Existing non-Office wrapper calls already20s. Office FINALISE_EXECUTE/FINALISE_REPLAY_LOOKUP currently omit timeout options; scope them to20s without changing all other Office actions. |

The saved current contract confirms the adapter is SECURITY DEFINER, postgres owner, VOLATILE, with its original search_path and service-only ACL. This is source/contract provenance, not a fresh installed database definition query. Root's existing normal compile/catalogue/contract checks remain necessary.

## Existing wrapper is correct reuse

Current0931 wrapper validates native workflow/generation1037–1039. Its SP12 NEXT block1042–1077 retains plain current authentication/Office service context, feature/key/session checks and weekly_source_pay_query_admit_v2 **before** either finalisation business gate. Global pg_try_advisory_xact_lock1082–1095 and workflow/generation try gate1098–1110 return the original closed FINALISATION_PENDING/finalisation_pending/single_flight_deferred receipt immediately on contention. Only after both gates does1111–1120 delegate to the current raw atomic authority with all eight original arguments.

The raw atomic call inside this wrapper is the intended private execution seam; it is not another bypass to remove. No replacement latch, R2 namespace, queue, background revision, deferral table, new command or global database setting is required. Keep the original wrapper and atomic Source order byte-identical.

## Required minimal integration

Office adapter1256–1284 currently combines FINALISE_EXECUTE and FINALISE_REPLAY_LOOKUP, validates workflow/generation/key, opens original RETRY_FINALISATION service context, calls **raw atomic**1267, appends original OFFICE_CANDIDATE_MUTATION_RESULT_V1, closes context and returns. Replace the callee with single-flight only. Preserve expected_row_signature, stable idempotency key, actor injection, replay_probe_only versus replay_key_probe_only conditions, original JSON merge, service context close and every other adapter branch. A deferred wrapper result must still reach context close and retain its flags. Preserve original permissions/feature/guard-refusal behavior and service-only grants.

The non-Office Daily seam28324 also calls raw atomic. Its already-built daily_materialisation_json, NULL expected-row-signature policy, workflow/generation, environment/session and stable `${idempotencyKey}:finalise` business key stay unchanged; changing the RPC name must not regenerate the business key or fetch replacement economic input on an unknown result. No raw fallback on wrapper contention/timeout is justified.

## Exact20-second /202 contract

The current index seam passes no options to either finalisation RPC. Generic sbRpc165008–165032 defaults these unmapped functions to **no timeout**, so existing platform bounds are not the card's20s contract. Pass the existing `{timeoutMs:20000}` transport option for both finalisation branches. Native Candidate non-Office replay/execute already use MANAGER_FINALISATION_RPC_TIMEOUT_MS=20000 at141 and5958/5989/6077. Its common officeAdapter10856–10864 passes no options: qualify only FINALISE_EXECUTE/FINALISE_REPLAY_LOOKUP with that same existing constant.

The same Daily construction seam `buildCandidateDailyAtomicMaterialisation:28146–28284` sends BEGIN_CANONICAL_DAILY_SAVE through Office WORKFLOW_ACTION_EXECUTE or candidate_workflow_transition_atomic_v1 at28171–28184 without a timeout. Applying20s to these two actual calls is narrowly consistent with the full HTTP/RPC bound; retain their original fresh begin-context key and authority/process, rather than broadly changing generic Office calls or introducing a new acquisition phase.

Native safeFinalisationResult5742–5762 already preserves finalisation_pending and single_flight_deferred. Candidate retry handler8358 and Office retry11295 already choose202 when deferred, otherwise200. Manager PHONE/EMAIL paths already return202 while native background completion proceeds9070–9084 and11349–11370. Keep those mappings and existing errors; a timeout/unknown result is not proof of failure or success. Same-business-key retries/replay remain with the existing owner.

The existing non-waiting Manager R2 lease3337+ already wraps finaliseWorkflow for actual manager/Office/recovery HTTP routes; preserve it. It is not a substitute for the required database global/per-generation wrapper, and it does not justify another gate framework.

## Protected-boundary STOP-CHECK

Read `supabase/release/protected-boundary-lock.json` before product editing. Its current file list contains exactly five browser-isolation/security files, not index.js, candidate-app-backend.js, the1832 adapter or0931 wrapper. Static raw-SHA comparison in this review found **all five exact listed hashes match**. No locked file edit or object rewrite is proposed; the current format lists file paths and has no separate locked-object array.

Retain the frozen security definitions/verification, no browser access broadening, and the existing protected check in scripts/cloudtms-db-release-lib.mjs:562–580. The original adapter/wrapper signatures remain the same. If root's actual planned edit touches another locked file/object, the card's stop-and-ask rule applies; the current selected scope does not itself require another approval. New Candidate-named definers would require current-release security inventory changes in the same change; this minimal design replaces one existing adapter rather than adding such a definer. Update exact successor provenance and normal contract/security receipts without weakening the frozen files.

## Smallest build / pending acceptance

Reuse current single-flight D01. One exact complete adapter successor closes D02; existing index Daily finalise call/options plus narrowly required native Office timeout options close D03. D05 must record the pre-edit protected lock/hash check. No migration, new screen, producer, Source/query admission change or extra financial owner is necessary.

D04 remains future joined NOT_RUN: genuine Candidate/manager/Office HTTP finalise, concurrent global/per-generation contention returns prompt pending202, each relevant RPC20s, original key retained, unrelated login/reads responsive and no raw bypass. Normal source/ACL/compile/contract checks do not substitute for those joined journeys, and no new fixture/race/timing programme is proposed here.

## Inspected bytes

| Saved source | Raw SHA-256 |
| --- | --- |
| index.js | 8decf8f784118c153e4e5d2db0848050812b8adab72e5b8b4a53a2e48f2358c0 |
| candidate-app-backend.js | 162dc06cead09de49fc9d7b60f1189eac3c5420544f7872bb158a428f4e8379b |
| 10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql | 7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b |
| 11082026_1832_cloudtms_office_candidate_adapter_v1.sql | c4c750e434cbfa8d6c60cb276dafd0a9a00f9ac6d21b3578aeb722c2013a96da |
| protected-boundary-lock.json | 60d2d3d8a74535cf52c5213803d19b0654af48955b6096726e2b7a882545f30c |
| final-definer-closures.json | 16f2523c54d4f44b719b11f01c2f407212f0258187f1c9f27ed2f21f08869a99 |

**Result: the narrow missing work is two callee corrections and exact existing caller timeout propagation; wrapper/gates/replay/process stay with their current owners.**
