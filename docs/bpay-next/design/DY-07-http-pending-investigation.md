# DY-07 HTTP pending mapping investigation — sequence 94

11 October 2026. Read-only repository investigation of the named Daily HTTP finalisation path. **Finding: no additional pending-202 mapping change is required for genuine Daily callers.** Root's current single-flight integration remains subject to its separate source/compile/security review. Product, SQL execution, database, fixtures, tests, hosted calls and application acceptance were not performed by this reviewer; D04 remains **NOT_RUN**.

## Binding scope and boundary

Full card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1041–1069`, all checklist DY-07-D01–D05 `IMPLEMENTATION_CHECKLIST.md:2610–2626`, and master sequence 94 `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:178` govern. The card names the existing single-flight wrapper, Office FINALISE_EXECUTE/FINALISE_REPLAY_LOOKUP and index Daily seam; it requires prompt pending 202, relevant RPCs bounded to 20 seconds, preserved native permissions/replay and a protected-boundary check. It names no screen or new route/process (card1045–1058).

Read `supabase/release/protected-boundary-lock.json`: the five listed browser-isolation files retain exact recorded raw SHA-256 values. Its file-only list contains neither the two Worker files nor the existing Office adapter/single-flight definitions. No locked-file edit is proposed here. The current selected signature replacement introduces no new Candidate-named definer. Future touched locked files/objects still require the card's explicit stop-and-ask rule (1048).

## Exact Daily result path

`broker/src/index.js:198426–198445` injects `finaliseDaily` at198439 into the existing dependency object. `finaliseWorkflow` in `broker/src/candidate-app-backend.js:5993–6053` first performs original same-key receipt and semantic replay probes, obtains native approved identity, then delegates DAILY to that dependency. Both probes derive the same Daily `${idempotencyKey}:finalise` key at5937–5938/5965–5966; only `idempotent_replay === true` is treated as a completed replay at5959/5990. A busy probe is not reported as a completed receipt.

Current index seam28286–28325 retains original materialisation, workflow/generation/environment/session, NULL expected-row-signature policy and the same suffixed business key28307. The non-Office call now reaches `candidate_submission_finalize_single_flight_v1` with `{timeoutMs:20000}` at28324. The Office branch reaches the existing adapter with the identical payload/key and the same timeout28311–28323. No raw fallback or new request identity is present.

The current registered wrapper is `supabase/repeatable/10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:1019–1124`. Its original NEXT authentication/Source admission1039–1075 precedes the global and workflow/generation non-waiting gates1080–1109. Both contention receipts are `ok:true`, state FINALISATION_PENDING, `finalisation_pending:true`, `single_flight_deferred:true`, with the original finite reason. The wrapper's atomic delegation1111–1120 remains its intended execution owner.

Root's current adapter successor `11102026_1119_banking_pay_next_daily_single_flight_adapter_v1.sql:194–222` retains validated identity/key, original RETRY_FINALISATION service context, actor injection, exact replay flags and original result merge. It calls the wrapper205 and closes context221 even for a returned deferred result. The native Office funnel `candidate-app-backend.js:10856–10864` now passes its existing 20000ms constant only for the two finalise actions.

`safeFinalisationResult`5742–5765 explicitly retains both pending flags, state, workflow/generation, replay and finite reason/error fields. Its successful unresolved Office receipt branch5757–5763 removes internal setup/rate diagnostics; it does not remove either pending flag. DAILY applies this shaper to the dependency result6044–6053. No internal materialisation/render contract is exposed.

## Actual HTTP callers and response preservation

| Existing caller | Pending result handling | Required extra edit |
| --- | --- | --- |
| Candidate workflow RETRY_FINALISATION | Native access/selected Candidate check8350–8353; original manager lease8354–8357; `single_flight_deferred === true ? 202 : 200`8358. | None. |
| Manager PHONE/EMAIL approve | Native token/context/action guards9010–9062. Accepted queue branch9063–9066 returns202; fallback native final render plus `finaliseWorkflow`9075–9084 also returns202. The original manager terminal receipt8996–9007 reports finalisation_state FINALISATION_PENDING. It reports manager approval acceptance rather than economic completion. | None. |
| Office retry-finalisation | Original Office permission/generation/key/replay checks11269–11288; original render/lease and `finaliseWorkflow`11289–11295; deferred response chooses202 at11296. Completed receipt replay remains200. | None. |
| Office PHONE_APPROVE | Existing final-render queue/fallback11343–11374 returns202 with `finalisation_pending:true`; fallback invokes `finaliseWorkflow` with the original deterministic key. | None. |

The existing manager lease3335–3359 returns the same pending flags on contention and preserves the original short lease after failure. It complements the database gates; no replacement lease/framework is needed. Bounded recovery6105–6132 passes deferred results through as `ok:true` and increments the existing deferred counter rather than recovered. Recovery is not a new HTTP route.

Native dispatcher returns the caller's Response unchanged (`candidate-app-backend.js:12349–12353`, manager12380–12385, Office12474–12480). `jsonResponse`210–214 uses the supplied status. Private Worker `candidate-private-worker.js:547–563` retains body/status while stripping browser CORS. Public broker `candidate-broker/src/candidate-broker.js:1151–1161` retains all successful body bytes/status; its normal workflow/manager funnel ends at3514. Office index198594–198600 and `withCORS`136423–136428 preserve the original status/body. No layer remaps202 to200 or strips the pending flags in this path.

## Shared paper path is outside this Daily seam

The existing `finaliseReceivedPaperReturn`1214–1237 returns200 when a resolved finaliser result is deferred, whereas its exception path returns202. This is a real shared-paper behavior, **not a proved DY-07 missing Daily branch**. The registered current transition owner `04092026_1952_candidate_expense_history_anchor_recovery_v1.sql:1015–1027` admits genuine Daily CREATE only with PHONE. Current registered atomic finaliser0931:381–386 requires DAILY route PHONE or EMAIL and excludes PAPER. PAPER_RETURN caller8830–8841 is the separate paper action; do not broaden the current card into paper response work or invent a Daily paper route.

The separate current Candidate Daily phase1b response-contract path is dispatched before generic workflow actions12283–12285. Its broker response reconstruction is not the named `deps.finaliseDaily`/`finaliseWorkflow` path and does not justify a protocol rewrite here.

## Inspected current bytes

Root edited the named integration while this investigation was running. The final relevant saved read included the new callee/options, existing safe flags and retained HTTP mappings; earlier raw-callee observations are historical, not the current result above.

| Saved source | Raw SHA-256 at final read |
| --- | --- |
| broker/src/index.js | FCF488BC88EE955BFE560D49CC5D61A1055C5C87EAB4087487E843CAC731BB65 |
| broker/src/candidate-app-backend.js | D805CADE15BBAB1786B97CA4FFB7C346F24C07C93C0A0EFDB02A92524A742C94 |
| broker/src/candidate-private-worker.js | 096BE554ABD0426DED5743AD7AC345DD22D7BC5DA04E833E366BFE056800F671 |
| candidate-broker/src/candidate-broker.js | 4B8904EDF3DC787FFDD04F3ED89AFA19D9FE236EE35506E0DBA20B1154976685 |
| 11102026_1119 adapter successor | 138323F2D728A40C1391681F301F97021D0E277F2A93F980AA4CFFCAFC97F9EE |
| 10102026_0931 retained wrapper/atomic owner | 7FDDDC57B5F1CA6AFF196567A10DD09B393185B17EFDFE9B58BB766AEA7E847B |
| protected-boundary-lock.json | 60D2D3D8A74535CF52C5213803D19B0654AF48955B6096726E2B7A882545F30C |

This closes the requested source investigation of safe Daily pending mapping. It is not item completion, installation evidence or the required future joined Candidate/manager/Office contention/20-second/read-responsiveness acceptance.
