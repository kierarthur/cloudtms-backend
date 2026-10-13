# WB-01 authorise Worker source review

Source-only review, 10 October 2026. Final verdict: **PASS; H01 resolved**. No product edits, tests, gates, SQL, runtime calls, fixtures or remote actions were performed by this reviewer.

Reviewed the complete current `broker/src/index.js` diff and the original surrounding single Timesheet, Contract Week, correction-pair, Gold bulk item, durable bulk summary and authorise-summary callers against WB-01 Rules/Build/Done (`02_WORKBENCH.md:84–119`) and all WB-01 checklist rows, especially D10/D11. The reviewed Worker SHA-256 is `7eebdaade49743143a89a19295cccf745c78c90d1799ab02f5de18e800de854c`.

## H01 — resolved: original missing-rate refusal entering the old transient retry wrapper

`callTimesheetLifecycleRpcWithTransientRetry` calls the real authorise RPC before the new handler catch. Its finite `businessFailureCodes` set at lines 95198–95217 omits `BPAY_NEXT_ORDINARY_USED_RATE_MISSING`. `describeError` collects the original JSON DETAIL text and tests the joined text for `NETWORK`, `CONNECTION`, `SOCKET`, timeout and lock language (lines 95257–95296). The new genuine ordinary publisher supports arbitrary Additional identities and emits the missing identity through `upper(btrim(v_extra.key))` in the original `23514` refusal DETAIL (`10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql:627–629`). Thus a genuine used Additional code `NETWORK` can classify a definite missing-rate refusal as a transport failure, retry it, and eventually wrap it as controlled transient before the new typed 409 catch sees it.

The smallest correction is adding this exact named refusal to the existing finite `businessFailureCodes` set, preserving the wrapper, retry budget, unknown-outcome reconciliation and all other errors. Alternatively an exact genuine original-message classification before the existing retry decision has the same effect. This is required to keep a definite missing-rate guard from an automatic retry; it does not justify changing the general retry framework.

Fresh closure read: the exact `BPAY_NEXT_ORDINARY_USED_RATE_MISSING` entry is now present in that existing business-error set. The unchanged classifier chooses the exact named original message as `businessCode`, makes `retryable` false even when the genuine Additional identity contains transport words, and the unchanged nonretrying branch rethrows the original error for the new typed 409 handler. The retry budget, genuine unknown-outcome reconciliation and other refusal classifications remain intact. The display label lookup also now uses `Object.prototype.hasOwnProperty.call`, retaining arbitrary bucket text safely. Complete fresh Worker diff re-read, final SHA-256 `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14`. No remaining actionable source finding in these bounded changes; runtime/app acceptance was not performed.

## Changed transport paths

- The new helper recognizes only the exact original named code/message, parses bounded string error packets, requires a nonempty bucket of at most 245 characters and a native Contract UUID, and preserves both as typed fields. It handles PostgREST `code=23514` plus the actual named `message`, without treating generic SQLSTATE as a missing rate. The six display labels include `MILE`; arbitrary genuine Additional identities remain their original canonical code.
- Single Timesheet and Contract Week mutation-error builders return the definite 409 before their old general fallback. The Timesheet RPC catch tests it before unknown-outcome reconciliation. The original reconciliation machinery remains intact for genuine unknown errors.
- Single correction-pair and selected correction-pair bulk catches preserve the original refusal. The bulk catch is limited to AUTHORISE and rethrows unrelated failures. Original successful wake and mutation processing are unchanged.
- Gold bulk item and persisted-operation summary now preserve the bucket/Contract/code, including an `error_json` fallback in the durable summary. Their native success, failed, stale, affected-row and operation-count rules remain intact.
- `runTimesheetAuthoriseDecision` carries the typed fields into the actual summary result. No new endpoint, Banking decision kind, rate calculation, owner lookup, publication, payment or automatic retry was introduced by the changed transport hunks.

The earlier provisional inherited-label concern is **withdrawn**: the genuine owner uppercases Additional codes, so `constructor`, `toString` and `__proto__` become uppercase and do not resolve JavaScript prototype members. A defensive own-property label lookup is harmless, but it is not the demonstrated H01 defect.

## Exact existing Office binder seams

These are implementation locations, not a reviewed or executed Office change:

- `openBulkAuthoriseWorkbench` owns `runBulkAuthoriseWorkbenchPostRenderBindings`; its stable `leftPaneBinders` array at `main.js:180033` includes `bindBulkAuthoriseShellControls` and is the smallest existing post-DOM seam for typed Contract-rates buttons. The shared binder is assigned to `state.__runPostRenderBindings` at line 180252. It is reached both by `scheduleRenderTabPostRenderBindings` (180668/180709) after ordinary render and by `rerenderBulkAuthoriseWorkbench` (189788/189798) after the managed `setTab` render. Adding a finite stable binder here covers both actual render paths without wiring inside string-only `renderBulkAuthoriseShell`.
- The controlling last `openBulkTimesheetActionProgressModal` definition starts at line 90241 (earlier duplicate definitions are superseded). Its `normaliseFailure` retains the original typed fields, `renderFailures` starts at 90460, and replaces only `failuresEl.innerHTML` at 90473, with a 200-row visible cap. Its existing delegated `onClick` at 90614 and root listener at 90629 are the smallest actual failure-list button seam and survive repaint without duplicate per-row listeners. Keep the originating actor and this dialog/root lifetime checked around the asynchronous Contract fetch/open action.
- Both buttons must use the existing authenticated `getContract(contract_id)` and `openContract(actual returned wrapper)` followed by the real Contract frame's `setTab('rates')`; a bare `{id}` wrapper or invented route/initial-tab option does not establish the actual editor.

Line numbers are the source checkpoint above; concurrent Office edits can shift them. No app acceptance or SQL installation claim follows from this review.
