# SP-02 caller design

Read-only source investigation, 10 October 2026. Covers the full SP-02 card in `05_SOURCE_AND_PROTECTED_PAY.md:128–153` and checklist D04–D06, with the supporting HTTP error boundary. No product, SQL, tests, runtime, fixtures or hosted actions were performed. Root owns implementation. This is a finite caller map, not whole SP-02 acceptance.

## Database admission and existing transport

BD041's proposed zero-argument service RPC `bpay_next_source_legacy_finance_admit_v1()` is a suitable narrow transport admission: literal `true` for LEGACY, `PT410` / exact message `BPAY_NEXT_LEGACY_WRITER_RETIRED` for NEXT, `PT503` / exact message `BPAY_NEXT_LEGACY_WRITER_DISABLED` for DISABLED. The Worker forwards this result; it must not fetch owner status and choose a business branch. Unknown, malformed and unavailable admission is a fixed 503 and the requested financial action is not attempted. Call this zero-argument RPC with `{}`, not the orchestrator's ordinary `{p_request: ...}` helper.

An admission read is not a lease across the subsequent HTTP transaction. It does not replace the installed 0550/0555/0556 DB boundaries or the true financial owner's entry guard. Existing factual Source finalisation, authorised facts, TSFIN preparation, invoices and approved NEXT publication remain available. The already staged C1 request named by the card stays untouched; callers do not retire, cancel, migrate, delete or manufacture a receipt for it.

`index.js:198586–198669` already supplies the true dependencies: `dataRpc: sbRpcRecordingGuardRefusal` to finalisation/recovery, and `createWeeklySourceC1RawRpc({baseUrl, headers, fetchImpl})` to the C1 publisher. No extra index-side owner lookup is needed. The guard-refusal recorder catches and rethrows the original error. `sbRpc` retains the received DB object in `error.json` and raw text in `error.body`; it sets `error.code` only for recognised busy errors. Thus a normal received retired DB error does not automatically become a caller `error.code`.

## D04: finalisation and Recover finalised pay

`orchestrateWeeklySourceFinalisation` first awaits and validates `weekly_source_finalise_atomic_v1` outside its pay-tail catches. This is the real factual commit; an ambiguous factual response keeps the original idempotent-finalisation recovery rules. Do not put the new retired-finance admission ahead of this factual call.

`recoverWeeklySourceFinalisationPayProjection` validates the Office actor and original recovery request before opening the durable pay run. The newly saved admission block at this exact explicit Recover entry is correctly before `_pay_open_atomic_v1` and `_task_recover_atomic_v1`. It accepts only literal `true`, converts the finite DB diagnostic to stable caller error/status, and refuses unknown admission before the requested operation. It does not invent a run or C1 result. Keep all original run/task/version and original retry qualifications after successful LEGACY admission.

The remaining exact catch census matters if a true downstream DB owner returns the typed refusal:

| Seam | Existing handling | Required finite distinction |
| --- | --- | --- |
| Post-factual `_pay_open_atomic_v1` catch | `FINALISED_PAY_CHECKPOINT_REQUIRED`; `boundedErrorCode` | A definite retired refusal must retain the actual factual result and stable refusal, not imply a missing legacy checkpoint. |
| Snapshot preparation catch | `FINALISED_PAY_PREPARATION_REQUIRED` | Preserve Source preparation/TSFIN availability; distinguish only a received exact retired-finance refusal. |
| `_task_start_atomic_v1` catch | `FINALISED_PAY_RECOVERY_REQUIRED` | Do not offer a retired action as re-armable recovery. |
| Ordinary projection apply / task finish catches | Call `markUnknown` | A definite pre-write retirement is not an unknown financial outcome. Do not issue an invented UNKNOWN checkpoint for this diagnostic. Actual response loss keeps the existing durable unknown handling. |
| Explicit Recover open/recover calls | Errors propagate | Preserve the stable typed refusal through the HTTP boundary; no automatic retry. |

`boundedErrorCode` currently chooses `error.code || error.name`: an ordinary `sbRpc` retired error becomes `ERROR`, while its authoritative SQLSTATE/message still live in `error.json`. If a finite typed-error helper is used, it should recognise exact structured code/message pairs, including an intentionally retained historical `55000` + exact retired message, and leave unrelated `55000`, `23514`, transport loss and all other errors unchanged. The original result helper already retains `source_finalised: true`, `invoice_authority_committed: true`, the actual source finalisation, and only an actual nullable `ordinary_pay_projection`. Reuse these fields; do not claim legacy pay completion or synthesize a receipt/run/task.

The HTTP support seam is `weekly-source/routes.js::routeErrorResponse`. Its first branch already preserves `bpayBusyEnvelope`. Its generic branch only reads `error.code`; an unnormalised `sbRpc` DB refusal otherwise becomes `WEEKLY_SOURCE_REQUEST_FAILED` with the generic RPC message. A small exact typed diagnostic branch, or a typed error normalised at the named owner caller, must retain 410/503 and a fixed public `error_code`. Preserve the existing exact `55P03` + finite busy-message recognition and retry semantics; do not broaden recognition to all class-55 errors. An explicit normalised `WeeklySourceFinalisationPayError` already transports through the generic route unchanged.

## D05: C1 raw RPC

The current raw RPC has a finite admission preflight and a received-error-only recogniser. It does not parse successful C1 business JSON. A LEGACY admission can use the same raw transport with literal `{}` / scalar `true`; the requested original C1 function still receives the original `parametersJsonText` unchanged. There is one attempt per call and no automatic retry. Keep the request/response limits and `LOSSLESS_JSON_TEXT` contract; do not parse or stringify business bigint values.

`WeeklySourceC1RawRpcKnownOutcomeError` retains status, raw body, RPC name, `outcomeKnown: true` and `unknownOutcome: false`. Its bounded error-envelope parser recognises only the exact retired/disabled diagnostic. Preflight timeout, missing response or malformed success is converted to fixed `BPAY_NEXT_OWNER_STATUS_UNAVAILABLE` before the requested external C1 action, which therefore has a known non-attempt. A timeout after the actual action remains the original ambiguous C1 error. Update explanatory comments to distinguish the new admission HTTP request from the single original C1 action attempt.

The existing downstream chain already supports this distinction: the C1 adapter rethrows `outcomeKnown === true` / `unknownOutcome === false`; `invokeWithUnknownCheckpoint` only persists a genuine `recoveryCall`; durable publication maps only `C1_OUTCOME_UNKNOWN` to recovery-required. Thus a normalised definite refusal reaches the route without fake C1 publication, UNKNOWN or retirement receipts. Status/scope/read/recovery methods share this adapter, so do not assume their call is an independent retirement completion. The card's real C1 local receipt/run/digest/generation/family qualification stays with its existing DB owner and the F-18/F-19 reconciliation scope.

## D06: Office wording through existing F-15 status

`import-workspace.js::requestJson` already propagates `payload.error_code` to `error.code` and carries status/details. `friendlyWorkspaceError` recognises only Weekly Source codes today, so the named BPAY refusal becomes generic unavailable copy. Add the exact plain text only when the shared F-15 owner client's current `peek()?.active_owner === 'NEXT'`:

> Pay is published through Banking Pay NEXT; there is nothing to recover here

A retired error alone, a Source absence setting, a provider mode, or a fabricated workspace flag is not positive F-15 status. Missing, DISABLED, old LEGACY or unavailable status keeps truthful fixed non-NEXT error copy. This display condition does not gate factual Source permissions or choose a financial writer.

`main.js::bankingNextOwnerClient` already supplies the actor-bound, strict two-field F-15 client and dispatches `cloudtms:bpay-next-owner-changed`. `openImportsModal` can pass this existing client and actor context through an optional open-options argument while preserving existing `open(initialTab)` callers. A bounded diagnostic qualification when the Imports frame opens is enough; use the existing client, not a new owner endpoint or transport. The Imports module must not close the globally shared client on frame disposal.

Guard owner-notice updates by current actor, original frame/session generation and connected current Imports frame. A scoped listener may repaint on the existing owner-change event and be removed on disposal. No timer, command UUID, financial retry or new journal is needed.

The finalise click currently discards the successful `issueCommand` result before `loadWorkspace`; that load clears `session.error`. Therefore a successful factual-finalisation response carrying a typed financial refusal needs a separate informational notice or a result-aware post-load display, not an error assigned immediately before the clearing load. Recover errors already reach `friendlyWorkspaceError`. Preserve the original stored real recovery identities for actual ambiguous/admitted work; never turn the typed retirement into an armed retry.

Current Source workspace notices are plain escaped `{tone,title,body}` entries. The module does not itself read the legacy absence setting/arrival detector. The DB absence design must keep LEGACY factual semantics and avoid using the retired engine's arrival classifier as proof that NEXT is absent. Office should reuse only the existing positive F-15 status for the exact NEXT sentence, without adding a public Source owner flag or changing invoice/Source data.

## Source receipt

Saved source read during concurrent root implementation; these hashes identify the exact observed snapshots, not final approval of later edits:

| File | SHA-256 |
| --- | --- |
| BE finalisation-pay-orchestrator.mjs, including new explicit Recover admission | `874a9cddebd25d69dfb2e312870c4463e0dd85b9839f5b82ae4ef5abd1db05a4` |
| BE weekly-source-c1-raw-rpc.mjs, including new admission/finite error recognition | `f290730871fdd1654b7d871a94712ddb4ad2376d58467e1a9773c1dd04719f74` |
| BE weekly-source/routes.js | `606615afec53a10c6507f86cb0c39605c056d06e2a322a2addcf28645bacfa81` |
| FE weekly-source/import-workspace.js | `ad0952f73c3cb06ad1cb079f9858329948694ff72968b89243eee0128b491a27` |
| FE main.js | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |
| FE bpay-next-owner.js | `4947d2e61cd4fa734cbaa99b05c415bad8552865d286233123cc48f34e1ca922` |

Joined app/DB/C1 acceptance remains NOT_RUN. The proposed support is limited to the named caller refusal propagation and display; root must verify the final saved bytes independently after implementation.
