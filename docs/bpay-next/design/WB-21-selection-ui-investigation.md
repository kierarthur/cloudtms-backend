# WB-21 current selection UI and submit investigation

10 October 2026. Read-only source/design investigation. Read the complete WB-21 card (`02_WORKBENCH.md:823–855`), its ITEM and all 22 deliverable rows, the exact seven-step Batch WB-S interface (`:810–821`), R2-F06/F07 and WB-21 note, binding Rule B design, DR-01 scope, current FE journal/Ready store/Office joins, capability contract and existing native preparation adapters. No product edits, SQL, tests, module imports, application/runtime, data, hosted, fixture, network or secret operations. This note does not assert whole-item completion.

## Binding authority before design

The literal historical R2 requirement to introduce a new economic freeze at CREATE is superseded by **owner Rule B** (`F-25-rule-B-owner-design.md`). Retain the exact submitted expected Work revision IDs, sealed choices, 0151 raw selected-work query snapshot and Candidate fields, original PIN revision-change REVIEW, and live Case balances under existing money locks. No F25 producer/version/certificate/resolver/protocol is revived. The UI can and must freeze one immutable **choice descriptor and exact request bytes at the Create-drafts click**; that does not turn CREATE into a new financial command or change the financial authority. Later user edits belong to another local selection, not to the submitted offer. Any successor implementing finite matching must respect this owner rule and the current ordered editor chain.

## Actual current implementation

| Seam | Current source and exact fact | Required WB-21 join |
|---|---|---|
| Ready local ticks | `FE js/bpay-next-workbench.js:681,688–692,732–734`: ephemeral `baseline='ALL'`, Candidate exception Set, revision counter; choice callback is local only. `countSelection` is exactly `{mode:ALL\|NONE,candidate_ids:uuid[]}` with 32KiB bound (`:405`). | Reuse these ticks and server counts, but back them with the complete namespace/store. No per-tick write, money, request, agency-wide ID array or browser total. |
| Scope and reopen | `workbench:732` resets Candidate intent to ALL on scope change; `:734` clears exceptions on close. No saved unsubmitted intent exists. | Keep separately keyed bounded intent for each exact normalized scope; restore it on reopen in the same namespace. Scope changes must not silently overwrite another scope's exclusions. |
| Journal | `FE js/bpay-next-office.js:421–457`: v4 journal key is only `cloudtms.bpay-next.v1.<actor>`, 64KiB size limit, exact old-version adoption and validation, durable pending `{action,bytes}`. `:694` defaults to sessionStorage. | Extend the journal/store namespace from genuine server context: agency, owner epoch, actor, official pay date and normalized scope. Retain original uncertain journals during adoption; never relabel a saved run or unknown bytes using a new agency/date/context. |
| Exact dispatch | `Office:476–495`: body saved before network; initial definite refusal may clear pending, but unknown retry retains bytes/identity even if the next reply is auth/refusal. Only one pending action; explicit `retry()` sends the saved body. | Preserve this mechanism. Add only genuine current missing action/receipt kinds and a bounded submit step/cursor journal. Do not clear the offer on unknown page/seal result, replace run/command/request IDs, or automatically retry. |
| Thin selection UI | `Office:618,623–644`: one-Candidate current facts, Start selection, Add selected Work page, separate Case page/seal, then Prepare selected Work. The current explanatory text says unsubmitted ticks are lost on Close. | Replace this visible manual upload flow with the single **Create drafts** sequencer and complete local intent. Retain the accepted journal/progress/frozen-review owner underneath. Remove the old visible Add selected Work page control once the real join exists. |
| Create/date | Office CREATE accepts `{run_id}` or `{run_id,pay_date}` (`:225–236`), with actual server official-date receipt (`:40`). DR-01 final1621 derives/stores date and replay identity. | Use the exact known namespace date; do not display/edit a pay date or recompute it from browser time. A stored date/rollover mismatch must produce an explicit restart/refresh decision, not rewrite an existing request. |
| Capabilities | `Office:215–222,301`: exact 11-key config-only NEXT environment receipt. It has no agency, epoch, actor or official date. | Keep it as the environment gate. Namespace comes from coherent validated Workbench context (`workbench:385`), never invented capability keys, URL parsing or a global actor-only setting. |
| Close/escape | `Office:555,698`: dismiss tears down ephemeral views; no choice warning/beforeunload. `js/bpay-next-ui.js:33–60` offers only post-dismiss callback and hardcodes read-frame `dirtyClosePolicy:'close'`. Main Escape calls the token-bound header Close (`main:337902–337907`); header token guard at `:337104–337129`. | A warning must run **before** that actual frame is discarded. An `onDismiss` alert is too late. Reuse the real confirmation owner with a small read-frame supporting close seam; preserve token/current-frame/actor guards. Do not switch to record edit/save or gate child pickers. |
| Readback | BE Worker preparation already exposes WORK_CHOICES and SELECTED_COMPONENTS (`broker/src/bpay-next-preparation.js:28–29,65–66`), with native 2018 SQL page owner (`:3888–3899`). Office PAGES currently omits these (`Office:70–83`). | Add the existing exact page decoders and bounded receipt/readback walk; no duplicate factual RPC or full-agency preload. Retain server expected revision, selection mode/sealed state and exact component IDs. |

## Minimum complete local store

Initialise only after a coherent authenticated Workbench context and environment readiness. The authoritative namespace is `{agency_id, owner_epoch, actor_user_id, official_pay_date, scope:{candidate_id,client_id,channel}}`. Sort/search/visible cursor is presentation and must not silently change what Create submits; matching scope must be normalized and explicit. Actor/owner/context change disables submission and isolates the older saved selection.

Keep bounded intent, not a list of all matching agency members: ALL_MATCHING/NONE baseline, Candidate exclusions/explicit inclusions, exact Work choices `{work_id,expected_revision_id,mode}`, typed component/segment choices, and Case-component selected booleans as their real owning screens land. Candidate exclusion dominates subordinate explicit choices from the same intent. Do not derive Case identity from a Work UUID or use money as an identity/version. The current counter contract's ALL/NONE vocabulary is preserved; any mapping to the future matching descriptor must be explicit at its real native interface, not an early decoder relaxation.

Separate mutable unsubmitted intent from an immutable in-flight submit snapshot. Before the first request, validate and deep-copy the entire bounded ID/boolean descriptor, original namespace, run/command IDs and stage state. Ticks made afterward modify the next selection only. A pending unknown submission disables another dispatch, but never destroys future local intent. Use a finite byte limit with an explicit refusal before overflowing storage or requests; never truncate an exclusion/subset list to make it fit.

Optional localStorage must be genuinely optional: handle unavailable getters/getItem/setItem and keep a process-memory choice store so ticking/reopening the mounted UI still works. Accepted/uncertain request retention needs the stronger actual journal discipline: if bytes cannot be saved before dispatch, do not send and pretend crash recovery is durable. Warn precisely about unavailable persistence; do not discard known receipts. A legacy actor-only journal with a run/unknown request must remain recoverable under its original identity until an authoritative same-run read qualifies any migration; no blind deletion or adoption under today's namespace.

## Genuine current native interface and ordered sequencer

Use existing `/api/banking/pay/next/preparation`, never a from-selection RPC. The service-only SQL intake remains authoritative. Native CREATE/WORK_PAGE/COMPONENT_PAGE/COMPONENT_SEAL/SEGMENT_PAGE/SEGMENT_SEAL/CASE_PAGE/CASE_SEAL/SEAL_ENQUEUE/REVIEW/CONFIRM already have Worker schemas (`broker/src/bpay-next-preparation.js:11–21`). Office's INPUT/RECEIPT lacks component/segment mutation support; add the exact real contracts when called, not permissive pass-through. The command/expiry transports and current F11 queue/recovery wiring already exist; WB-21 does not create a second runtime/background owner.

After one explicit Create-drafts click, advance only on a validated original receipt. Record request bytes and page/step identity **before** each dispatch and its receipt **before** advancing. At most 100 identities per native page. Keep only the current page plus finite cursors/counts/receipts, not all server-enumerated IDs. A timeout/lost reply leaves the exact pending bytes and stops the chain. Retry is explicit and sends those bytes. Auth, actor, owner, frame or official-context changes pause further dispatch; they do not establish the outcome of a previous request. Resume the same run with acknowledged step counts, never regenerate already accepted identities.

The controlling seven steps remain exactly:

1. CREATE: preserve server official date and run receipt.
2. Explicit SUBSET Work choice before its COMPONENT_PAGE/COMPONENT_SEAL, with expected revision and native line IDs; segment pages/seal use their actual WB36 interface when that UI owns a choice.
3. EXCLUSION_PAGE: real later WB-23 exact typed exclusions/receipt.
4. MATCHING_PAGE until complete, or explicit mode-ALL WORK_PAGEs for NONE mode. Never walk display name/amount-sorted workbench pages to emulate complete matching.
5. CASE_PAGE/CASE_SEAL per Candidate using actual complete component membership, selected booleans and selection revision.
6. PAYE_GUARD_DECISION only once the genuine DR-10 GUARD interface exists and says it applies; never invent an unconditional guard call or skip an actually required one.
7. SEAL_ENQUEUE with the original command ID and authoritative counts. On exact acknowledgment, read back WORK_CHOICES/SELECTED_COMPONENTS and use the returned original run for progress. Current native WORKERS/STATUS/CHANNEL_* remain the live Rule B progress path; dormant ACCEPTED_* F25 shapes are not the replacement.

The current 1621 intake and Worker/Office allowlists contain **no EXCLUSION_PAGE, MATCHING_PAGE or PAYE_GUARD_DECISION**. A source census of migrations/repeatables also finds no `bpay_next_run_selection_exclusion` implementation; that checklist object remains required WB-23 work. Do not transmit invented actions, emulate all matching with visible pages, or claim the seven-step joined flow complete before those real later owners land. A finite sequencer may preserve its exact future stage design, but the user-facing submit path must be truthful about absent required capabilities rather than silently omit membership.

## All-skipped page boundary

DR-01 retains attempted pages and their exact skipped tuples; `item_count` is attempted membership and `selection_count` is accepted membership. Do not treat one attempted row as one selected Work or increment selection counts from client arrays. The current Rule B seal rejects `(pages=0) != (selected_count=0)` (`0151:14–18`) and zero Work without real sealed Case-only membership (`:39–41`). Thus all-skipped positive-page offers cannot be passed to the unchanged seal as a fake empty Draft.

The WB-21 UI must show every genuine skipped reason and explicitly say nothing eligible was selected/no Draft was created, retaining the receipts. If any Case membership is selected, use its actual Case-only semantics and authoritative counts. Positive attempted Work pages with zero accepted Work do not bypass the first seal validation merely because Cases are selected; that mixed path also needs the genuine receipt/count boundary resolved. A necessary named seal/empty-offer completion adaptation is a concrete source obligation for the parent to resolve; this investigation does not invent a zero-money job, cancel endpoint, unreceived receipt or cleanup protocol.

## Close and future-owned joins

The unsubmitted dirty warning is local only: Discard clears only that local unsubmitted namespace; Keep preserves it for reopening the same official date. Neither sends a server discard or clears accepted/unknown journals. Beforeunload uses the browser's normal warning when dirty; accepted unknown state stays retained. Ensure header Close, Escape and programmatic root close share the same pre-dismiss seam. Child closure does not discard parent intent. Recheck actual frame and actor after a confirmation child resolves.

| Owner | Required integration without early competing implementation |
|---|---|
| WB-21 now | Complete namespace/store, exact snapshot and receipt journal, submit-step owner, native current transport/readback, discard/keep and unknown-result presentation; preserve current Rule B money/pin authority. |
| WB-23 | Actual finite server matching, exclusion pages/receipts and all-page membership. WB-21 consumes these exact real actions after they exist. |
| WB-25 / WB-37 | Real Work/component/day/segment controls and typed callbacks into this same store; no fake current component picker. |
| WB-26 | Actual complete repayment/Case-component choices, including unloaded pages. Current Candidate debt-only rows are not a substitute. |
| WB-22 / WB-24 / WB-28 | Actor same-date acknowledged exclusion/default baseline, Candidate dominance/complete NONE-SOME-ALL displays and exact baseline reset; no agency-global remembered untick flag. |
| DR-02 / DR-06 / DR-10 | Native late-page/replay enforcement, real progress opener/window and actual PAYE guard respectively. Current thin progress remains real; a missing named future window is not a stubbed success. |

Lane C old timing/300-Work evidence is not recreated or upgraded here. Reviewer runtime, actual 50-tick no-network, 297-of-300 membership, two-user isolation, rollover, lost-reply and full Worker/queue/window journeys are **NOT_RUN**.

## Source identities at investigation

FE Office `73239f720c497fd17daea4e09cfbe2fead8a0a0443d0067796c2ad7e8a1344f1`; FE workbench `906c966bc4a671b1cc72a822071367172a1c1aff33764b6f9981bb25db845f3b`; FE UI adapter `dbdcb12ed8ec027b0df39c52ef6f50b6d91ca93e2cb74451a59e09a6ba029272`; BE preparation `91b3ca6654a761a2f16d7a9b2af5360b168858681d555467d6b7cd94ddc6ea89`; BE preparation RPC `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34`.
