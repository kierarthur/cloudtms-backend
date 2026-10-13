# Owner A5 current investigation — descriptor-only CREATE

11 October 2026. **Read-only investigation complete; implementation review pending.** Scope is the owner's exact A5 completeness correction when Office is next touched for DY08: close obsolete caller forms, remove the old thin selection surface and use plain module-unavailable copy. Preserve actual descriptors and original retry identity/bytes. No product/SQL/checklist change, database/test/fixture/action or screen capture was performed here.

## Exact caller schemas to close

| Current saved source | Obsolete acceptance and narrow correction |
| --- | --- |
| BE `broker/src/bpay-next-preparation.js:17,276` | CREATE's base map accepts `{run_id,pay_date}`; commandCall additionally accepts `{run_id}` or the real descriptor form. Make CREATE's sole exact input `{run_id:'uuid',command_id:'uuid',descriptor:'descriptor'}` and remove both fallback branches. Extra pay_date/actor/other keys still refuse. |
| BE same file:60,313,335–336 | Base six-field CREATE receipt and descriptor-only twelve-field receipt are separately accepted. Make the existing descriptor receipt the sole CREATE receipt and retain its complete native crosschecks. Remove only the caller-pay-date equality branch. |
| FE `js/bpay-next-office.js:33,300–303` | Identical old pay_date/base-only selection branches in INPUT/requestBody. Make the same descriptor-only exact input; no fallback or default descriptor construction. |
| FE same file:54,310–320 | Identical base/descriptor receipt split. Keep all twelve actual receipt fields, request UUID matches, native official date equals descriptor namespace date, empty iff selected_candidate_count is0, empty iff agency_sequence isNULL, and selection_count≤choice_work_count. Retain exact field/type validation and unknown receipt refusal. |

The actual twelve CREATE receipt fields are run_id, status, selection_state, selection_count, selected_candidate_count, official_pay_date, command_id, agency_sequence, choice_work_count, choice_case_count, empty and replay. Preserve integer strings and nullable sequence; do not reinterpret EMPTY as failed or fabricate a command sequence.

`validChoiceDescriptor` in Worker268–271 and Office208–218 is already the real finite descriptor: exact namespace/intent/revision/workbench_revision/adoption_revision, optional memory_baseline_run_id, existing Candidate/Work/component/Case intent and32KiB cap. Retain these definitions unchanged. `broker/src/bpay-next-preparation-rpc.js:7–12` imports the maps only to validate the fixed action name and sends original args to the existing intake; no transport route or new SQL protocol is needed.

## Preserve actual Create and same-key sequencing

The real `createChoiceSubmission` already freezes/clones the descriptor and uses the existing key `cloudtms.bpay-next.submit.v1.` plus its exact namespace (`Office:576–578`). It obtains run_id/command_id once on first creation at635. The accepted descriptor, IDs, validated receipts, phases, pending exact bytes and their SHA remain in that journal. CREATE at696 always sends `{run_id,command_id,descriptor}`. Submission688 enforces the same descriptor and command identity; SEAL691 retains the same command;692–698 persist/retry identical bytes.

Do not change namespace/key/version, silently migrate or clear an unresolved journal, generate a replacement UUID, infer a new descriptor from current ticks, or mutate unknown bytes. Keep real upload/acknowledgement actions WORK_PAGE, MATCHING_PAGE, COMPONENT_PAGE/SEAL, SEGMENT_PAGE/SEAL, CASE_PAGE/SEAL, EXCLUSION_PAGE and SEAL_ENQUEUE. Those are internal bounded sequencer steps, not obsolete manual screen controls. Existing accepted Review/confirm/worker/frozen-detail controllers are also required.

## Old thin surface removal boundary

`Office.render:1455–1459` already returns to the real Workbench for an unsealed selection. The old construction remains in the later render body anyway. **Delete the contiguous old manual selection block at1471–1482**: Choose Candidate/old Case-choice hint; Start selection issuing descriptor-free CREATE; raw Resume run ID/Resume/Open confirmed channel Drafts; technical Run UUID with its old refresh; one-Candidate Current approval facts checkbox table and Next Work page. It is largely unreachable under the current unsealed return, but still contains an obsolete real mutation call and misleading source surface. Do not retain it merely because today's normal render does not reach it.

Keep the following actual sealed behavior beginning1483: Open progress, current frozen channel Draft addresses, accepted worker Review, frozen payment lines, original action/recovery state. The shared controller's run_id is still required; only the raw Run label/manual opener is retired. Old-only `chooseCandidate`1438, `loadCurrent`1431 and transient selection/current-page variables may be removed if their remaining refresh references are also removed. Do not delete shared controller/journal bindings or authoritative page schemas just because the old visual chooser is gone. Current-case helpers require a separate reference check; DR03 already removed the visible retired Case selection surface.

Remove the adjacent obsolete statement at1466 that unsubmitted checkbox choices are lost on Close: real WB21 choice stores now retain/ask about those ticks. Do not use A5 to broaden into future Umbrella execution or unrelated accepted Draft redesign.

Workbench `js/bpay-next-workbench.js:640` currently explains MODULE_NOT_ACTIVE as “Banking Pay NEXT is not active.” Keep the exact server code/type and recovery semantics; change only the displayed message to plain unavailable/paused copy, such as “Banking Pay is not available.” No LEGACY/NEXT implementation terminology or switch action is needed in this user notice.

## Current SQL page path: descriptor does not hit a missing-manifest refusal

The actual registered public intake/page final owner is **0814**, not0454: `11102026_0814_banking_pay_next_preparation_discard_v1.sql:2071,2983`. Descriptor CREATE2079–2089 has the existing authenticated admin/Source admission path and exact run_id/command_id/descriptor keys. Its CREATE2153–2154 calls the current five-argument accepted-choice owner. The original SQL-only thin fallback is still present at2095/2155+; caller closure does not establish that every service-only predecessor overload was removed. The original three-argument preparation helper is also used internally by real accepted creation, so deleting it would break the real path. No SQL rewrite is assigned merely to close the Worker/Office input forms.

Current registered Work-page append still comes from0454. Its descriptor branch **runs first** at126–127 and delegates to `bpay_next_ack_choice_work_page_v1`; it does not enter the legacy append branch's selection checks. The registered ACK owner is2232: v1 at262–266 delegates to v2. V2 requires the already-stored descriptor at178–179, validates exact original members/request replay at181–211, then requires an OPEN/PREPARING unexpired run at213–217. An actual descriptor CREATE therefore has the manifest needed by this path; no special bypass or removed page refusal is justified. EXCLUSION0454:502–503 likewise requires the descriptor that real creation stores. Preserve those missing-descriptor, late/sealed and replay-conflict refusals. This is source-path evidence, not an executed CREATE/page journey.

## Verification constraints and evidence

The existing source gate's final-definer/call allowlists include native preparation/append/page owners; no SQL owner/call waiver is needed for caller-only A5. Existing unit files `tests/bpay-next-preparation*.test.js` and FE `tests/unit/bpay-next-office.test.cjs` still contain assertions/examples of pay_date CREATE: these are retired contracts, not grounds to retain the obsolete form. Root owns updating only necessary existing checks, syntax and normal contract export/seal/coupling; no new fixture/business programme is needed. Future joined normal Create/same-key unknown recovery remains NOT_RUN here.

| Inspected source | Raw SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-preparation.js |5839856b51ccfae4628dbafeb8490bb68197a9d30555465898426653d179c4e9|
|0454 manifest/append owners|d40cc95cb6675f09b1fe06a87801e42a6a28ca1f02a563534ae5e1a65263b5cb|
|0814 final public intake/page|29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c|
|2232 real descriptor page ACK|cc1491f55ff1f4afe48f82e8892015aa2f91500f42d0d41d692f0e6b78dd3e48|
|FE js/bpay-next-office.js initial A5 read|0dcf5fc982195684fc2001985bb258a6c9397b89b7c34f0888612860c0f77c15|
|FE js/bpay-next-workbench.js initial A5 read|f01b5540b281e9ee30b45098811f6ec7c8067ddad93f9247faa4bea760990e3e|
