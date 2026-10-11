# FC-09 protected Draft link investigation

11 October 2026. Read-only current FE source trace; no product/runtime/A4 approval. The edit-state protection now supplies exact run_id, nullable run_worker_id/channel, run_status, nullable worker_status and native next_action. Human NEXT Draft reference is not supplied; show PAYE/Umbrella Draft without a fabricated reference or UUID.

## Existing native entries

| Protected state | Existing entry and exact limitation |
|---|---|
| Parent PREPARING, including no Worker yet | Office.openBpayNextPreparationProgress(options, protection.run_id) is the original progress window (Office line1583). It uses original PROGRESS, WAKE/discard and existing Continue to review. It requires a genuine progressNamespace containing actor_user_id, agency_id and owner_epoch; current FC09 creator options do not carry agency_id. Do not fabricate that namespace from run/Candidate or assume ownerClient.peek provides it. Reuse an already-qualified native Workbench namespace if the caller has one. |
| Parent PREPARING/REVIEW and existing Worker PREPARING/REVIEW/READY | Office.openWorkerReview(options, protection.run_id) is exported (line1454), uses the original run-scoped controller and WORKERS/STATUS/PROGRESS. It exposes existing Remove from Draft/DR05 WORKER_RELEASE and whole-Draft discard, with native current-state checks. It is a whole-run review, not a direct Worker-select opener; selecting the protected Worker must remain based on its real identity. |
| Parent confirmed DRAFT | The actual confirmed channel host is Office.open(options) with its existing CHANNEL_DRAFTS/loadDrafts -> controller.setDrafts -> bindDraft({run_id,pay_channel}) -> CHANNEL_WORKERS/CHANNEL_STATUS path (lines1838–1850,1877–1890). openDraft(address) is private. No exported direct confirmed-address opener currently exists. |
| Confirmed Draft factual inspection only, with exact Worker | Office.openFrozenWorkerDetail({...options,detailRunId:run_id,detailCandidateId:actualCaseCandidate},run_worker_id) is already exported (line1366). It reads original STATUS/LINES and frozen detail. This is Draft detail, not a Cancel payment interface. |

Do not send a confirmed DRAFT to openWorkerReview: refresh calls controller.mutate(REVIEW,{run_id}) and the closed REVIEW receipt phase is only PREPARING/REVIEW (Office line404). Similarly, Office.open({reviewRunId}) with a fresh journal currently calls REVIEW before refresh, so reviewRunId alone is not an independently working confirmed-Draft deep link.

The generic legacy main.js openBankingPayBatchChildModal(pay_batch_id) takes a legacy Batch identity. NEXT run_id is not that identity. Do not route a NEXT UUID there or fabricate a legacy Batch.

## Smallest complete caller change

Use the supplied native next_action/run_status to select the original preparation/review path; re-read current native facts in that destination rather than deciding release authority in the editor. A PREPARING/no-Worker case must not be misrepresented as an empty Draft merely because WORKERS has not enrolled yet. Prefer the original progress view when its real namespace is available.

If the required link is the complete confirmed Draft host (rather than clearly labelled read-only Draft detail), add only a narrow existing-Office address adapter: validate the exact protected run/channel/Worker/Candidate under current actor/NEXT/frame; hydrate the existing CHANNEL_DRAFTS receipt, prove that channel address is present, then use existing setDrafts/bindDraft/loadWorkers/loadStatus. Skip the pre-confirm REVIEW branch for this explicit confirmed-address entry. No new route, reader, protocol, command or financial owner is necessary. Keep the existing journal's original pending bytes; do not overwrite/rebind an uncertain request to force navigation. Existing singleton sheet handling must prove/focus the requested run or safely refuse, rather than silently focus an unrelated open run.

Before any native window navigation, use the editor's actual frame/current guard and the same immutable API/authFetch, actor and owner-epoch qualifier. Pass the original standard adapter and exact factual Candidate ID. Close/focus behaviour must preserve the existing modal stack and readback guard, not introduce another framework.

The confirmed host's Cancel this unpaid Candidate Draft already invokes the original CANCEL_REQUEST through existing reauthentication and native guards (line1890). Its presence does not prove that a Case-bearing confirmed Draft is cancellable: original native eligibility and later owning cancellation scope remain controlling. This link must not add inline release/cancel, guarantee edit will unlock, or imply that Draft cancellation has happened. Read-only Draft detail is an available honest fallback if complete cancellation is not currently admitted.

## Source locators and provenance

- Office lines1366–1451: openFrozenWorkerDetail, exact STATUS header identity check and frozen readers.
- Office lines1454–1563: openWorkerReview, existing ordered worker/release/discard actions.
- Office lines1566–1600: preparationScope and progress namespace requirements.
- Office lines1838–1850: original channel workers/status/Draft address readers and private opener.
- Office lines1877–1890: confirmed channel address, original Candidate Draft cancellation.
- Case Preview lines42–48: closed native protection tuple including run/worker status and next_action.
- Owner client lines14–18: only active_owner/owner_epoch; no agency namespace.
- main.js Payment Advance opener line12988: FC09 owner qualification and existing native options; same pattern applies to Credit/Debt.

SHA-256 of source read at this checkpoint: Office 2403fe59b1a99ad058a9143aaa2591f3c3807c1f65536b97db6f255fd32546fd; Case Preview df77ccac51c17e90a9b85b05f980a43d56d68d556478b47a51e8eebc5a1654f5; main.js e59fd9163b59f95236e398041513b65fd484ec00a46651e412c98a4e927b48fa. Root is authoring concurrently; these identify this investigation, not later product approval. No product edit, test, render, database/business/hosted call or commit was performed.
