# DR-02 submission source review

11 October 2026. Independent read-only source review of the current in-progress DR-02 frontend and matching closed receipt/header seams. Product files were not edited. SQL/database execution, tests, fixtures, installation, browser interaction, A4 screenshots, unknown-outcome journeys and queued runtime execution were **NOT_RUN** by this reviewer. Root owns any separate evidence.

Latest outcome: **SOURCE PASS** at the closure correction rereview below. All identified essential source findings are closed; earlier findings are preserved as review history.

## Initial outcome: changes required

1. **Recovery after closing a failed submission is blocked in the same Workbench.** In `FE/js/bpay-next-office.js:970–1014`, createDraft sets submissionBusy, opens a submission frame without onDismiss, and clears submissionBusy only when no frame exists or finished is true. After a known failure or unknown request, closing that child leaves the parent Create Draft permanently disabled (`957–962`), although the original journal is retained. Add only a dismiss/rejoin path: keep the original journal, allow a settled failed/unknown submission to be reopened, and keep a single driver while a request/sequence is actually running. Closing during a running upload must not permit a competing fresh driver or new identity.
2. **The required pending progress opener loses the original run address.** `openBpayNextPreparationProgress` accepts and validates runId but currently renders only “Draft preparation has started.” (`office.js:910–918`). DR-02 card line 90 explicitly requires the DR-06-pending placeholder “Preparation started — run <id>”. Preserve/display that original run identity in the placeholder; there is no authorization here to add the DR-06 progress reader.

These were reported to root immediately. Current source is in progress; these findings describe the observed body before root corrections.

## Source-consistent parts

- The new network SEAL_ENQUEUE receipt is closed and requires native selection_count equal to the original expected_count. Worker and frontend match; the public intake obtains the current final seal's existing count instead of deriving money or inventing membership. (`FE/js/bpay-next-office.js:54,295`; `BE/broker/src/bpay-next-preparation.js:61,301`; `BE/supabase/repeatable/11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:655–659`.)
- Historical receipt restoration is restricted to the explicitly closed former SEAL_ENQUEUE shape in a persisted READBACK/COMPLETE journal. It validates through the current receipt checker with the original attempted count, while original request bytes/run/command/page/count and original CREATE sequence remain checked. New network payloads do not use this exception. (`office.js:533–552`.) Existing unknown pending bytes are preserved through validated schema restoration (`568–582`).
- sealEnqueue accepts only the existing GUARD/SEAL seam and persists SEAL before dispatching the original run/command/page/count. There is no new Guard command, success result or schema; the real conditional Guard read/decision remains explicitly DR-10 scope. Unknown seal recovery first resends the stored original bytes, then resumes READBACK. (`office.js:717–723,989–998`.)
- SCR160 freezes the exact local descriptor/revision, uses the existing countSelection reader, checks unchanged actor/owner/context and local revision again before accepting, and emits no CREATE on cancellation. Its failure retry rereads count; zero Candidate count disables confirmation. It shows Candidate count, channel scope and the existing All except label, with no money estimate. (`office.js:879–909`; `FE/js/bpay-next-workbench.js:1121`.)
- The normal Create action requires environment readiness, original Office frame/actor/NEXT owner, Ready/Header availability, server can_create_draft and coherent current Workbench revisions. The header boolean is closed in Worker/FE and added in the public HEADER enrichment after existing active-admin/NEXT checks. Mutation authority remains server owned. (`office.js:957–983`; `FE/js/bpay-next-workbench.js:15,813,831,962`; current SQL `0454:1455–1458`.)
- resumeOnly restores the existing journal under its original business namespace before asking for new confirmation. A restored journal owns its original descriptor/run/command; new UUIDs are generated only when no retained journal exists. The ordered driver retries pending bytes first, then advances the existing Work/Case upload, seal and bounded readback before acknowledgement. (`office.js:568–582,974–999`.)
- The two retired manual “Add selected Work page” / “Prepare selected Work” buttons were removed from the normal source branch. Current main.js continues to open the same Office module through the same standard adapter and actor/owner route; no new Office route was added. (`office.js:1088–1093`; unchanged `FE/js/main.js:7151–7164`.)

The existing journal's acknowledgeComplete deliberately retains the original record if storage removal fails and only acknowledges a matching local revision; this review does not claim a runtime storage or reload journey passed. The active/hidden native frame behavior and original request continuity must be assessed with the authorized app acceptance, not inferred from this source review.

## Correction rereview — one remaining state defect

The original dismiss finding is closed in saved source: the child onDismiss releases the parent only after its driver is no longer running, and the driver's finally releases it when the child was dismissed. Retry explicitly takes the busy gate again. Original journal state and IDs are unchanged (`office.js:990–1025`).

The owner A3g direction relayed in this task prohibits preparation terminology/UUIDs on the customer screen and authorizes the friendly equivalent to the card's technical pending placeholder; root will record that binding as BD088. The original run is now retained in data-next-run-id and the returned frame's run_id, and progress opens from the accepted seal before readback (`office.js:912–922,1001–1006`). This closes the original identity finding under that explicit owner direction. Native Close remains supplied by the standard adapter; no DR-06 reader was added.

**Remaining required correction:** after a readback failure, progressWindow.submissionFailure changes the copy and exposes Try again (`918–922,1017`). If the original retry succeeds, the COMPLETE/ack branch never restores normal started copy or hides that failed footer (`1008–1014`), so the user still sees a failure with a disabled Try again after successful readback. Reset that same frame's local copy/footer when the original readback/ack succeeds; retain its original run and retry closure. No new command, reader, protocol or financial data is needed. This was reported immediately. All execution/acceptance limitations above remain unchanged.

## Final rereview — SOURCE PASS

The final correction is sound: submissionRecovered restores “Draft creation has started.” and hides the failed footer on the same progress frame; the COMPLETE/EMPTY acknowledgement branch invokes it after the original readback before final local refresh (`FE/js/bpay-next-office.js:918,1008–1014`). It keeps the original run_id, native frame and original retry closure. The existing busy/rejoin gates and strict new/historical seal validation are unchanged. No further essential source defect was found within this requested review.

Reviewed saved source SHA-256: `js/bpay-next-office.js` = `7149509FF02C1FFCD7BF6EAC5ECC1A10EE6EBFF722670D3B1C041276026566C4`; `js/bpay-next-workbench.js` = `3B1F1B778D1483A865A1BA28EE5D874C117F15CBDD503CE4C700C49341C16EA5`. These identify this source review only. Root's A4 rendering was still running; this reviewer did not observe its results. Native parent interaction, actual queue/install, unknown/reload/close app journeys and all app acceptance remain **NOT_RUN** here.

## Closure rereview — native footer and final notices

Observed saved hashes: Office `9F1D1BDE637D913E08D7FF2D580F8F2277478592A6C3B7C7C507EA9EF8E7C871`; Workbench `1B10BD4C36FF894A079200639285512FDD8B2A60A7562B6460C2F311C6AE8106`.

The final small sending/progress frames, decorative spinner, explicit status/alert roles and hidden-safe notices preserve the reviewed original journal and driver. Native footer placement routes through the existing standard adapter; it does not add an Office route, financial read or command. Strict new seal count, closed historical restore, SCR160 and original progress retry/recovery remain unchanged (`office.js:879–923,975–1025,1030,1167`; `workbench.js:813–814,831`). Screens, native interaction and app acceptance were NOT_RUN by this reviewer.

**Required footer cleanup:** ownerChanged in Workbench currently removes root but omits refreshFooter (`workbench.js:960`). The action footer now lives outside root in Office footerHost (`814`), so invalidating actor/NEXT ownership leaves stale Create/Export controls visible. Callbacks still refuse invalid owner; this is a teardown/display defect, not a financial-authority bypass. Apply the same refreshFooter removal already used by close (`962`) to owner-change teardown, or disable/hide that external footer there. Reported immediately to root.

DR-02-completion.md D07 correctly says the actual GUARD kind/action remains DR-10 and requires its future insertion before GUARD→SEAL. Its preceding sentence currently says the journal “sends ... conditional PAYE_GUARD_DECISION”; rephrase that clause as the required future DR-10 insertion so the completion note cannot imply the absent action/reader is currently built. No new Guard code is requested here. All app acceptance remains NOT_RUN.

## Closure correction rereview — SOURCE PASS

The external-footer defect is closed: ownerChanged now removes refreshFooter before root, matching ordinary close (`FE/js/bpay-next-workbench.js:960,962`). This removes stale actions on actor/NEXT invalidation without changing mutation authority. D07 completion line15 now expressly calls PAYE_GUARD_DECISION the “required future DR10 insertion”; line17 retains the real Guard-before-SEAL requirement and does not claim that Guard is built. That accurately describes the current staged seam.

Final reviewed saved hashes: Office `9F1D1BDE637D913E08D7FF2D580F8F2277478592A6C3B7C7C507EA9EF8E7C871`; Workbench `F01B5540B281E9EE30B45098811F6EC7C8067DDAD93F9247FAA4BEA760990E3E`. No remaining essential source finding in this requested review. A4 belongs to the separate screen reviewer; native parent interaction, actual queue/install and every app acceptance remain **NOT_RUN** by this reviewer. Product files were not changed here.

