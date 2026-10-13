# DY-10 independent screen/design scope review — sequence 97

11 October 2026. **BD109 design direction PASS, with the finite state/retry requirements below retained as implementation obligations.** Showing a first unpublished Daily failure in the existing native admin Timesheet window is the smallest complete supported placement. The existing NEXT Workbench cannot list a Daily that has not yet produced Work; a fictional Work/Candidate Banking row would violate the owner's data boundary. No A4, built, installed, joined or whole-item PASS is claimed.

Read the full DY10 card (`06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1119–1141`), every D01–D05 checklist row including historical D04, current master sequence97/dependencies/1530 editor chain, BD109 `DY-10-design.md`, current investigation and relevant saved native reader/caller/action sources. Viewed supplied B10 SCR156 desktop and390px phone plus SCR156b desktop PNG and both JSON specs, read-only. No product/checklist/pack/spec edit, database, fixture, test, hosted call or render was performed.

## Required behavior remains full card scope

The card requires distinct genuine approved zero, withdrawal, genuine booking cancellation, missing/unprepared, unapproved, processing, damaged evidence and financial calculation failure. It also requires bounded isolated retry/checkpoints, original current physical and immutable approval identity, accepted-click ordering, no fallback zero, no repricing of retained Drafts and one actual paid-excess recovery through the existing owner. BD109 retains those requirements. Existing publisher1001/evidence1129/withdrawal0939/POSITION_APPLY1346/retention1107/reconciliation0814 remain the money/provenance owners; a presentation helper cannot create an approved zero, authorise, settle, revoke retained economics or infer a recovery.

Owner Rule B/E2 means accepted historical inputs are never substituted or repriced. A later safety refusal can hold back/system-release the non-executable attempt through its existing job; that is distinct from fetching newer facts and trying this same Draft again. Preserve the original figures and the next-Draft destination truth. Source inspection does not prove the card's future race/payment journeys.

## Native reader support is necessary and bounded

Current `GET /api/timesheets/:id/details` (`broker/src/index.js:73432`, dispatch201243) exists before NEXT Work. Its resolver/current Timesheet and current TF reads73535–73563/73637–73648 supply native scope/version/currentness, actual authorisation/revocation/archive facts, processing/rate/channel flags and hours. The response spreads those originals at74630–74652 and assembles them at74814. They cannot distinguish a first failed calculation from never prepared when TF is absent.

The one BD109 supporting fact inside this same reader is therefore necessary: exact current physical DAILY Timesheet → existing indexed outbox membership, seven native unique reasons plus a sentinel (limit8), validated attempts/retry time and a closed failure/pending/unavailable result. Never expose raw last_error or derive zero from absence/error. A failed bounded read/malformed row/impossible count is unavailable, not “no failure”. Keep older approved TF facts separate from a failed subsequent calculation attempt; a new failure does not revoke an existing approval or relabel its accepted money.

Positive DAILY/current physical identity is essential. The native resolver's highest-version fallback145181–145196 is display recovery for inconsistent data, not a current retry target. Historical/mismatched/noncurrent replies must not qualify the mapper or mutation. Preserve the existing lifecycle-only details branch and every non-Daily original field. FE's explicit `fetchTimesheetDetails` object359293–359340 must pass through only this closed fact; otherwise the support never reaches the actual window. Use the active native modal/actor token and full-details refresh so a lifecycle shortcut cannot hide the new status.

## Smallest complete state and control presentation

Use one concise native notice/status near the Daily identity and original facts. Preserve actual Daily worked/booked date, Client and No Contract (Daily) only when those original reader facts exist. Amount/hours must retain their own authority; failed/unavailable money is “—”. Do not add a Banking selection/estimate surface or duplicate the native Timesheet financial detail.

| Native fact/state | Minimum truthful presentation | Necessary action / constraint |
| --- | --- | --- |
| Current genuinely authorised, complete native hours=0 | Authorised state plus actual0.00 approved hours; distinct from failure | No Retry merely because zero. Only show £0.00 where an actual financial field provides it. UI does not certify a revision. |
| Current authorised positive hours with genuine zero money | Keep actual positive approved hours and literal financial zero | Never label this “zero hours” from certified-zero money or pay amount. |
| Exact original withdrawal/revocation authority | Withdrawn with plain native reason where safe | Original permitted authorisation/lifecycle path only; no approved-zero or fake cancellation. |
| Exact positively supplied genuine booking-cancellation authority | Cancelled, separate from Withdrawn and zero | No authority was found in current Daily details; do not invent it from other fields. |
| Current Daily has no TF and no pending/failed outbox fact | Not yet calculated / plain unavailable pay | Existing permitted action or Refresh; not failed, zero or “No real hours entered”. |
| Actual non-authorised native state | Not authorised, with original actionable rate/channel/signature reason if supplied | Preserve original Authorise/Process permissions and their refusal semantics. Retry does not authorise. |
| Valid pending outbox/current processing fact | Updating… / being processed | Refresh reads status; no seconds counter, automatic UI drain or zero. |
| Invalid/mismatched native evidence | Plain blocked/unavailable reason, money “—” | Refresh or existing correction/support route; no guessed failure cause from free text. |
| Recorded native calculation failure | Failed with “Pay for this Daily shift could not be worked out. Retry.” | Retry uses only existing targeted enqueue; no batch abort, new money input or fake success. |
| Outbox/read unavailable, unknown Retry outcome | Could not check/load status; Refresh | Keep the unknown result distinct from recorded calculation failure; no automatic repeat. |

The exact eight card authorities remain distinct even when several produce no payable amount. If genuine cancellation is not present in current native data, show its actual unavailable state rather than claim that named branch is source/app-proved. A cancelled-branch screenshot cannot be invented from QR or withdrawn stamps. That evidence limit must remain explicit in completion/D05; it does not justify adding an unrelated rota/Source reader or cancellation producer.

**Exact existing Retry route:** `POST /api/tsfin/recompute` → `handleTsfinRecompute`172913–172930, registered200203. It is admin-only, accepts `timesheet_ids`, and enqueues the existing `enqueue_ts_financials_priority` with native reason MANUAL. Send one validated exact current physical ID; the original handler is bounded and does not run an inline drain. Acknowledged enqueue means processing requested, not prepared/authorised/paid. Refresh then reads actual details. Preserve unknown-response truth and original permissions; never call `/api/tsfin/queue/drain` from this UI.

Native `/daily-manual-process` is a separate mutation with its own manual/unprocessed/action_flags eligibility (`main.js:206478–206492,252461`). It must not stand in for generic TSFIN Retry, especially for an automatic/SELF_REPORTED Daily. Similarly the original Authorise action may be offered only through its native flags, not used to acknowledge a calculation failure. One visible Retry beside a recorded failure and one Refresh for read/unknown recovery are sufficient; no new command, choice or automatic retry loop is needed.

Current native no-TF badge `main.js:364819–364824` asserts that no real hours were entered. For positively qualified Daily this is false when initial calculation failed; suppress/replace that specific fallback with the new finite fact. Preserve other-scope behavior and original native lifecycle controls. Native generic cancelled detectors136191–136196/206559–206562 are broad consumers, not evidence of a Daily cancellation producer: `timesheet_status_enum` has no CANCELLED; `timesheet_qr_status_enum.CANCELLED` denotes only the QR artifact. Archived alone, false booked, REVOKED, absent TF or a free-text reason cannot prove booking cancellation.

## Required differences from supplied B10 and future A4

SCR156b depicts Candidate Banking, Failed/one-line reason, dash amount, Retry and Open Timesheet. Its identity presumes a listed row. The full DY10 card permits the existing native screen and requires the shift remain visible when calculation fails; it does not supply an unpublished NEXT Work list. Therefore its **first-failure placement changes to the existing Timesheet window/list**, using the same Failed/reason/dash/Retry pattern and exact native identity. Record that required placement difference in BUILD_LOG and the final screen receipt. Existing published Banking rows retain their own reader/reason/retry; this note does not waive their visibility.

Do not copy the supplied obsolete Deductions due aggregate, invented Ready totals/counts, Show estimate/Export CSV/Banking tabs, sort controls or ticks into the Timesheet failure panel. None is needed to retry its calculation. Open Timesheet is unnecessary inside the already open Timesheet; safe native header Close, permitted native lifecycle controls and contextual Retry/Refresh suffice. Keep the existing frame and native tablist, not a second modal/navigation layer. All facts supplied in the subject and every retained control must be checked on actual final pixels.

Minimum actual local capture states after build, dark/device-light/390px-phone: initial failure with no TF/Work but actual entered hours; pending; unprepared; not authorised/rate issue; invalid or unavailable evidence; actual authorised0hours and positive-hours/zero-money distinction; genuine withdrawal; Retry acknowledged then actual status Refresh; unknown Retry/read failure and recovery; failed later calculation beside retained prior approval. Capture genuine cancellation only if its real native authority is found. Inspect all42 policy rows, manual wording lists, every visible design difference and each payroll-needed control. Owner25/spec-readerAdditions26 exemptions apply to the approved actual-product workflow; no A4 PASS is available until those real bytes exist. D05 financial/queue/race/settlement journeys remain AUTHORED / NOT_RUN.

## Reviewed saved identities

Raw SHA-256 before root's DY10 build; these hashes do not claim future product bytes reviewed.

| Source | SHA-256 |
| --- | --- |
| BD109 DY-10-design.md | `2a8d6fd0a650232b6abd58df317fee29f6625ad62dd07a3361bb7e63a50424d2` |
| DY-10-current-investigation.md | `34e0654e8af2befcb34c6bfbc01c2ab5e1f5ae1e44610d620ef6404a96a104ec` |
| BE broker/src/index.js | `020ba5667127beb0abfafa8f57b2e78bb2801d62878858e55b608c1452c45639` |
| FE js/main.js | `50289381a37c18a5c157e73e66e791c342023d9280b1cf5d86d07805e11cb3bd` |
| FE js/bpay-next-office.js | `b70ba97d9ae8f70298121e851f71512948cccf2f271e2c8b4f554ded457bddbb` |
| FE js/bpay-next-workbench.js | `7fad175a5dada59a04ea5967d203cbb5e6d6a078f83c8de9b4ccf54507459178` |

**Verdict: design scope accepted. Preserve every finite state and exact current-ID Retry constraint above; genuine cancellation authority remains an explicit source/evidence limit. No pixel, joined or implementation acceptance claimed.**
