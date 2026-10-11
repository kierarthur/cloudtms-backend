# DR-06 Office and transport source review

11 October 2026. Independent read-only source review of the current uncommitted transport and Office additions. Only this review note is written. Product, SQL execution, tests, fixtures, browser/hosted actions and installation are not performed; application acceptance **NOT_RUN**. Root separately reports SQL0708 local compilation and its source controls; those checks are not performed or independently claimed here.

**Final source verdict: PASS for the inspected transport/Office/optional native-header additions below. All essential findings in the retained review history are corrected.** This is a source verdict only; timing, queue execution, native interaction, browser-close/login recovery and joined app acceptance remain NOT_RUN by this reviewer.

## Initial essential finding

**Reload during sealed-input readback has no recovery action.** The qualified last-run reattach passes `submissionComplete:pointer.readback_complete` (`js/bpay-next-office.js:1240`). An incomplete pointer therefore opens with `submissionComplete=false`, but `submissionError` initializes to an empty string (`:1063`). The existing `retryReadback` action is offered only for a nonempty `submissionError` (`:1076–1085,1094–1096`); no initial restored-pointer branch supplies one. Once the native run reaches REVIEW, Continue to review stays disabled (`:1124`) and this progress window provides no way to finish its retained readback. Qualify the original incomplete pointer/journal and expose this existing same-run readback retry without creating a replacement submission. Reported to root immediately.

## Source checks completed

- Worker and Office add only the existing closed `WAKE:{run_id}` action and singleton `PROGRESS:{run_id}` page. Their exact receipts/rows, singleton envelope, requested run identity, integer conservation, enrolled/preparing bounds and 72-hour timestamp check agree (`broker/src/bpay-next-preparation.js:11–12,29–30,53–54,73,293–300,325–334`; Office `:28,48,69,97,405–414`). Existing request/response bounds, active-admin admission, deadline and mutation ambiguity handling remain (`preparation.js:6,528–553`).
- WAKE adds the existing internal ENROLL header only for validated `accepted:true`; `accepted` and `throttled` must differ (`preparation.js:300,549`; Office `:307`). The existing index schedules `{id:'1',kind:'ENROLL'}` and preserves durable recovery on publication failure (`broker/src/index.js:198856–198879`). No browser financial step, new route, queue kind or monetary formula is introduced. The index's seal-only comment at198865 is stale commentary, not a dispatch defect.
- The progress timer performs only PROGRESS reads, with one read in flight, ticket/current checks, native dismiss cleanup and visible/active gating. REVIEW is reached by explicit existing review-window actions, rather than a timer (`Office:1068–1088,1125–1130`). Failed reads retain earlier figures visibly stale and disable forward/wake/issue actions (`:1093–1096,1104,1119–1124`). PREPARING with zero pending remains a finishing state; counts do not infer REVIEW (`:1101,1111,1124`).
- Navigation retains one actor/agency/owner-qualified pointer and its original namespace, command and sequence; it performs no journal census (`:1036–1050,1240`). Progress's WAKE controller uses a separate run/agency/owner-qualified key and checks its run/action state, preserving old controller/submit journals (`:790,1064–1066`). Existing unknown submission bytes and readback-before-ack structure remain (`:607–643,748–759,1199–1222`). Exact persistence/acknowledgement and restored readback handling remain under focused review.

Initial inspected saved bytes: Worker SHA-256 `C7A106DE1840E6B6C82A113BBE5B882BDB571F68F670B06965CD5F5B1C0351FB`; Office SHA-256 `0540E260F0A2DB7B6C47D7F1D1C511BF05E9EF8D72E06CAB53A35BBFE8481FE9`. This is an open source review until the essential recovery finding is corrected and re-read. It is not item completion or runtime acceptance.

## Further focused findings and correction history

**Native caller lacked the required controller UUID dependency.** The actual `main.js:7159–7207` options supplied to Office do not include `uuid`. The initial progress controller spread those options without the existing fallback (`Office:1064`), while `createController:788` requires a UUID function; native accepted-seal/reattach opening would therefore fail before rendering progress. Existing review/root controllers already use `options.uuid || (()=>win.crypto.randomUUID())` (`:1030,1361`). Reuse that dependency fallback; WAKE itself still allocates no new run or command identity. Reported to root immediately; correction pending source re-read.

**Acknowledgement failure was ignored.** The new last-run persistence check can cause `acknowledgeComplete` to return false before retiring the original journal (`:751`). The initial submit driver ignored that result and marked `finished`/`submissionRecovered` (`:1217`). Root has now changed the caller to require true before reporting success, preserving exact recoverable submission state when persistence/acknowledgement fails (`current:1221`). This finding is closed by that source change, without a runtime claim.

Root also added `restoredReadbackPending` to the qualified reattach and initializes the existing inline readback retry message from it (`current:1065,1244`). This closes the missing recovery-action finding at source level; the same retained descriptor/run/command check and read-only readback are reused. Focused review of final bytes and native caller activation continues.

The existing native standard adapter invokes `onMount` again when a retained frame is restored (`js/bpay-next-ui.js:38–58`), so the progress window's `onMount` factual read resumes its active/visible polling rather than requiring a new modal-activation protocol. The normal caller's `ownerReady` guard is retained (`main.js:7205`), and progress additionally checks actor/agency/owner namespace against current Workbench context (`Office:1061–1062`, now also before queued reattach). No additional server feature is required for these corrections.

## Final-byte follow-up still open

**Interrupted accepted-seal journalling can retain an exact unknown SEAL request.** The original dispatch now saves the accepted last-run pointer before saving its next READBACK journal state (`Office:636–641`). A close/crash between those writes can therefore leave an incomplete pointer alongside the original pending `SEAL_ENQUEUE` bytes. Current restored `retryReadback` rejects every pending journal before attempting recovery (`:1079–1088`, especially1085). Its new inline Try again consequently cannot complete this valid original operation. Restore only the qualified same-run/same-command accepted-seal pending state, replay its existing `submission.retry()` unchanged, then require the original READBACK/COMPLETE state and perform the existing readback. Do not allocate IDs or rebuild selection. Reported to root immediately; source review remains open for this exact edge.

The native UUID fallback is now present (`Office:1067`), both explicit review actions require completed original readback (`:1090–1091,1123–1127`), and queued reattach checks current scope and namespace actor (`:1056,1244`). PROGRESS alone now has a2000ms Worker dependency limit and Office read timeout; other route/mutation limits and ambiguity semantics remain unchanged (`preparation.js:539–544`; Office `:335,365,404`). More open state and focus continuity survive factual rerenders (`Office:1066,1095,1122,1128`). No further essential defect was found in these inspected changes.

Follow-up saved bytes: Worker SHA-256 `C18AF4CE82DA211592931BB9ACC8F881698233909787CB0CDEF25B7798BFDDC4`; Office SHA-256 `1BDF466B4AC982A498AF5EAEC9B7A410AA9B552D14F3C89573F7F687A5717D86`. No runtime, timing, joined journey, screen or item acceptance is claimed.

## Final correction re-read

The retained pending-seal edge is now closed. Restored recovery validates the original namespace and same run/command, permits only pending `SEAL_ENQUEUE` in its original SEAL phase, calls the existing exact-byte `submission.retry()`, and then requires READBACK/COMPLETE before finishing the original readback (`Office:1079–1090`). The existing submission validator binds those seal bytes to the same command/run and original page/count; dispatch rechecks the command's accepted sequence (`:559–576,636–641`). `resumeOnly:true` and a refusing UUID callback prevent replacement creation. Interrupted/lost outcomes remain retained rather than reset. Direct restored readback keeps the original submission journal; its completion pointer is navigation/readback metadata, not a replacement memory acknowledgement.

The normal native caller now receives the same UUID dependency fallback used by existing controllers (`Office:1067`). The submit driver requires a successful original acknowledgement before reporting recovery (`:1223`), while its closure retains errors through `submissionFailure`; the progress action does not infer closure success. Both issue and Continue actions stay gated by completed readback and fresh progress; timer and Refresh remain read-only. Actor/NEXT/current actor-agency-owner scope, original pointer namespace, single retained run address and old uncertain submit bytes remain intact. No financial command/step or unapproved list is introduced.

The optional `headerDetail` adapter parameter changes only where the caller supplies that existing DOM node (`js/bpay-next-ui.js:34,54`); other native windows keep their original path. Progress fills it from the validated original actor/start with textContent and existing London timestamp formatting (`Office:1060,1105,1134`). Standard stack, mount/restore, close, footer ownership and focus lifecycle are retained. Future DR-15 attention/excluded detail lists and DR-09 Discard are absent, as required by current card/owner scope.

Final inspected SHA-256:

| Source | Hash |
| --- | --- |
| `broker/src/bpay-next-preparation.js` | `C18AF4CE82DA211592931BB9ACC8F881698233909787CB0CDEF25B7798BFDDC4` |
| `js/bpay-next-office.js` | `DE90D938FBEB684A4EE2087CD9306D65B01B75160430342843A7FF9E286720CD` |
| `js/bpay-next-ui.js` | `28DE1F5DB7C0E7620E4D4EB8698CB7A7A2ABF9C54BB4C779584A83C802BB3A6D` |

No essential source defect remains in these inspected bytes. Root's separate screen/compile/source evidence and subsequent saved changes require their own precise receipts; this review does not claim them or mark the card built.

## Final native-header and presentation amendment

Re-read of subsequent saved bytes: source PASS remains. The optional detail is now a sibling immediately after `modalTitle`, with one marker on the same retained DOM node; mount removes older marker nodes and dismiss removes this frame's own detail (`js/bpay-next-ui.js:54–56,63`). Normal callers without a detail retain their existing path and clear a prior frame's optional detail when mounted. Stack identity, close/restore, footer ownership and `onMount` semantics are unchanged. The initial hidden span uses the existing `data-next-live-copy` marker so native empty-copy sanitation can preserve it until validated progress supplies actor/start text (`Office:1060,1105`). Root reports the original sanitation/render observation separately; this reviewer performs no screen action.

Actor/NEXT/agency/owner invalidation now hides and empties the original actor/start detail, replaces the body with unavailable copy and removes controls (`Office:1095–1096`). The timer is cleared before that branch, and late reads still require current identity/ticket before updating facts (`:1071–1075`). The header cannot retain metadata from an invalidated actor/scope.

Footer reasons explain unfinished/read-failed/unknown-request/readback gates using their existing state, without relaxing Continue or issue admission (`Office:1124–1130`). The REVIEW Safe-close note now says only to reopen the Draft, while PREPARING retains the continuing-build copy (`:1120`). The CSS header grid is restricted to the progress subtitle within the native NEXT read window, including the hidden-state override (`css/bpay-next-office.css:442–448`); the phone More placement adds only the scoped left0/right-auto override (`:481`). No controller, journal, route, money calculation, queue payload or later-card reader/action changes are introduced.

Latest inspected SHA-256 supersedes the earlier presentation hashes:

| Source | Hash |
| --- | --- |
| `broker/src/bpay-next-preparation.js` | `C18AF4CE82DA211592931BB9ACC8F881698233909787CB0CDEF25B7798BFDDC4` |
| `js/bpay-next-office.js` | `81873E385B90FE87BC09A36B09DA30F036604DE4F9A47EE0773BFBDC488C5DBF` |
| `js/bpay-next-ui.js` | `A4AE919379621A21E68EF9955BC4A8CEBB5E37F20EDE56691EEF7A16B60D0EE5` |
| `css/bpay-next-office.css` | `44DAAB79C1AB05067DD473C157BDA99BB21F2870A825867E42DDF67030667885` |

No essential source defect remains in this amendment. Native interaction, installed queue/WAKE processing, two-second end-to-end timings, browser-close/login recovery, screenshots and joined application acceptance remain **NOT_RUN by this reviewer**. Root's separate A4 screen evidence does not establish those application journeys.

## Final committed frontend checkpoint

The current local frontend HEAD is verified as `c059f3078c4cb8124492bd9ea22dfbbd5da993d5`. The final small change displays More only for a factual PREPARING run without read error or unknown pending request; WAKE closes its popup before dispatch (`Office:1078,1125`). Header invalidation still hides and clears actor/start (`:1096`). These presentation gates preserve the reviewed exact submission/WAKE journal and read-only polling paths; source PASS remains.

Final frontend saved SHA-256: Office `28EBFFC719FB87EA5D10AB9CDB9EC1694A9F9681A72A7D5DD6E1E3604BF5C621`; native UI adapter `A4AE919379621A21E68EF9955BC4A8CEBB5E37F20EDE56691EEF7A16B60D0EE5`; Office CSS `A38E4C1F6D0463F616A2E61B355DAA9D46C928B387F622FFAF6A2207AEB61355`. This records the local source commit and current bytes only; no remote publication or installation is inferred. The application/runtime acceptance boundaries above remain unchanged.
