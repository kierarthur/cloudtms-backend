# DR-02 supplied screen investigation

Read-only design/source investigation, 11 October 2026. **A4 screen acceptance is pending actual product captures.** The complete DR-02 card and its thirteen checklist rows were read, together with the current owner screen instructions, policy, tools/policy.json, indexed B4 specs, T01/T07a references and current saved frontend. Only this note is written. No product, design pack, generator, renderer, test, database, fixture, hosted call or installation is involved.

## Scope and current source checkpoint

DR-02-D10/D11 join one Create Draft action to the existing accepted-choice submission. Preserve the original CREATE → finite Work/component/exclusion/matching/Case uploads → SEAL_ENQUEUE → ENROLL → CLAIM/PIN chain, original identities/bytes, sealed count, earlier-command barrier and accepted input revisions. No browser money calculation, new submission controller or reconstructed agency-wide list is required. Future DR-10 Guard and DR-06 full preparation progress remain with those cards.

The root is implementing concurrently. The previous DR-02-current-investigation.md records the earlier missing join; the saved source now already contains the HEADER can_create_draft field, main Create Draft/footer controls, openCreateDraftConfirmation, sealEnqueue and a bounded progress placeholder. This note describes the required final behavior, not an approval of this moving checkpoint.

- FE js/bpay-next-workbench.js: HEADER row schema at 15 onward; mountWorkbenchShell at 802; current main footer at 813; permission/busy condition at 831; snapshot exposes canCreateDraft. Preserve existing server sorts, continuous scrolling, native columns and ready/issue joins.
- Same file: createChoiceStore at 968 and freeze at 1031; Ready countSelection at 1121. CountSelection makes the existing HEADER read with current intent/memory and rejects changes across its await. Use its Candidate count; do not count loaded rows or derive a total by subtraction.
- FE js/bpay-next-office.js: createChoiceSubmission at 511 retains the original frozen descriptor and pending bytes; advanceSelection/advanceCases and new sealEnqueue at 717 are the existing chain. Readback and revision-checked acknowledgeComplete remain the completion seam. The SCR160 confirmation at 880 uses the existing store/count; the named preparation opener is at 912.
- FE js/bpay-next-ui.js: createStandardWindowAdapter at 31 supplies the real modal stack, active/alive checks, small geometry, confirmation alertdialog title, safe initial focus and guarded close. Reuse it; do not add another overlay.
- FE css/bpay-next-office.css: existing confirmation/native action styles and new DR02 selectors at 373 onward provide the small question/values layout. Keep one primary; Export CSV and Cancel must survive native verb styling as secondary.

## Exact supplied references

Paths below are relative to the read-only SCREEN_DESIGN_POLICY_20261010 pack. All six B4 PNGs and the three T01/T07a PNGs were individually opened; matching B4 JSON, including reviewNotes, was read. DESIGN_INDEX maps the final SCR160 variants to B4; T07a is a template reference.

| Screen | Spec and actual reference PNG | Required appearance and behavior |
| --- | --- | --- |
| SCR-160 | batches/B4/specs/SCR-160_create_draft_confirm.json; batches/B4/png/SCR-160_create_draft_confirm_dark.png | Small confirmation over retained parent; server-count question, exact cut-off consequence, genuine scope/local selection values; Cancel then Create Draft. No duplicate header Close. |
| SCR-160b | Same directories, SCR-160b_create_draft_counting.json / SCR-160b_create_draft_counting_dark.png | Create Draft? and Counting ticked Candidates…; disabled primary, enabled safe Cancel. No invented count. |
| SCR-160c | Same directories, SCR-160c_create_draft_count_failed.json / SCR-160c_create_draft_count_failed_dark.png | Could not count the ticked Candidates. / Nothing was created. Your ticks are kept. Visible danger notice: Try again. If it keeps happening, contact support. Cancel then Try again; retry repeats only the count read. |
| SCR-38 | Same directories, SCR-38_create_draft_sending.json / SCR-38_create_draft_sending_dark.png | Small Creating Draft window, Sending the ticked payments… and Keep this window open until the Draft starts. Duplicate Create disabled while uploads/receipt are pending. |
| SCR-38b | Same directories, SCR-38b_create_draft_not_allowed.json / SCR-38b_create_draft_not_allowed_dark.png | Main Create Draft disabled; known false permission displays You do not have permission to create Drafts. in the footer hint. Do not substitute environment readiness for permission. |
| SCR-38c | Same directories, SCR-38c_create_draft_not_started.json / SCR-38c_create_draft_not_started_dark.png | Small failure/retry window, original ticks/request retained. The pictured definite refusal sentence applies only when that fact is proved. |
| T01 parent | specs/T01_ready_list.json; png/T01_ready_list_dark.png and T01_ready_list_devicelight.png | Existing Banking Pay main layout, footer Export CSV secondary and Create Draft sole primary; dark in both device settings. |
| T07a pattern | specs/T07a_create_draft_confirm.json; png/T07a_create_draft_confirm_dark.png | Same small question, values and two-action pattern. Final indexed B4 instructions control where illustrative values differ. |

## Narrow required UI changes and findings

1. Wire the main action through current HEADER permission, coherent Ready/store/context, actual owner/actor and no pending/busy submission. Open SCR160 first and run only countSelection. Cancel closes without CREATE. The affirmative action must recheck the counted store revision, namespace/context, actor/owner and initiating frame before freezing/dispatch; stale count or changed ticks cannot silently proceed.
2. Use the exact consequence: **The Draft will include exactly what is ticked now. Anything that changes afterwards goes to the next Draft.** Values show the genuine channel/filter scope and current local selection label. ALL_MATCHING exceptions may read All except N; explicit/NONE intent must keep its real label. Use singular Candidate for an authoritative count of one.
3. During count failure show the supplied danger retry/support notice. In the current new function, result is always hidden and the catch does not populate that notice. Also protect consequence, loading and error copy with data-next-live-copy; this native cleanup requirement was demonstrated and fixed in WB-38. These are narrow current source findings, sent to the builder immediately.
4. Show the supplied permission reason only for a genuine false HEADER flag. Show the actual loading/stale/owner/submit reason for other disabled states. A disabled control alone does not explain why staff cannot continue. A genuine zero Candidate count keeps Create disabled with a plain Nothing to pay reason; a failed count must not become zero.
5. Retain one immutable original submission through sending, refusal and unknown reply. Manual Try again retries its exact pending bytes or resumes its accepted phase; it must not re-count/re-freeze current ticks or generate replacement run/command IDs. A later refused page cannot erase an earlier accepted CREATE or claim no Draft exists.
6. On the correlated SEAL_ENQUEUE receipt invoke openBpayNextPreparationProgress with that exact run identity. DR-02's placeholder is temporary; do not add DR-06 bars/counts, author/expiry, Continue to review or accepted-Draft discard actions now. Its initial title/copy say Draft preparation; tools/policy.json forbids preparation in public copy, so use Draft creation wording and keep the ID internal. Remove the redundant footer Close when native header Close already dismisses this status. Browser close after accepted seal does not cancel the server preparation. Preserve recoverable uploads before seal and bounded original readback.
7. Remove Add selected Work page and Prepare selected Work from the normal Sheet intake; selection already belongs in the Workbench. Preserve the current native parent/Candidate caret, inline Open and More, Due/This Draft and local footer patterns from the owner's 11 October correction. DR-02 does not authorize another redesign of those completed screens.

## Every proposed control and required difference

| Visible control or difference | Payroll need / exact authority |
| --- | --- |
| Main Create Draft | Starts the selected Draft intake once; DR-02-D10. Visible label is canonical Create Draft under SDP-56/70, although the card says Create drafts. |
| Export CSV outline | Existing selected read export; T01 secondary action. It must not compete with the Draft action as another primary. |
| Confirmation Cancel | Allows staff to review ticks or leave while counting without submitting anything; SCR160/SDP-55. |
| Confirmation Create Draft | Affirms the displayed count/scope and exact cut-off; SCR160/SDP-08. No password/code or bank execution step is required for this selection action. |
| Count-failure Try again | Repeats the failed pure count read; SCR160c. No business request or zero fallback. |
| Sending header Close | Native dismissal only; does not cancel an accepted intake or imply that unsealed uploads will continue after the browser exits. Keep the supplied pre-seal open-window instruction visible. |
| Request-failure/unknown Try again | Continues the original retained request/result; SCR38c's replay rule and DR-02 identity contract. |
| Placeholder dismissal | Allows staff to close the temporary accepted-seal status. Use the native header Close without a redundant footer Close. Full DR-06 controls come later. |
| Omit sending/failure PAYE and Umbrella numerical subtitle | SCR38's own reviewNotes and SDP-08/09 say no sending channel counts; no current tick reader supplies that split. The PNG's illustrative subtitle must not become invented data. |
| Truthful unknown-result wording | SCR38c's illustration says The Draft was not started, but its retry note permits a request that arrived before a lost reply. Unknown copy must say it could not be confirmed and offer exact retry. Only a proven pre-admission refusal may claim not started. |
| No pay date, money estimate or technical request details | SDP-12a, WB-12 server-count seam and owner A2. Namespace date/IDs remain internal; no extra reader or field is introduced to match a decorative illustration. |
| Temporary accepted-seal status | Card D10 explicitly specifies Preparation started — run <id> until DR-06. Policy public wording forbids preparation and UUIDs. Use plain Draft creation status, retain the actual run ID internally and pass it to the named opener. Record this precise presentation departure. The initial source wording and redundant Close are findings, not approved alternatives. This is not full progress acceptance. |
| Genuine sample rows/counts and retained native parent | References contain illustrative data and some earlier parent controls. Use actual reader fields and completed card controls; record each visible sample/owner difference during A4. No future capability is added solely because the illustration shows it. |

## Required actual screenshot states

Capture the saved real Office entry with the smallest fixed responses and hosted network blocked; supplied reference PNGs are not product evidence. Dark desktop, device-light desktop and 390px phone are required for each distinct visible state below, with extra phone scrolling only if needed to see the actual controls.

| Actual state | What A4 must see |
| --- | --- |
| Main permitted and not permitted | One primary Create Draft, secondary Export CSV; known false permission has its exact visible reason. Existing Ready screen remains usable and unchanged except the named footer join. |
| Counting | Exact consequence, loading sentence, disabled primary, safe Cancel; no pre-count figure. |
| Counted confirmation | Authoritative Candidate count, real scope and All except N or explicit selection label; two controls, no date/money/channel split. Include a single-Candidate variant if plural wording changes. |
| Failed count and retry | Supplied danger notice and two actions; no zero/blank replacement; original ticks retained. |
| Successful zero count | Truthful zero/no payable selection with disabled Create reason. A deductions-only Candidate remains unticked and excluded from the server count. |
| Sending | Native small Creating Draft state, visible loading/open-window instruction, duplicate action disabled; no fabricated progress/counts or cancel-success promise. |
| Proven pre-admission refusal | Actual plain reason, ticks retained, retry only where the closed contract permits it. Do not use this image for a later-page failure. |
| Unknown reply / accepted partial-upload failure | Truthful retained-original outcome, exact Try again, conflicting/new Create unavailable. Distinguish this from definite nothing-created copy. |
| Accepted seal placeholder | Only after its actual correlated receipt, exact original run passed internally, bounded temporary status/dismissal, no future progress controls. |
| Return to parent after Cancel/Close | Original ticks and genuine enabled/disabled main action restored; no duplicate windows or stale focus. This can share an existing main capture if the actual close interaction produced it. |

Actor/owner/revision changes, original journal restoration, unknown exact retry, sealed readback equality and the card's 40 Timesheets/12 Candidates journey also need the normal later joined-app evidence. They are **AUTHORED / NOT_RUN** here; fixed local screenshots cannot prove database, queue, browser-restart or background-worker behavior.

The final A4 review must pair every actual image with its applicable supplied PNG and record every visible difference and every control's payroll purpose, then complete all 42 policy rows and user-eyes checks. Item 16 uses the actual tools/policy.json wording lists manually. Owner exceptions keep item 25 and readerAdditions outside generation; other N/A results must be genuinely outside this card's actual surface. No visual PASS is claimed by this investigation.


## Read checkpoint hashes

PACK denotes the read-only ordinary backend design pack; FE denotes the managed frontend worktree. These SHA-256 values identify the investigation checkpoint, including concurrent work in progress. They are not final implementation or screenshot acceptance hashes. T01 dark and device-light are intentionally identical bytes.

| Read file | SHA-256 |
| --- | --- |
| PACK batches/B4/specs/SCR-160_create_draft_confirm.json | `106dbaa0795b9aeed4d8b3af5ce6616bf5d4421a270a953688458c64f8ae4fcd` |
| PACK batches/B4/png/SCR-160_create_draft_confirm_dark.png | `f790b89112763839931f091dca7100d1986ecb986f2767e3ad5a0bc9e86c0229` |
| PACK batches/B4/specs/SCR-160b_create_draft_counting.json | `1e0992e9cc80861988f2466c0a136e852dba81c27c782d23039e5db01f852f12` |
| PACK batches/B4/png/SCR-160b_create_draft_counting_dark.png | `74ea4c8fe813febae3452a20fa7cee43564e6e4e9e9edfad25c8de2257549c86` |
| PACK batches/B4/specs/SCR-160c_create_draft_count_failed.json | `1436a92321ae6882ed039a5674bf56db069b388aacfb3e18f6116f6d17c4ef79` |
| PACK batches/B4/png/SCR-160c_create_draft_count_failed_dark.png | `99f9216b2bf9fdda033e57eaa183ce8d8e06aabc4c861a0df80bb1b17b79afa0` |
| PACK batches/B4/specs/SCR-38_create_draft_sending.json | `8a8860ee37c45ff4fbef2e0cb6f8fccec17b3303a5edbf58d2e241d2833f6509` |
| PACK batches/B4/png/SCR-38_create_draft_sending_dark.png | `352dbdc931cf398edad617686635dd2913a3a96683fd30a3710ef529729c595e` |
| PACK batches/B4/specs/SCR-38b_create_draft_not_allowed.json | `2debadafbafb7d4153ed87db178ce29ebda1437a3fd1899ba528cf86cef050dc` |
| PACK batches/B4/png/SCR-38b_create_draft_not_allowed_dark.png | `c7453e5d89ea23ab48cc1629f1ea97c1fb2b663b493dba0aad6afb2259584ba8` |
| PACK batches/B4/specs/SCR-38c_create_draft_not_started.json | `7e9e6f3a23e37039e6616856dc93ead70b4a5129b01f9e08809d124a9072e6c4` |
| PACK batches/B4/png/SCR-38c_create_draft_not_started_dark.png | `06384f35bb061a5685cbb4630d722c0f9c4a9c07b0661268603a15b792ca8ef5` |
| PACK SCREEN_DESIGN_POLICY.md | `8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5` |
| PACK DESIGN_INDEX.md | `cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe` |
| PACK tools/policy.json | `45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d` |
| PACK specs/T01_ready_list.json | `3feb615e970639be63a6e5b6f23d74848e3fa02b8b5e19c81458a53abd86fade` |
| PACK specs/T07a_create_draft_confirm.json | `d3017832b7891f9597c10548c0e8b536caff9452575bf83bf98db04321398fa2` |
| PACK png/T01_ready_list_dark.png | `ba3d7a1d0becd5b0c7209ac75e538958d87807a68b7cac33482df11b629d7b96` |
| PACK png/T01_ready_list_devicelight.png | `ba3d7a1d0becd5b0c7209ac75e538958d87807a68b7cac33482df11b629d7b96` |
| PACK png/T07a_create_draft_confirm_dark.png | `e38dcce12df9ea789a4b375ff9073b386962efefabf6af55984a648abd16064c` |
| FE js/bpay-next-office.js | `599bf1bcb7e386155f02756110e6791338a8e45a630c4320bc5b4df6ce303579` |
| FE js/bpay-next-workbench.js | `3b1f1b778d1483a865a1ba28ee5d874c117f15cbdd503ce4c700c49341c16ea5` |
| FE js/bpay-next-ui.js | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
| FE css/bpay-next-office.css | `041eb107116eb7879933280261070b4fc19f6c5741964dd0f879d523ebea3f0b` |
