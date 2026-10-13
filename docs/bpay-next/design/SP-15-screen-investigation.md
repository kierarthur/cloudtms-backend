# SP-15 screen investigation — sequence 113

Source-only investigation of D04/D05. No product, design pack, reader, command or database was changed; no screen was rendered. This is an implementation boundary, not A4 or joined application acceptance.

## Required minimum

The full card is `05_SOURCE_AND_PROTECTED_PAY.md:515–542`; D04 requires two owned Source code messages and a real **Open Timesheet** action. D05 verifies the existing Weekly Source commands and relevant-query hide behavior. Owner E2 makes a query an automatic release; do not introduce a query-only Candidate REVIEW state, preparation retry, recovery pause or the future full DR15 Review screen.

Add one closed two-code table in the existing Office module and reuse the current accepted-run Review host. The card's exact entries are:

| Native code | Card-owned message |
| --- | --- |
| `SOURCE_PAY_QUERY_OPEN` | Pay Query open on this Timesheet — resolve it in Weekly Source Office Checks |
| `SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE` | Pay Query evidence could not be confirmed — check the Timesheet's queries |

Other issue codes retain their existing owners. Render the Source reason only when supplied by the current exact worker STATUS; do not translate an unrelated refusal into a query. Its Timesheet action belongs inline beside that reason, with the current host's existing footer retained. Do not add a duplicate footer action, full reason table or Source workspace route.

## Existing factual mapping is sufficient

`js/bpay-next-office.js:202` STATUS supplies `run_id`, `run_worker_id`, `candidate_id`, `worker_status`, `review_issue_code` and nullable `review_issue_work_id`. The issue Work UUID is **not** a physical Timesheet UUID.

`CURRENT_WORK` already takes `{candidate_id, after_week, after_work_id, limit}` and returns `work_id`, `candidate_id`, `original_timesheet_id`, `week_ending_date`, nullable `timesheet_reference`, current revision facts and existing labels. Its final owner is `private.bpay_next_current_work_page_v1(uuid,date,uuid,integer)` in `04102026_0580_banking_pay_next_office_status_v1.sql:7`; the F04 registry confirms that physical owner. It is an exact-Candidate factual list using the existing Candidate/week/id index, at most 100 rows and 120,000 encoded bytes per page. It requires active NEXT but has no READY-worker or parent run-state restriction. It is explicitly not a payability assertion.

Minimum click resolution:

1. Read existing STATUS for the original accepted `run_id` and `run_worker_id`, limit 1. Require its returned run/worker/Candidate to equal the qualified current row and the exact Source code/work pair being displayed.
2. Read CURRENT_WORK for that Candidate, following only its existing `(after_week, after_work_id)` cursor until the exact `work_id === review_issue_work_id` is found or the page is complete. Require the matching row's Candidate identity and a physical `original_timesheet_id`. Do not select the first row, a newest approval, an adjacent Timesheet or an arbitrary fallback.
3. Recheck the actor, NEXT owner/epoch, original accepted-run namespace and live parent frame after each await. Read fresh STATUS before opening and require the same run/worker/Candidate, code, Work and `preparation_revision` as the original issue lookup. Changed/missing evidence produces a plain recoverable notice and existing Refresh; it sends no preparation or Source command.
4. Open the physical ID through the existing native Timesheet child adapter. Supply its real optional `timesheet_reference` when available. CURRENT_WORK supplies no physical version; use null rather than invent one. The native exact-physical-ID check remains mandatory.
5. When the child closes, refresh the original accepted-run issue truth under the same parent/actor qualification. Do not recapture selections, undo a payment, pause recovery or resolve a query as a navigation side effect.

This uses only existing STATUS/CURRENT_WORK pages and the original native Timesheet opener. It needs no SQL, new server field, new page, new financial authority or live Contract/Client inference. If the exact Work is absent or native current physical identity has rotated, fail closed and offer Refresh.

### Readers which must not be substituted

REVISION_COMPARISON is typed as `{run_work_id, work_id, original_timesheet_id, captured_revision_id, current_revision_id, newer_approval_available}`. However, its native owner `03102026_2359_banking_pay_next_simple_review_confirm_v1.sql:206` requires parent REVIEW/DRAFT **and worker READY**. FROZEN/RATE_SCHEDULE similarly requires READY in `04102026_0025_banking_pay_next_frozen_detail_pages_v1.sql:141`. Neither serves an automatically released CANCELLED Source issue; do not weaken those guards to obtain a navigation address.

The existing frozen wrapper `11102026_1005_banking_pay_next_frozen_client_origin_v1.sql` adds captured origin to four existing frozen kinds. There is no separate ORIGIN kind in the current Office contract. Frozen origin's physical version/name fields are not an excuse to create a new reader or require frozen financial preparation for this link.

## Native caller boundary

`js/main.js:7185` has the existing Workbench `issueOwners.timesheet` handoff. It explicitly rejects `issue.accepted_run_id`; that guard protects the pre-Create Workbench caller and must remain intact. Root's selected integration is a separate `issueOwners.reviewTimesheet` callback, sharing only the existing native physical-navigation lifecycle. Accept only an original accepted run/worker/Candidate, one of the two owned Source codes and the exact Work resolved above, with unchanged preparation revision. Do not broaden the original callback's contract.

Reuse the native `openTimesheet({timesheet_id, __bpayNextExactPhysicalId, __bpayNextExactPhysicalVersion:null, candidate_id, reference_number})` child lifecycle: preserve the live parent frame, restore its footer/tab state on child dismissal, and recheck the original actor/NEXT/parent afterwards. `main.js:148890` already rejects a fetched physical ID mismatch and checks a version when one was actually supplied. Do not treat a Work UUID as a Timesheet, remove the physical guard or fall through to a legacy Banking read for a qualified NEXT caller.

The normal `openWorkerReview` at Office:1491–1590 is the required current host, rather than the old thin diagnostic selected-worker table around 1988. Its generic preparation error must not conceal a supplied Source-owned reason or promise preparation retry for an automatic release. DR15 still owns factual human Candidate identities and the complete future Review composition; this change does not fabricate them.

## D05 is already authored

`weekly-source/workspace-actions.js:1047–1074` keeps one `state.pending` OPEN_MANUAL_REVIEW payload, including `source_row_id`, trimmed Reason and one `command_id`, created before the first send and reused after a lost reply. Reason remains locked while that exact request is pending. `openCommand` at 922–951 assigns the RESOLVE_MANUAL_REVIEW UUID once outside the retry handler and posts the same cloned payload on retry. Both retain original actor/frame guards. The upload detail at 1014 hides Send to Pay Queries when `pay_query_open === true` and requires a real Source row ID. Existing remedies retain their original command paths; no new invoice, pay or query command is warranted by this screen row.

These are static observations only. Genuine relevant-query behavior, command replay, Source remedy completion and subsequent admission remain joined NOT_RUN.

## Supplied screen references and future evidence

DESIGN_INDEX maps SCR-142/B10 to root `png/T04b_action_required_dark.png` and `png/SCR-08b_issue_detail_pay_query_dark.png`, with corresponding root specs. Those supplied images were independently viewed read-only. SCR-45/B4 (`batches/B4/png/SCR-45_review_reasons_dark.png`, its phone image and SCR-45b reason wording) illustrates the later complete DR15 Review, and does not authorize that whole screen here.

SCR-08b contains a Candidate/name/reference, query Reason, Raised by/time, held and already-paid amounts, and Open in Source Checks. STATUS/CURRENT_WORK do not supply those query display facts or balances. Preserve WB07's Source-owned display reader and existing Timesheet Source/Office Checks route; omit unsupported illustration values and do not infer them from the five-field gate or add a second Source command/navigation protocol. The card-owned inline reason plus real Open Timesheet is the necessary current D04 adaptation. Root should record these differences before A4.

After root implementation, actual local dark/device-light/390-phone screenshots must cover both codes, an automatically released worker, real physical child opening/parent return where the harness can exercise it, loading, failed/missing Work evidence and changed actor/parent qualification. Review the actual image beside the supplied references, every visible difference and control purpose, all 42 policy rows, manual `tools/policy.json` wording, and phone touch/overflow behavior. No screenshot or full application pass is claimed here; later DR15 identity/composition and business replay acceptance remain explicitly deferred/NOT_RUN.

## Saved-source checkpoint hashes

| BUILD-FE file | SHA-256 |
| --- | --- |
| js/bpay-next-office.js | 64f8eafd29c1eb002a8f445860282f61eaf9c314f1c292a2dd862203aca2798d |
| js/bpay-next-workbench.js | 499e2c2fd3468c8801c86c1a1ab409792b19be47390c069a2f73fb62de0061cb |
| js/main.js | 358b817d22ca9b0affaeb210486d655600595e60628afdbf967ccf0cedc072e8 |
| js/weekly-source/workspace-actions.js | 66345020ef21fdb2b71db996b8d9fbff100f1bbc9d4694dc2659b60cab5b416a |
| js/weekly-source/import-workspace.js | 7d4621851bf5c5e5b14c0dbd2f149291a16f34ae050778a952cfc203855d9bb1 |

These precede SP-15 product edits. Re-read actual saved bytes at implementation/review; do not attribute a later successor to this checkpoint.
