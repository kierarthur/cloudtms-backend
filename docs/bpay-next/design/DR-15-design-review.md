# DR-15 independent design review

12 October 2026. Sequence 119. **DESIGN PASS within the conditions below.** Independent saved-source review only; no product/checklist edits, SQL/database calls, business execution, tests, fixtures, rendering or hosted requests. Source implementation, visual acceptance and joined/runtime acceptance remain pending. This does not mark DR-15 built.

## Authority and narrow deliverables

Reviewed the full approved `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:403–423`, R3:140–146 and the DR-15 entry, all eight checklist rows (`IMPLEMENTATION_CHECKLIST.md:3496–3518`), the current investigation and registered owners. D02's closed PREPARE_ISSUES manifest, D04's existing Worker maps, D05's native Office reasons/Not in this Draft, and D07's existing public preparation-page successor are sufficient. D01/D03/D06 remain context. No new command, table, money owner, producer, proof, maintained count or automatic retry is required.

Owner E1 carries actual per-Timesheet click skips into DR-15; E2's six named failures become omissions only after their native SYSTEM release finishes; E3 prohibits capture-time PAY_ON_HOLD; E4 retains real preparation failures To review; E5 retains original sealed inputs. Those directions supersede the old query-to-REVIEW example. Owner A3g supersedes the card's old raw-unknown-code screen wording: retain the typed code in the closed reader/journal, show plain support wording, and do not drop the row. Sources: `BUILD_LOG.md:610–615,1028`; `design/DR-05-worker-reprepare.md:5–9,33–35`; current investigation:7–11,36–70.

The public successor must copy current `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` from `11102026_2259_banking_pay_next_prepare_retry_v1.sql:1346`, keeping its unrelated closed kinds and gates. Original private `bpay_next_simple_review_worker_page_v1(uuid,uuid,integer)` at `03102026_2359_banking_pay_next_simple_review_confirm_v1.sql:165–200` still rejects PREPARING at184–186. It cannot supply the complete PENDING surface, and should not be broadened or reinstalled just to assemble this new read-only branch. The investigation's Workbench reader reference at31 is stale: final navigation owner is `12102026_0235_banking_pay_next_origin_wait_issue_navigation_v1.sql`, rather than0230.

## Native issue membership and identity

The page needs distinct factual buckets, with exact native identities:

- **Not in this Draft:** original Rule B `run_choice_member.selected=true` with non-null native skip; legacy explicit selection-page skips qualified by the original `choices_request_json` expected revision; completed SYSTEM RELEASE requests with their original captured issue. Unticked members, genuine zero results, unfinished release, manual removal, expiry and discard must not be relabelled as rejected preparation failures.
- **To review:** current actual REVIEW worker issue, including PF08 operational attention where its native facts qualify. No synthetic worker ID or generic REVIEW_REQUIRED replaces the issue.
- **Pending:** original selected membership/current native worker preparation or unfinished cleanup. A missing unenrolled worker remains nullable and has no invented mutation target.

Rule B skip authority is the original accepted member, not a later current Work/hold/snooze check. Legacy page labels use an exact explicit expected revision; genuine historical ID-only input cannot supply that revision and must remain honestly unknown. Deduplicate the same accepted skip across Rule B and its legacy acknowledgement without collapsing distinct native issue sources. Native anchors: `11102026_1358_banking_pay_next_guarded_create_v1.sql` accepted members/skip vocabulary; `11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:100–101,192–212`; original ACK2232:200–227,251–254; `private.bpay_next_worker_reprepare_request` and current2259 retirement/receipt owners.

Frozen Work labels must join `(work_id,expected_revision_id)` from the accepted member or `(work_id,captured_revision_id)` from the captured run Work to the immutable revision. Use LEFT joins where historical evidence may be missing so the diagnostic itself survives. Current pointer/history-latest fallbacks and bank-beneficiary names are not frozen Candidate authority. For a Case-only diagnostic, an explicitly current Candidate label from the exact native Candidate point is acceptable; current2259/selection-candidate receipt has no frozen Candidate name to invent. Capture channel/payment provenance from the original native selection where available. Sources: investigation:74–93; current Case audit `11102026_2129_banking_pay_next_case_audit_v1.sql:389–391`.

Preserve the actual finite code census, SP15's exact query copy and stable plain fallback. PAY_ON_HOLD is historical compatibility only; current original-click hold omission is `BPAY_NEXT_SELECTION_ON_HOLD`. `ORIGIN_UNAUTHORISED_WAIT` is a current informational explanation and must not turn an accepted retained Case into a new live exclusion. Signed-line refusals and well-shaped unknown native codes remain visible. No parked2153 accepted-input protocol is activated.

## Existing current-label continuation fence

**The existing global Workbench revision already covers Candidate name/reference updates while NEXT owns the module.** Its actual relation is `private.bpay_next_workbench_revision`, not a new `workbench_control` object. Schema `10102026_1252_banking_pay_next_workbench_summary.sql:6–27` supplies `workbench_revision`, `adoption_revision` and per-Candidate metadata version/name/reference.

Current registered `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` proves the complete seam:

1. `bpay_next_workbench_candidate_name_trigger_v1()` at176–196 is attached AFTER UPDATE of `display_name,first_name,last_name,tms_ref` and the existing pay/bank fields on `public.candidates`. It exits outside NEXT at181, compares the actual canonical name/reference tuple at182–187, and calls metadata_changed at188–189.
2. `bpay_next_workbench_metadata_changed_v1(uuid,boolean,boolean)` at151–173 uses the existing Candidate control lock, updates canonical name/reference and advances `metadata_version` at164–170, then schedules the existing METADATA task at172.
3. `bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean)` advances `private.bpay_next_workbench_revision.workbench_revision` at142–146 in that same transaction. This happens for a changed metadata version even when there are no Work/group rows: the existing-task reuse at110–115 cannot reuse the old metadata version after169 increments it.

These are current final registry owners. Existing source gates name the exact signatures at190–193, their exact call edges at273–274 and308; no generic gate exemption is needed. This is sufficient for the proposed existing module agency/owner epoch + run review_revision/status/selection_state + Workbench workbench_revision/adoption_revision namespace. Do not add a digest, Candidate name vector, new revision clock or metadata producer.

Read context and rows coherently; compare the same existing tuple on every continuation, refusing/restarting a stale page. Current-label tail point revalidation is a permissible small support: compare the exact native source identity and current sort operands, rather than locating a latest revision. Do not acquire new writer locks merely to version this read. Frozen Work names remain frozen even when this conservative global fence changes. Native state changes that alter a diagnostic's bucket/order must fail the existing context/tail qualification rather than silently continue from an obsolete key.

## Closed page, screen and recovery requirements

Candidate/Payment/Reason are actual whole-list server sort keys; Actions is exempt. Use finite reason ordering consistent with the one Office copy table, unique native source ties, and a closed cursor, with explicit null/unknown ordering. Multiple Work omissions for one Candidate cannot use Candidate UUID alone as the continuation identity. Bound relational membership before JSON packing (at most100 rows), enforce the ordinary request/cursor/response budgets, and validate echoed run/context/source identities in Worker and FE. Full-scope counts must derive from native membership, not current page length; no maintained counter is needed. Be honest about residual joined sorting/timeout costs instead of asserting unused indexes provide a constant full path.

Current Office gaps are concrete. `openWorkerReview` uses CURRENT_WORK scanning at `js/bpay-next-office.js:1715–1724` to find query issue navigation; DR-15 must use the exact frozen issue identity supplied by the reader. Both actual hosts need the same manifest and factual labels (`1673–1810` and `2155–2197`), replacing bare generic Candidate/REVIEW_REQUIRED output where those diagnostic rows are shown. Current PREPARING WORKERS calls cannot silently rely on the private2359 restriction. The newly touched Not in this Draft list needs native continuous scrolling; no parent DR16 money-card redesign is authorised.

Open Timesheet only when genuine physical identity is available. Keep current actor/agency/NEXT/endpoint/frame fences after awaits and before navigation/mutation. Try again/Remove use only existing qualified worker routes; skipped/completed-release rows are read-only, PF08 same-job Retry stays distinct from DR05 economic reprepare, and pending/unknown command retries retain exact original journal bytes. The informational list must not add a Confirm gate; existing native unfinished-job/worker controls remain authoritative.

## Saved-source hashes

| File | SHA-256 |
|---|---|
| `design/DR-15-current-investigation.md` | `b3eaf9458a842fea8dafd7d8ed90e295931df52aa270e0f97ca45d81f6cb076d` |
| `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` | `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd` |
| `11102026_2259_banking_pay_next_prepare_retry_v1.sql` | `1a57552c42d63d0dc885a4e7294472c03319537da9e913a96cbbadc10c2ab879` |
| BE `broker/src/bpay-next-preparation.js` | `e930ccece59e729c734725e9b050c8c9c9990a5a97de40773f9ebb134437f475` |
| FE `js/bpay-next-office.js` | `f876b7961aaf8d0cbce0fcbcf981ce8f641ba74a122882c1b4b9836b87578929` |

The narrow design can proceed using these existing owners and fences. This is a design verdict, not a source, visual, runtime or delivery completion claim.
