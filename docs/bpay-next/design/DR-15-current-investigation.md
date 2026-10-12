# DR-15 current investigation

12 October 2026. Sequence119. Read-only source investigation; no product/checklist changes, SQL, tests, fixtures or hosted calls. DR-15 is not marked built. Future runtime, joined, timing and screen acceptance: NOT_RUN.

## Authority and implementation boundary

Read the full owning card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:403–423`, all eight current checklist rows, and R3:140–146 / R3.json's exact DR-15 entry. D02 PREPARE_ISSUES contract/manifest, D04 named Worker maps, D05 native Office reasons/Not in this Draft, and D07 existing public preparation reader successor are the deliverables. D01/D03/D06 are retained context. The cited review for this Draft card is R3, rather than R2's Workbench review; R2's accepted-cut/shared-chain principles still apply.

Owner E1 includes actual per-Timesheet selection skips; A8 explicitly carries those into DR-15 (`BUILD_LOG.md:1028`). E2's six named failures are automatically released, not left as unresolved REVIEW. E3 removed capture-time PAY_ON_HOLD. E4 retains real preparation/technical failures To review. E5 retries the original sealed inputs, not newer approvals/prices. These supersede older card examples (`BUILD_LOG.md:610–615`, `design/DR-05-worker-reprepare.md:5–9,33–35`). A3 screen behavior and all non-action header sorts were adopted at612. Preserve unknown outcome/exact journal bytes and original actor/agency/NEXT invalidation.

Only a narrow read-only successor to the existing preparation reader, closed transport shapes/mappings and the existing native Office host are needed. Do not add a command, financial owner, proof/continuation framework, table, producer, stored maintained count or automatic retry. PREPARE_ISSUES is the exact named card contract; choose its final finite request/result manifestation explicitly, not a generic new diagnostics route.

## Current final owners

| Signature / seam | Current registered physical owner |
|---|---|
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | `11102026_2259_banking_pay_next_prepare_retry_v1.sql:1346` |
| `private.bpay_next_simple_review_worker_page_v1(uuid,uuid,integer)` | `03102026_2359_banking_pay_next_simple_review_confirm_v1.sql:165` |
| `private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)` | `10102026_0639_banking_pay_next_ps_c15_reservation_status_v1.sql:411` issue header |
| `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `12102026_0005_banking_pay_next_source_tax_classification_v1.sql` |
| `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | same0005 |
| `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql` |
| `private.bpay_next_capture_segment_page_v1(uuid,integer)` | `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql` |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `11102026_2129_banking_pay_next_case_audit_v1.sql` |
| `private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer)` | same2129 |
| `private.bpay_next_prepare_retry_handoff_v1(uuid)` / retry association | 2259 |
| `private.bpay_next_begin_prepare_projection_v1(uuid,uuid,bigint,integer)` | 0559:2388–2406 |
| `private.bpay_next_accept_worker_action_v1(uuid,uuid,text,uuid,bigint,uuid)` | 2259 |
| `private.bpay_next_append_work_skip_v1(uuid,uuid,uuid)` | `11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql:104–164` |
| Original Create / append / same-week skip | `11102026_1358_banking_pay_next_guarded_create_v1.sql`; complete appends `11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql` |
| Native WAIT plan/classifier / reader | plan/classifier `12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql`; reader now `12102026_0230_banking_pay_next_origin_wait_navigation_v1.sql`; retained predicate1332 |
| Actual transport/UI | BE `broker/src/bpay-next-preparation.js`, FE `js/bpay-next-office.js` |

Do not edit the original2359 or0575 file or restore it over newer2259. A public successor can assemble the factual native rows itself where necessary, preserving old private reader status constraints and all latest Case/audit/retry/guard routes.

## Closed issue census from actual final definitions

The census used only each signature's current registered complete body, rather than counting obsolete predecessor writers.

| Actual diagnostic writer | Exact issue values / anchors |
|---|---|
| PIN0559:861–868 | SELECTED_WORK_NOT_APPROVED, SELECTED_WORK_REVISION_CHANGED, SELECTED_WORK_POSITION_PENDING |
| capture0005:853–881,904–950,1006–1023,1093–1108 | CAPTURE_POSITION_CHANGED; POSITION_COMPONENT_EVIDENCE_MISSING; POSITION_APPROVAL_AMOUNT_MISMATCH; UMBRELLA_WORK_EXECUTION_UNSUPPORTED; SOURCE_BOOK_APPROVAL_UNBOUND; SOURCE_PAY_QUERY_OPEN; SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE |
| hold0005:1226–1241 | FINANCIAL_RESOLUTION_REQUIRED; TARGET_UMBRELLA_MISSING |
| segment2223:1059–1060,1112–1113 | SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE |
| Case prepare2129:575–629,674,718–719,907–943 | CASE_SELECTION_REQUIRED; CASE_TARGET_UNSUPPORTED; CASE_INPUT_UNBOUND; CASE_WEEK_BASIS_UNBOUND; CASE_RETURN_FLOOR_UNBOUND; CASE_NO_PAYABLE_AMOUNT. The caught BPAY_NEXT_CASE_* values are converted to the same unprefixed finite issue codes. |
| Case PAYE-net2129:1076–1099,1141,1314 | CASE_WEEK_BASIS_UNBOUND; CASE_RETURN_FLOOR_UNBOUND; CASE_INPUT_UNBOUND |
| PF08 handoff2259:50–52 | PF08_NEEDS_ATTENTION; native job retains retry_prior_issue_code/work_id and first/last recorded error metadata. |

BE `bpay-next-job-step.js:52–60` PREPARE_ISSUES currently enumerates the first nineteen issues above, including the six Case codes. PF08_NEEDS_ATTENTION is admitted only by the separate retired-prepare branch, not normal PREPARE outputs. Preserve that separation. DR-15's display manifest must also include this current worker issue, the selection-skip union below, exact ORIGIN_UNAUTHORISED_WAIT and the card's signed-component refusal. Unknown well-shaped native issue codes remain visible according to the card's Contact support rule; do not reject/drop their rows merely because their copy is not in the manifest. Known technical codes receive plain copy, never raw database error text.

SOURCE_PAY_QUERY_OPEN and SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE must use the existing SP-15-owned copy table, not fresh wording. Current Office1671–1674 is its source:
- Pay Query open on this Timesheet — resolve it in Weekly Source Office Checks.
- Pay Query evidence could not be confirmed — check the Timesheet's queries.

Do not resurrect PAY_ON_HOLD as a new capture code, nor BPAY_NEXT_SELECTION_PAY_ON_HOLD as a current decoder. A1's current correction is ON_HOLD; historical retention is a database CHECK-only compatibility branch.

## Omitted and automatically released sources

The immutable choice source is `private.bpay_next_run_choice_member`, schema1832:21–47: original run, Candidate, native member kind/id/no, Work, expected revision, selected flag and skip_code. Use actual `selected=true and skip_code is not null` for omitted accepted Work. Unticked rows are not automatic skips and must not be fabricated as failures. Accepted append reuse reads the stored skip directly (0426:111–119); it never requalifies current hold/query facts.

Current CHECK `bpay_next_run_choice_member_skip_code_check` is the complete DR10 successor in migration `11102026_1358_banking_pay_next_paye_same_week_guard.sql:32–55`, preserving A1's1048 correction. Current values are:
BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE; BPAY_NEXT_SELECTION_SNOOZED; BPAY_NEXT_SELECTION_ON_HOLD; BPAY_NEXT_SELECTION_WORK_NOT_APPROVED; BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT; BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE; SOURCE_PAY_QUERY_OPEN; SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE; PAYE_EXCLUDED_BY_SAME_WEEK_GUARD. Old PAY_ON_HOLD spelling is retained only if an already-stored old row exists. The latest constraint has been read; original1832 alone is not the current union.

The legacy append paths store bounded `skipped_json` in existing immutable `bpay_next_selection_page` receipts. Explicit WORK_PAGE retains complete `choices_request_json=p_choices`, including every skipped item's expected revision (0454:192–212). Although `skipped_json` itself contains only Work UUID/code, its same-page original choices supply the exact immutable label binding. A skipped item has no `work_choice` row: the skip branch continues before that insert (0454:198–204). Only the genuinely ID-only append receipt at0454:100–101 lacks `choices_request_json`. Do not infer an expected revision for that narrower historical path from today's pointer or revision history.

E2 automatic release is already retained in `bpay_next_worker_reprepare_request`: original_issue_code/work_id, original input job/command, Candidate/run/worker, captured and execution revisions, initiator_kind SYSTEM, action RELEASE, bounded completion status and finished time (0559 migration:36–78). The exact six values are SELECTED_WORK_NOT_APPROVED, SELECTED_WORK_REVISION_CHANGED, SELECTED_WORK_POSITION_PENDING, SOURCE_PAY_QUERY_OPEN, SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE, CASE_NO_PAYABLE_AMOUNT. 0559:2393–2397 enters cleanup; completion, rather than admission, means excluded. Current worker diagnostic is cleared during cleanup, so use the immutable completed request's original diagnostic. Never turn completed release into retryable REVIEW or count unfinished cleanup as removed. Manual removal and expiry/discard retain their own reason/outcome; do not mislabel them as E2.

## Frozen identity and current reader gaps

The accepted Work label binding is already native:
`run_choice_member.(work_id,expected_revision_id)` or `run_work.(work_id,captured_revision_id)` joins the exact immutable `work_revision` pair. Core1324:132–176 supplies candidate_display_name/reference, timesheet_reference, week_ending_date, physical_timesheet_id/version and source_kind. Core1324:505–519 gives the run-work composite FKs. For a released issue before run_work exists, use the original choice member's expected revision. A later CURRENT_WORK page is not the accepted label authority.

Current public WORKERS2259:1576–1583 returns only worker UUID/Candidate UUID/state/frozen gross/code and discard outcome. It omits issue Work/labels. Existing STATUS0639 has issue_work_id. The old private worker page2359:178–199 is available only for parent REVIEW/DRAFT and seeks Candidate UUID; it does not provide PREPARING/PENDING membership or name sorting.

Case-only Candidate identity is native, but `selection_candidate`1358:552–559 freezes channel/umbrella/floor, not Candidate display name/reference; memory LEAF0121:10–32 likewise does not freeze Candidate labels. Native Case creation/request/event/funding receipts retain exact Candidate IDs and economic operands, not Candidate display labels. Use a genuine retained original Work revision where one exists; otherwise the existing Case audit reader's exact `public.candidates.id` point join supplies qualified current Candidate display name/reference (2129:389–391). This is current identity metadata under OR46, not a financial frozen fact; no snapshot store or writer is required.

Office1707–1727 currently opens an issue by scanning CURRENT_WORK after reading STATUS. DR-15 should use the returned exact accepted physical identity directly, rechecking current actor/NEXT/frame and the same issue identity before opening the existing native Timesheet child. Office1778 currently says generic Candidate,1782 maps only SP15 and a generic could-not-prepare string;2197 still renders bare Review required. Both active native Draft paths need the shared manifest and truthful labels.

REVIEW's PREPARING response is an actual incomplete state, not success. The public2259 WORKERS path delegates to2359's restrictive reader, while Office refresh proceeds to readRows. Preserve REVIEW/Confirm gates and the PF08 attention-only branch; expose real pending/no-worker members through current Rule B run_choice_member/selection_candidate/run_command-member/job/worker associations inside the named reader, without inventing worker UUIDs or expanding a financial owner. Existing Worker ACCEPTED_CANDIDATES/ACCEPTED_WORK DTOs contain parked2153 accepted_input/effective-cut operands, and current2259's p_kind switch has no such branches. They are not a reusable current Rule B reader, and must not be activated to solve DR15.

## Small complete read/UI design

Use one finite PREPARE_ISSUES contract for immutable omission/complete-release facts plus actual current REVIEW/PENDING diagnostics, with exact source identity/discriminator and nullable frozen-label fields. Keep ordinary WORKERS/STATUS/attention contracts compatible or extend them only as needed for the named native review. Bind every response to the exact run/preparation/review context, actor permission and existing NEXT epoch; current read state changing during scroll invalidates the cursor and restarts the same read, not an automatic economic retry.

Whole-list server sorts are required by A3e/SDP34 for Candidate, Payment and Reason; Actions is exempt. Default Not in this Draft is Candidate A–Z. Candidate sort uses exact chosen label, Payment uses the genuine frozen reference/week and stable native identity, Reason uses the finite closed code/order supplied by the one manifest, never browser page-local sorting. Include a unique native source tie key after the actual requested sort operand and full retained cursor tuple. Do not use after_candidate_id alone for rows containing multiple Work/issues per Candidate. Inspection/packing must be bounded before JSON aggregation; list membership can traverse all pages by native cursor while the browser keeps only bounded page/continuation state. Existing indexes: run-choice PK and (run,candidate,kind,member_id), run-work (worker,work), worker-action original-command/worker and pending-run. Any truly required additional read index must be tied to these exact existing sources, not a new projection or counter.

Read-only source joins must not perform new diagnosis, query admission, current pricing, active hold inspection for a replacement offer or rewrite frozen states. Later post-click changes are informational only. NOT_IN_DRAFT facts never block Confirm; native existing REVIEW/Confirm guards still do.

## Supplied SCR-46 mapping and scope

Read supplied B4 SCR-46 spec. Purpose: ticked Timesheets skipped at Create and Candidates left out during preparation, one reason per line. Overlay is **Not in this Draft**, native medium frame, actor/start subtitle, Candidate / Payment / Reason / Actions, no ticks, continuous scroll, Open Timesheet only when real physical identity exists. It opens from the progress information line and Draft review and never blocks Confirm. The supplied examples map payable-week, PAYE same-week exclusion, no-payable repayment, hold, original changed selection and Pay Query. One reason per actual immutable fact; do not infer the supplied sample counts.

The parent picture also contains whole-run/PAYE/Umbrella monetary summary cards. Those belong DR16, not DR15's own reader; do not add summary money to fill the picture. Use existing server counts where actually available; a whole-list count newly required must be bounded/read-only over exact membership, not browser summed page lengths. Future totals/export/net-sheet/execute-date surfaces stay with their generated owners. No new emails, technical phases, UUID prose, raw database messages or toast. Root supplies actual screens and independent full42-policy comparison later; this investigation does not claim implementation pixels reviewed.

## Final source-boundary supplement

Viewed the supplied original SCR46 dark and phone PNGs without modifying the pack. Desktop is a native table; phone is the same record list as cards with an explicit Sort by selector. Close dismisses the child, Open Timesheet exists only for genuine Work rows, and no mutation/checkbox exists in this list. The footer sample counts require real whole-scope reader facts, not page lengths. The current spec has no reader additions.

Reason sorting must follow an explicit finite order consistent with displayed plain-English reasons; raw code alphabet alone does not prove the same order. Keep a native stable source identity as the final tie key and current run/review context in every cursor. Joined name ordering can have residual sort cost: do not claim constant indexed scan or a two-second timing result from a source-only read.

PF08 stores technical first/last error codes on the actual job (2259:128,159–161), under a finite syntactic BPAY_NEXT_* limit rather than a fixed diagnostic enum. The display must retain a generic technical-failure entry and unknown-code fallback, and must not leak raw errors or manufacture a successful pending result. Exact retry continues through the existing PF08 route; it must not invoke DR05 REPREPARE for PF08 attention.

Current source hashes: public reader2259 `1A57552C42D63D0DC885A4E7294472C03319537DA9E913A96CBBADC10C2AB879`; BE preparation `E930CCECE59E729C734725E9B050C8C9C9990A5A97DE40773F9EBB134437F475`; BE job-step `01374C96E62F6E78242F53B75A4EF13B8A624024A1763F5B10B063D1A71C8FAA`; FE Office `F876B7961AAF8D0CBCE0FCBCF981CE8F641BA74A122882C1B4B9836B87578929`. These are investigation inputs, not whole-item source receipts or release authority.

## Original skipped-page and Case-label closure

The actual retained page table is `private.bpay_next_selection_page`; repository SQL contains no `bpay_next_run_work_choice_page` object. Its native identity is `(run_id,page_no)`. The following exact read joins cover original skipped labels without adding storage or changing append behavior:

| Original source | Exact accepted label authority |
|---|---|
| Rule B accepted choice, including skipped member | `run_choice_member.(work_id,expected_revision_id)` → `work_revision.(work_id,id)`; member Candidate must match native Work Candidate. |
| Legacy explicit WORK_PAGE | Same `(run_id,page_no)` receipt's skipped Work UUID → the exactly matching `choices_request_json` item's `work_id,expected_revision_id` → the exact immutable Work/revision pair. Original explicit validation admits at most100 entries and checks the revision's Work ownership (0454:143–148,192–195); use that bounded original array, not a run-wide history scan. |
| Current accepted matching/explicit acknowledgement | 2232:221–227 verifies original typed member Work/revision/mode;2232:251–254 preserves explicit `p_body` in the same choices field, with original matching request and captured result. For ID-only matching acknowledgement, the original typed member supplies the expected revision even though this page's choices field is null. |
| Genuinely legacy ID-only append with no typed accepted member | 0454:100–101 stores only Work IDs/skips. No exact historical revision is implied. Expose the missing frozen-label provenance honestly; do not use CURRENT_WORK or arbitrary revision fallback. |

Work-choice rows cannot recover skipped explicit members because they are only inserted after the skip `CONTINUE` (0454:198–204). Page replay compares original raw choices, and accepted acknowledgement compares the original explicit body and matching request before returning retained results (2232:200–211). The existing1621 choices-array CHECK binds item count and≤32768 bytes;2232 adds bounded matching/captured receipts, including≤120000-byte captured result. These are retained request receipts, not a new proof mechanism.

Case creation request0320:7–48 contains Candidate ID, native rule/component/command/actor IDs and approved operands; current create context1536:110–118 contains epoch, financial revision, channel, floor and valuation policy. Neither contains Candidate name/reference. Core1324:355–387 and typed event0300:496–545 likewise contain identity, events and economic links; creation1536:361–362 and funding0410:430–437 insert those exact facts without display labels. A beneficiary/bank name is not Candidate display-name authority. For a Case-only omitted row, join its exact native Candidate ID to the same current name/reference expression already used by the audit reader2129:389–391, retain explicit current-label provenance and allow nullable labels. Do not invent an original name or add capture writes.

Follow-up inputs:0454 SHA256 `D40CC95CB6675F09B1FE06A87801E42A6A28CA1F02A563534AE5E1A65263B5CB`;2232 SHA256 `CC1491F55FF1F4AFE48F82E8892015AA2F91500F42D0D41D692F0E6B78DD3E48`;1536 SHA256 `853DDCE45455DEFED9F715A2593267794F3B224E469A9EACCDE2AECA6E44A470`. Source-only investigation; all future joined/runtime checks remain NOT_RUN.

