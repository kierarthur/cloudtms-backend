# DR-01 current scope investigation

Read-only source investigation, 10 October 2026. No SQL, product code, tests, fixtures, runtime, provider or hosted actions were executed. This note maps the exact card/checklist; it does not certify installed or app behaviour.

## Binding scope and deliverables

Authority: plan `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:39–66`; all DR-01 checklist rows (ITEM and D01–D11); master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:57,141–146`; `addendum/R2_EXACT_CONNECTIONS.md` §1/§4; owner start sheet; BUILD_LOG BD019 owner rule B. The historical accepted-input resolver wording is superseded by BD019. Retain expected revisions, selection Candidate capture, original Source query snapshot and `pin_v2`, including original postclick REVIEW and live Case balances. No parked F25, producer, enroller, PIN or new command protocol is needed.

| Checklist objects | Required finite work |
| --- | --- |
| D01/D09 create preparation | Derive genuine official date on the server; optional supplied date must agree; retain original run identity/replay and expiry. |
| D02/D10 ID selection append | Skip future-week works individually and retain per-page result. |
| D03/D11 work-choice append | Same skip, preserving exact expected revision, ALL/SUBSET and WB36 segment ownership. |
| D04 named pay-date repeatable | Copy current controlling bodies only, with the above deltas and narrow intake/receipt support. |
| D05/D06/D07 existing preparation transport | Optional CREATE date; official date receipt; closed skipped array; named definite refusal mapping. Keep authentication, bounds, exact retries and enrollment wake. |
| D08/ITEM | Record source evidence and future joined acceptance honestly. Screen replacement remains WB11; skipped presentation belongs WB21. |

Selection owners are ordered WB36 → DR01 → WB40. The 1600 PIN chain is F05 → F25 → WB36 → WB40; DR01 is not its next writer. Capture/hold later editor chains also remain intact. R2 still requires WORK:DAY parent identity, genuine included/excluded segment amounts and candidate grouping across both channel Drafts; this item adds no money calculation.

## Actual controlling sources

| Signature | Current physical/root source |
| --- | --- |
| `private.bpay_next_create_preparation_v1(uuid,uuid,date)` | `03102026_1590_banking_pay_next_selection_manifest_v1.sql:7` |
| `private.bpay_next_append_selection_page_v1(uuid,bigint,jsonb)` | same `1590:48` |
| `private.bpay_next_append_work_choices_v1(uuid,bigint,jsonb)` | `04102026_0570_banking_pay_next_component_selection_v1.sql:112` |
| `private.bpay_next_record_all_work_choice_v1(uuid,uuid)` | same `0570:85` |
| `private.bpay_next_preparation_args_v1(jsonb,text[])` | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:1109` |
| `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)` | same `1236:1165` |
| `private.bpay_next_work_choice_guard_v1()` | same `1236:1023` |
| `private.bpay_next_seal_selection_v1(uuid,bigint,bigint)` | `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql:6` |
| `private.bpay_next_claim_prepare_job_v1(uuid,integer)` / `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | same Rule B `0151`, PIN at181 |
| `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | segment final `1236:2984` |
| `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `10102026_1438_banking_pay_next_hold_workbench_projection_v1.sql` |

The current registry confirms these final choices. Use the full current 1236 intake/argument bodies, not the older 0575 or 2018 copies: their SEGMENT_PAGE/SEAL and Case actions must survive. Replacing only the three card-named money-independent owners plus those genuine closed-contract support seams is sufficient; do not replace the guard/claim/PIN/capture/hold merely because their predecessors appear in the card.

## Confirmed gaps and minimal seams

**CREATE.** 1590 rejects a NULL date and inserts the caller's date directly. There is no official-date call. It validates NEXT ownership/active actor, inserts run ID once and checks exact actor/date on locked replay; preserve these checks and the existing expiry. Fresh CREATE should call the existing date helper with server time and use `current_official_pay_date`. A supplied unequal date raises exactly SQLSTATE22023 `BPAY_NEXT_PAY_DATE_NOT_OFFICIAL`. Preserve exact retained-run replay when the server day/settings have subsequently changed: do not recalculate a retry into another pay date, change expiry, or reveal another actor's run. A fresh conflict/race still needs the original locked identity revalidation. Return the stored authoritative date as `official_pay_date`.

**Intake.** Current 1236 `v_keys` requires CREATE `run_id,pay_date`; argument validation requires exact key count and a nonnull string. Support precisely the two closed CREATE shapes (run_id alone or run_id plus a valid supplied date). Preserve all other action key sets, bounds, canonical date checks and authorization before dispatch. No new route or public function signature is required.

**Selection cutoff.** Both append functions currently accept all valid works; choice append can reject the whole page for approval/current/applied mismatch, which is an existing separate invariant. Add the named future-week branch before membership/choice/Candidate writes, without converting unrelated malformed input or identity failures into skips. Existing WORK and revision contain `week_ending_date`; Workbench1410 validates Sunday and compares to the server London week's Sunday (`v_today + 7 - isodow(v_today)`), equivalent to work-week Monday `week_ending_date-6` after current London Monday. Reuse this native week identity; do not compare against the official pay-date helper's `current_pay_week_start` (it is official date minus6 and may not be Monday). Capture1610, current1236 capture and current0151 PIN do **not** contain this future-week admission check. Do not claim existing enforcement as a passed reuse check and do not add delayed PIN freshness to compensate. Make the fresh append decision once and retain it; no replay-time week or snooze refresh. If the build binds the week to the original CREATE day, the existing immutable `pay_run.created_at_utc` supplies that day without a new cutoff object.

Only whole-work skip codes are allowed: this item FUTURE_WEEK (`BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE`), existing WB36 whole-Timesheet snooze (`BPAY_NEXT_SELECTION_SNOOZED`), and the later WB40 hold (`BPAY_NEXT_SELECTION_ON_HOLD`). Segment snooze must keep the work and use actual selected-segment SNOOZED state. Do not implement WB40's unbuilt hold owner ahead of its card. If implementing whole snooze now, use genuine current WB36 basis-qualified WHOLE_TIMESHEET authority, not any arbitrary active segment/expense row or an unqualified stale snooze.

**Durable page replay.** Core1324:439–449 stores original ID array, first selection number and item count only. Choice replay currently reconstructs expected revision/mode from each inserted choice, so skipped tuples have no matching choice row. A bounded additive receipt field on this existing private page is genuine support: retain original request choices (including skipped expected revisions/modes) and the exact skipped list, or an equivalently exact finite existing-page representation. ID-only replay must compare its original ID array; choice replay must compare every original tuple, including skipped tuples. Do not reclassify after midnight/approval/snooze changes. Keep page numbering contiguous and selection ordinals/counts based only on actually appended rows; increment Candidate membership only for actual accepted work.

Smallest compatible count choice: retain existing `item_count` as the original attempted count (1..100), with `skipped` count explaining the difference, and increment `selection_count` by attempted minus skipped. This preserves current input-count validation and allows all-skipped page receipts without weakening the original page table's 1..100 check. If instead item_count is defined as appended count, schema/decoder checks must genuinely allow zero and every existing caller must be updated; the card does not require that wider semantic change. In either choice, page skip evidence must be bounded, closed, unique and a subset of its original request.

**Empty selected set.** Current0151 seal rejects `(expected_pages=0) != (expected_count=0)` and permits true zero-WORK Case-only selection only through its original Case seal checks. An all-skipped appended page can therefore be received successfully but cannot then be treated as an executable empty Draft by the unchanged seal. Record this honestly in joined acceptance/WB21 sequencing; do not fabricate WORK, silently enroll an empty run, or broaden PIN/enrollment. A Case-only plus all-skipped-page flow would need a specifically justified finite seal support correction, not an implicit claim that the existing zero-page Case-only path handles it.

## Worker and existing caller support

`broker/src/bpay-next-preparation.js:12,47–48,249–267,388–410` currently requires CREATE pay_date, omits official date/skipped fields, uses a positive WORK_PAGE item_count and exact input-array count equality, and translates every22023 to generic REQUEST_INVALID. Keep the8s/128KiB response and32KiB request envelopes, full closed parse, actor/admin guards, fixed mutation retry bytes and existing finite SEAL refusal parser. Add only optional CREATE date handling, `official_pay_date` canonical TS_DAY output, skipped-array validation (native requested work IDs, unique, closed whole-work code), and the exact date refusal before generic22023 translation. This is a definite rollback/refusal, never an unknown mutation or automatic retry.

The exact receipt mirror in FE `js/bpay-next-office.js:28–41` must accept the new fields so the current strict decoder does not reject genuine receipts. The editable input at603 is expressly replaced by WB11, not DR01; no new Workbench screen, generic API, background retry or ACCEPTED_* invocation is authorized here. WB21 later displays skips and derives the actual selected count for SEAL rather than blindly retaining the original attempted selection count.

## Official-date discrepancy and later ownership

Actual latest helper is `public.pay_banking_official_date_context_v1(timestamptz)` in `26052026_2100HRS_NEW_FUNCTIONS.sql:185280–185379`: STABLE, definer, service-only. It reads settings_defaults.id1 weekday0..6, derives the first configured weekday on/after the server London date, returns current/next equal and following+7. Its configuration errors already fail closed. It has **no bank-holiday feed or next-working-day adjustment**. DR01 explicitly names reuse/derivation from this helper; it does not specify a holiday policy or replace that public owner. WB11 at02:472 claims the existing helper already applies the feed and at492 requires a next-working official date before a holiday. That is a real WB11 acceptance gap, not evidence that the helper currently meets it. Narrow DR01 should consume the real helper consistently and document this later named gap; do not invent a holiday calendar/adjustment or modify the shared legacy date owner under DR01 without a concrete scope correction. No caller-supplied as_of time becomes authority.

## Source receipt

SHA256 of reviewed current sources:

| Source | SHA256 |
| --- | --- |
|1590 selection manifest|`47a0df6a06bd7816b0102c1aa5fca0bddd97ebc08a0a04d786254db3179967b3`|
|0570 component selection|`d19da6893ce30575e8e186d8ce56b82b76e9fb61160f0f560430c682c9955f46`|
|0151 Rule B consumers|`2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`|
|1236 final segment authority|`f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`|
|Worker preparation|`adef1957995903841072a7731bbae8131334330feafb09da835628ba52b01da9`|
|FE Office|`a1b901e812309bed55f368e23a30c213f2ec82f35590459d971aee7bbebdeacd`|
|final-definer registry|`dcdab6c14bca465a9756342c845d5cad4e5506599aaab51ba160edcdff5a2057`|

Future joined checks must cover wrong/omitted date,10works/1future, all-skipped page, duplicate/lost reply across London rollover, unchanged body vs changed skipped choice tuple, current/older week and whole/segment snooze distinction. These are future acceptance statements; none were executed by this investigation.
