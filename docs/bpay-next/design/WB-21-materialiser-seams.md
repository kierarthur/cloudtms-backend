# WB-21 accepted membership materialiser seams

Status: read-only native source investigation, 10 October 2026. Documentation only; no SQL, tests, runtime, business calls or product edits. This is a recommendation for the builder, not an installed-source verdict. Rule B remains the existing expected revision, captured selection Candidate fields, raw query snapshot and existing query pin. No parked F-25 resolver, producer/version protocol, financial snapshot or new command kind is proposed.

## Recommended capture boundary

The least competing authority is to establish genuine native `run_selection`, `work_choice` and `selection_candidate` at accepted CREATE, and insert the original `run_work_query_snapshot` there in the same transaction. Subsequent bounded pages acknowledge/materialise only already accepted native membership and retained component/Case choices.

This is possible under the actual guards. The earlier `WB-21-create-order-lifecycle.md` blanket recommendation against prepopulating native rows is too strong and is superseded **only for this storage choice** by this source finding. Its separate PREPARE/lifecycle analysis remains outside this note. Prepopulation does require explicit upload coverage; merely leaving the old append counters unchanged is incorrect.

Core1324:439–470 gives `run_selection` a real Work/Candidate FK and unique positive ordinal, and `selection_candidate` a real Candidate FK and unique positive member ordinal. Neither schema requires an existing numbered Work upload page. The inspected repeatable trigger census finds no row INSERT guard on these two tables or on `selection_page`. The actual 1236 work-choice INSERT guard requires PREPARING/OPEN and the original initial choice shape, not a page or today's Work pointer:

```sql
-- 1236:1037–1041 and 1077–1081
select r.* into strict v_run from private.bpay_next_pay_run r
  where r.id=new.run_id for update;
if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
  raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
end if;
-- INSERT into work_choice:
if new.page_count<>0 or new.component_count<>0
  or (new.selection_mode='SUBSET' and new.selection_state<>'OPEN') then ...
end if;
```

The 1241 schema also requires the real `(run_id,work_id)` run-selection FK; ALL is SEALED with component/page counts0, and SUBSET begins OPEN with counts0. Therefore insert accepted Work rows first, then their exact accepted expected revisions/modes. Do not call the old ALL helper to choose a newer current revision. Let existing 1236 segment boundary/sealed-count triggers run normally: they copy the run's snooze control cut and automatically count a no-segment choice. Do not overwrite or double-increment `segment_sealed_choice_count`.

Insert one native Candidate row for each accepted Work Candidate or Candidate with actually selected Case components, with the four original captured columns populated from the accepted CREATE read. Do not create Candidate membership for excluded-only Case choices. Preserve actual NULL values; NULL captured fields are not a reliable "not yet captured" marker. One atomic accepted-descriptor branch/receipt is the capture-complete marker.

### Query snapshot FK and capture

0151 migration:9–18 requires:

```sql
foreign key(run_id,work_id)
  references private.bpay_next_run_selection(run_id,work_id) on delete restrict,
foreign key(work_id,expected_revision_id)
  references private.bpay_next_work_revision(work_id,id) on delete restrict
```

Thus create the native Work row before its original raw query snapshot. Derive physical Timesheet from the **accepted revision**, exactly as the existing 0151 seal does; do not accept a caller-supplied physical ID or read a newer physical pointer. For accepted SOURCE/PROTECTED Work, execute the existing actual gate once during accepted capture, after existing Source admission and before the accepted command's final clock tail, and store its raw result in `bpay_next_run_work_query_snapshot`. Retained E1 query-open/unavailable skip rows need only the exact original Work identity and finite skip code; they are not selected Work and should not acquire a financial query snapshot.

A temporary raw gate field in a new choice-member relation avoids the FK initially but creates a second retained query authority until transfer. It is unnecessary when native prepopulation is legal. No new economic JSON or alternative query-pin consumer is needed.

## Exact named owner seams

| Current function / source | Existing current-input read | Minimal accepted branch |
|---|---|---|
| `append_selection_page_v1(uuid,bigint,jsonb)`,1621:70–196 | Work136; current revision snooze146–163; ALL helper175; new native membership170–183 | Keep pure validation/run lock/exact existing-page replay first. Each fresh input must match the retained accepted Work-or-skip manifest and stable page order. Acknowledge the existing native Work/choice; return stored skips; do not read approval/current/applied/week/hold/query again or call record_all. |
| `append_work_choices_v1(uuid,bigint,jsonb)`,1621:200–324 | Work272; requested revision/current/applied281–285; snooze292–300; native inserts306–314 | Preserve the exact three-key tuple, mode/UUID checks and original request replay. Match exact accepted expected revision/mode. No late Work-state requalification. Accepted branches leave native Work/Candidate/choice totals unchanged and advance only actual upload counters/page receipt. |
| `record_all_work_choice_v1(uuid,uuid)`,0570:85–109 | Chooses `w.current_revision_id` only for a new choice | Bypass for accepted native choices. Its exact existing ALL replay is safe, but the new-choice path is not an accepted revision materialiser. Preserve legacy callers outside the accepted branch. |
| `append_component_selection_page_v1(uuid,uuid,uuid,bigint,uuid[])`,0570:201–260 | Work240–242 current/applied check | Keep exact choice/revision/mode and exact page replay. Replace only fresh accepted branch's current-pointer requalification with membership in the accepted SUBSET line manifest. Keep immutable approved-line/revision/component-key lookup and native page/child guard/counter updates. No general negative-hours/amount ban. |
| `seal_component_selection_v1(uuid,uuid,uuid,bigint,bigint)`,0570:264–295 | No current Work lookup | Preserve native count checks/replay/SEALED transition. Add exact accepted SUBSET coverage only if original page counters alone cannot prove all accepted line IDs were acknowledged. No recapture. |
| `append_case_selection_page_v1(uuid,uuid,bigint,bigint,uuid,uuid[],boolean[])`,0330:268–346 | Component lookup335–336 is identity only | Fresh inputs must exactly match retained native component IDs and selected booleans for this Candidate/selection revision. Keep original page/header/item guards, request UUID, ordinals and counters. The immutable component-to-Case/Candidate join remains legitimate identity validation. No live Case status/rules/balance enumeration/filter. |
| `seal_case_selection_v1(uuid,uuid,bigint,bigint,bigint)`,0330:349–389 | No live Case requalification | Preserve header counts, exact replay and first selected-Candidate sealed counter. Require complete accepted selected/excluded component coverage before success. Native Case balances remain live under their original later money locks. |
| `work_choice_guard_v1()`,1236:1023–1103 | No current Work pointer | Reuse original complete body; no general guard waiver or trigger rewrite is needed for prepopulated choice headers. Actual component pages/children still insert through the original page guards. |
| `seal_selection_v1(uuid,bigint,bigint)`,0151:6–76 | Fresh seal updates Candidate settings50–55 and calls raw gate66–68 | For the exact accepted branch, validate upload/choice/segment/Case completeness and existing captured rows, then perform original SEALED/review transition. Do not update captured fields or re-insert/re-evaluate the raw query snapshot. Exact SEALED replay stays first. Preserve old capture behavior for a genuinely supported legacy preparation branch. |

The exact existing replay boundary must remain before any accepted lookup that could be reclassified. An old stored successful page returns its original `skipped_json`; request mismatch still refuses. A foreign Work, revision, line, Case/component, changed mode/boolean or altered same-page request is an identity error, not a business skip.

## Upload/count consequences that must be explicit

Prepopulated `selection_count`, `selected_candidate_count`, `work_choice_count` and sealed ALL count are **accepted native totals**, not upload progress. The old1621 append code must not increment them again.

Use one explicit monotone uploaded-accepted-Work count (or an equally exact per-member acknowledgement) and the existing numbered immutable page receipts. Validate each fresh page against the exact next retained stable ordinal range, so pages cannot overlap, skip a retained member, acknowledge it twice, or add a later member. Stored attempted/skip order must also be covered exactly; skipped-only pages advance the original page count but add no selected Work. An accepted total alone cannot distinguish "CREATE retained all Work" from "all required pages acknowledged".

The original `selection_page.first_selection_no` cannot keep using `v_run.selection_count+1` once selection_count is the full accepted total. On the accepted branch it must use the next acknowledged accepted ordinal, while preserving attempted `item_count` and original `work_ids_json`/`choices_request_json`/`skipped_json`. Existing page schema allows this: first_selection_no is positive, and attempted item_count remains1–100. An all-skipped page still needs its real receipt even when selected delta0.

Seal must require uploaded accepted Work count = native selection_count, complete retained skip/attempted coverage, all native component choices SEALED, all segment choices sealed by their original owner, and exact Case page/selection coverage. It must not infer uploads from prepopulated membership. Pure count assertions without no-duplicate/order coverage are insufficient.

0151:14–16 presently rejects pages>0/count0 through `(p_expected_pages=0) IS DISTINCT FROM (p_expected_count=0)`. A real skipped-only Work upload plus a genuinely selected/sealed Case-only Candidate requires that small named seal input relaxation: nonnegative counts/pages, with count>0 requiring pages>0, and the **existing** count0 Case-only proof retained. Empty/all-skipped with no selected Case is still not a fake successful Draft. This is not permission to change enroller/PIN money semantics.

Case prepopulation needs the existing append's Candidate insert/counter seam adjusted: a pre-existing accepted Candidate must not be incremented again. For all-excluded Case-only Candidate, retain the original exclusion-only refusal unless actual accepted Work/selected Case membership already establishes that Candidate. Do not precreate case-selection headers with nonzero counters or direct SEALED state;0310 forbids those and checks each real ≤100 page.

## Retained E1 evidence and scope

Store exact original requested Work IDs, selected native revision/mode and per-Timesheet finite skip code at acceptance; uploads return that original receipt rather than evaluating a changed live condition. Work membership contains no money. Component SUBSET acceptance retains the genuine immutable approved line IDs, and segment selection retains actual approved stable keys/complete signed membership through its existing WB36 owner. No generic sign predicate, reconstructed caller amount or inferred RECOVERY selection is permitted.

Acceptance is the place for actual approval/current-applied, native pay-hold, run snooze cut, future week and Source query eligibility. The detailed native hold join and signed component authority are already recorded in `WB-21-eligibility-seams.md`. This note does not add a public hold trigger or Case financial freeze. Later changed-revision behavior remains the original Rule B REVIEW/PIN contract until its named later owner changes it.

## Exact source bytes read

| Source | SHA256 |
|---|---|
| 1621 preparation pay date | e8415355b5a11f2e75e0bc532f5610b920d542799b89725bba25493d1f1af641 |
| 0570 component selection | d19da6893ce30575e8e186d8ce56b82b76e9fb61160f0f560430c682c9955f46 |
| 0330 Case creation selection | 9dc328ef5fb4e172622f72a97ce3cb84184740c5fe2575eb67f8bf1dac5fe903 |
| 1236 segment final authority | f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d |
| 0151 Rule B consumers | 2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf |
| 0151 Rule B snapshot migration | 7e270d8bdeed7f2c5f52432b13273a107d2b91ca3f3d8ee6dbc2e1adcd0c9645 |
| Core1324 schema | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| 0310 Case guards | 7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe |

The in-progress1832 membership schema was read only for context; this note does not approve that draft or assert implementation completeness.
