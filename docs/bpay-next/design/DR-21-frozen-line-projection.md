# DR-21 frozen line projection seams

**Read-only supporting findings, 12 October 2026.** The card explicitly requires Client/week ending/type/unit/units/rate/subtotal and frozen finance in each Candidate expansion. Current public LINES has identity/type/date/consumed/frozen money only; its missing display fields are real existing captured facts, not permission to read current Client/Contract or manufacture paid quantity. No product/database/test/pack/hosted action was performed.

## Smallest additive projection

Keep the original `private.bpay_next_simple_review_line_page_v1(worker,after_line_no,limit)` call and original page/order/limits/authority for its currently admitted scope. In the named public preparation successor's LINES JSON projection only, join the returned page row `l` to:

1. `private.bpay_next_run_line rl` by `rl.id=l.run_line_id`, `rl.run_worker_id=request.run_worker_id`, `rl.captured_revision_id=l.captured_revision_id`, `rl.component_key=l.component_key`.
2. `private.bpay_next_approved_line a` by `a.id=rl.approved_line_id AND a.revision_id=rl.captured_revision_id AND a.component_key=rl.component_key`.
3. `private.bpay_next_work_revision r` by `r.id=rl.captured_revision_id AND r.work_id=rl.work_id`. This is the exact immutable captured revision; never `work.current_revision_id`.

The native run_line FK tuple enforces run_work/worker/Work/captured revision and revision/approved line/key(core1324:586–589). Joins are indexed PK/unique point reads after the already bounded native page, so they cannot fan out or turn pagination into a full history scan.

| Required fact | Exact source |
| --- | --- |
| Client | `r.client_display_name`, core1324:160. Same captured Client display is returned by existing origin helper0620:77; no extra live Client read or new ID is necessary. |
| Week ending | `r.week_ending_date`, core1324:151. Do not derive it from today's date or work_date. |
| Type | Existing `l.component_kind` / exact approved line194; retain the closed actual kind. |
| Unit | `a.unit_label`, core1324:196. Nullable, no guessed hours/miles. |
| Units | `a.approved_quantity::text`, core1324:197. This is **approved quantity of the immutable line**, not allocated/paid quantity of the current Draft. |
| Rate | `a.approved_unit_rate::text`, core1324:198. This is captured approved unit pay rate, possibly NULL; no live Contract rate or blended/derived rate. |
| Subtotal | Existing `l.frozen_ex_vat` (or the explicitly named existing target frozen field), representing actual Draft frozen line money; no quantity × rate recomputation. |
| Frozen finance | Worksheet's exact completed DRAFT/current accepted NET instruction/result facts, separately at Candidate scope; never invent per-Timesheet net or apportion recovery across lines. |

Existing RATE_DETAILS0620:168–178 already proves the same approved unit/quantity/rate lineage through its frozen-line guard. It does not supply week ending. Adding these fields to LINES avoids one detail RPC per displayed line and leaves all existing frozen-detail views/functions unchanged. Current native LINES is public1019:771–777; exact original line reader2359:256–299. Update only the associated closed Worker/Office LINES field schemas/rendering; preserve old fields and native decimal text precision/NULL. Do not cast quantity/rate to money or silently restrict them to six decimals.

## Truthful display requirement

Use explicit **Approved units** / **Approved unit rate**, or an equally clear captured-approval caption over those fields. A partially consumed approved line can have a frozen Draft subtotal different from its full approved quantity × approved rate. Do not call those operands paid units or imply their product equals the Draft subtotal. Unit/quantity/rate missing in native capture remain em dashes. A WORK line can contain several original rate buckets and have NULL single rate; keep NULL and existing bucket detail rather than invent an average.

## Existing guard boundary to resolve explicitly

The original2359 line reader admits parent REVIEW/DRAFT and worker READY only(:282–285). Existing frozen-detail guard0025:29–34 has the same scope. Therefore a NET-refused worker now REVIEW cannot use that existing expansion even though the worksheet retains its accepted money; EXECUTING/COMPLETE/CANCELLED parent addresses admitted by channel directory0236 also cannot call it.

Do **not** broaden those shared guards silently. The minimum field projection above solves the normal READY/DRAFT case only. If the current card's all-worker frozen expansion is required for those other states, root must name a narrow read-only worksheet-qualified supplement in the owning preparation reader: exact confirmed run/PAYE worker membership + original captured line/revision identity + truthful completed financial state, preserving original LINES callers/financial guards and all frozen-detail owners. Otherwise show a truthful unavailable expansion and record the exact unresolved acceptance; do not claim every worksheet worker can expand via2359.

**Acceptance:** source/field authority only. New product projection, installed schema, real refused-state detail, browser expansion and timing are NOT_RUN.

