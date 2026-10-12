# DR-27 current build investigation

12 October 2026. Source investigation only; no product changes, SQL invocation, business data, fixtures, tests or hosted actions. DR-27 is approved sequence 128, after DR-21. All app, queue and performance acceptance remains NOT_RUN. The owning card is plan `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:744–769`; all ITEM/D01–D09 working checklist rows were read. `review/R3.md:244–250` supplies the binding correction. DR-28 UI is sequence 155; its handoff is not an early screen build.

## Required scope and authority

The card owns an agency-scoped, read-only list/count of **confirmed Run/channel addresses**, not preparations or one Run's workers. All statuses default, newest first, with bounded keyset `(created_at_utc DESC, run_id, pay_channel)`, limit 1–100, server filters, exact matching count, authoritative full batch identity, status label/note, and finite generation/anchor that refuses changed traversal rather than silently omitting or duplicating rows. Confirmed CANCELLED/COMPLETE history remains readable regardless of preparation readiness. Frozen financial/membership facts and current operational status are distinct (`03:748–756`, R3:246).

Named delivery: migration/repeatable `banking_pay_next_channel_draft_list[_v1]`; the **same** Run/channel summary owner as DR-16; typed `private.bpay_next_channel_draft_summary_apply_v1` using exact native OLD/NEW contributions; page kinds `CHANNEL_DRAFT_LIST` and `CHANNEL_DRAFT_COUNT`; one Confirm successor; existing preparation Worker closed mappings and contract documentation (`03:762–765`). D01 is preservation of existing worker STATUS, not a new global list through that function. D08 is the DR-28 handoff.

Owner G1 explicitly adds **EX-03** to the card's future summary/revision-writer census. EX-03's distinct-actor approvals/Golden Key/withdrawal remain sequence 180, governed by `04_EXECUTION_AND_OUTCOMES.md:187–211`; its future state transition must update this same summary and `change_revision` exactly once. Do not implement approval behavior at DR-27.

## Current owners and reusable data

| Existing owner | Current registry physical definer and source evidence |
|---|---|
| `private.bpay_next_run_channel_contribution_v1(private.bpay_next_run_worker)` | `supabase/repeatable/12102026_0442_banking_pay_next_run_channel_totals_trigger_v1.sql:4–40` |
| `private.bpay_next_run_channel_totals_trigger_v1()` | Same file:44–98; Run INSERT initializes two rows; exact Worker OLD/NEW subtraction/addition, fixed channel lock order and no reverse business locks |
| `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` | `supabase/repeatable/12102026_0645_banking_pay_next_confirm_refresh_v1.sql:5–112` |
| `private.bpay_next_channel_drafts_page_v1(uuid)` | Same 0645 file:114–149 |
| `private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)` | `supabase/repeatable/12102026_0442_banking_pay_next_run_review_totals_v1.sql:200–275` |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | `supabase/repeatable/12102026_0718_banking_pay_next_active_preparations_v1.sql`; re-read registry immediately before authoring because DR-21 is in progress |

DR-16's physical table is **`private.bpay_next_run_channel_totals`**, migration `12102026_0442_banking_pay_next_run_channel_totals.sql:8–30`, PK `(run_id,pay_channel)`. It already stores Candidate/READY counts, gross ex/VAT/inc, gross deductions, NET recoveries, known cash, missing/applicable NET counts, replacement pending/refusal, `totals_revision`, update time. It has no list visibility/status/creation/confirmation/reference/route/timing/change-generation fields. It is owner-only with RLS; no browser/service table grants. The card's proposed `bpay_next_run_channel` must resolve to this **single physical owner**. Settle/document its physical naming rather than creating a parallel totals table. DR-16's own card `03:440` and BD134 require this sharing.

The contribution reads exact captured DRAFT allocation state by Worker captured economic revision/selection and current accepted NET projection/request points. It is not an allocator. It includes accepted money through pointer retirement, but deliberately excludes monetary contributions when Worker status becomes CANCELLED (`0442 trigger:20–39`). Therefore its **current aggregate alone is not immutable confirmed gross/membership history**. Retain a separate explicitly frozen Confirm-time snapshot on the same rows where the list requires original Draft figures; keep live operational/cash fields labeled separately. Do not turn cancellation's current zero contribution into a historical £0 batch or remove its address.

Confirm locks module then Run, validates exact review/SEALED counts, original command/finished jobs and complete typed pre-confirm RELEASE exclusions, checks the real 72-hour deadline after the Run lock, and changes Run to DRAFT with `confirmed_at_utc` (`0645:21–75,105–110`). Completed exact Confirm replay exits before any new mutation (`29–33`). The new same-row list activation/frozen snapshot must occur once under these original guards; no live Work/adoption/bank freshness or repricing. Existing CHANNEL_DRAFTS is at most two exact addresses and already preserves later-cancelled history by excluding **only exact completed pre-confirm RELEASE** (`130–147`). Reuse that membership rule, not current `worker.status<>'CANCELLED'`.

Worker STATUS already shows frozen Worker gross, accepted projection/input/cash, pending NET, reservation state and bounded transfer history (`0442 reader:217–271`). It has no workflow-readiness requirement, only NEXT ownership and exact Run/Worker scope. Preserve its accepted-versus-pending distinction and original transfer's own projection. Existing `CHANNEL_STATUS` validates captured channel in Worker (`broker/src/bpay-next-preparation.js:809–811`). No new status reader or money formula is needed.

## Exact remaining implementation

1. **Extend the one summary table.** Add typed operational status/count contributions, confirmation/list visibility and authoritative identity, immutable original Draft figures where required, native `change_revision`, and finite list/count generation support. Existing `totals_revision` is per-row and by itself cannot detect a new row or status-filter membership moving across a cursor. Newest/status indexes must cover the actual tuple and predicate. Preserve integer counts and numeric pennies as decimal strings in transport.
2. **One shared typed updater.** Preserve DR-16's exact OLD/NEW native money contribution and channel lock ordering. Expose the card-named summary helper; make original Worker trigger and current Confirm transition use that one owner. Status-only changes must also advance native change metadata even if the monetary contribution is unchanged. A transaction that updates several native rows must not double-add contributions via both a trigger and an explicit writer call. No caller-supplied opaque `p_delta_jsonb` economic authority.
3. **Read-only list and exact count.** Closed agency/actor/module/filter/sort/context args; no mutation/wake. Seek bounded rows, include truthful operational labels, and recheck native generation before returning. Count must use the exact same scope/filter/generation; do not substitute selected Candidates, preparations or matching rows on the current page. Return explicit stale/refreshed context on changes; page-number navigation cannot require browser loading every predecessor. A maintained same-owner status count or bounded native count continuation can satisfy exact total; an unbounded financial history recount cannot.
4. **Confirm/supporting membership facts.** Activate only channels that actually existed at successful Confirm, even if their current live count later reaches zero. Freeze exact native number of Timesheets, not `captured_line_count`: core `03102026_1324_banking_pay_next_core.sql:486` is line count, while `run_work` has one captured Work/revision per Worker/Work (`503–519`, index `1031–1032`). A Work is not automatically one physical Timesheet across every Source kind; retain truthful native origin/count meaning. Any new count contribution must be derived from existing bounded capture/native membership and maintained once, not recomputed by scanning all Workers on a list call.
5. **Public/Worker contracts only.** Add two finite kinds to existing public preparation page and `broker/src/bpay-next-preparation.js`. Use existing genuine read-only early-branch permission pattern: service JWT, current active admin and NEXT owner (`0718:427–441`), exact physical agency identity, then a pure private reader. This avoids preparation-authority gates on paid/cancelled history. Keep exact **GET `/api/banking/pay/next/preparation-page`**, current index:199022 and `bpay-next-legacy-fence.js:175`, under F-10:669–688. Keep legacy `GET /api/banking/pay/batches` retired; its retained index branch is at `broker/src/index.js:199603–199605` and is not a reusable NEXT fallback.

## Status, timing, reference and future writer handoff

Current native Run statuses are PREPARING/REVIEW/DRAFT/EXECUTING/COMPLETE/CANCELLED (core:418–433, later CANCELLING support); Worker statuses and transfer outcomes are more detailed (core:470–500,651–675). Derive a settled NEXT vocabulary from those actual facts. Never map today's Run DRAFT to legacy Waiting approval without an actual captured execution approval request. Unknown/unsupported carried facts remain visible with an explanatory note.

Original Run has `pay_date`, creation and confirmation timestamps; transfer has immutable native cash, execution kind, status and creation (`core:418–433,651–675`). Pay date is not evidence of a scheduled bank execution. Full Run/channel address is already authoritative; do not present its eight-character abbreviation as batch identity. Execution route, bank reference and timing belong to EX-09/EX-10/EX-17 (`03:765`) and may be NULL/not produced until those writers exist. Do not synthesize reference or current Candidate payment channel. OR-46 preserves captured channel/creditor despite later Candidate changes (`addendum/R2_EXACT_CONNECTIONS.md:48–54`; FIN-06:69–73).

Required future exactly-once writer matrix: DR-18 Confirm; DR-26 PAYE NET; EX-02 execution request; **EX-03 approval/withdrawal (G1)**; EX-09 submission; EX-10 provider outcome; EX-14 CSV/external issue; EX-22 cash evidence; EX-23 whole-payment refusal; EX-27 cancellation; EX-31 retry (`03:757`). DR-32 watches this native `change_revision`; do not build DR-32 refresh or any future money/approval producer now. Record precise future ownership obligations in the contract, then extend those owners at their approved item boundaries.

## Delivery and unresolved design decisions

D01 preserve STATUS above; D02/D03 missing list-support migration/owners; D04/D05 missing closed list/count manifests; D06 retained preparation transport context; D07 two Worker mappings; D08 handoff to DR-28; D09 full card context. Dependencies DR-18/F-10 provide current Confirm/address/transport seams. No new command, queue kind, money protocol, execution rail, approval owner, payment provider or public table/trigger is needed by this card.

Before product authoring, settle: one physical table name; exact current status vocabulary; original-versus-current amount/count meanings; generation/count/page-number behavior; real native Timesheet count; nullable future route/reference facts. DR-28 additionally owns global visible-header sorting (`03:771–784`); document the finite server sort contract needed for that future screen without early building its renderer or unsupported picture fields. Historical missing DR-16 summary rows remain explicit unavailable; no invented zero backfill.

## Saved source checkpoint

Registry re-read at investigation completion still names the owners above. SHA-256 values are saved-byte provenance, not installed-catalogue or business acceptance:

| Source | SHA-256 |
|---|---|
| DR16 totals migration 0442 | `12CBAD8F671FE84491674633D5D226734282D5052F9859EDE86A6DBBF2329971` |
| DR16 contribution/trigger 0442 | `ECEC108929C8C2492A8D104A7F648D420C70E754FE45761044FB8E3FB9C68A11` |
| DR16 review/STATUS 0442 | `8CBBD0D0BDE4FC93C507EF1002A94B050DD3A85D64107DDF592B1E2DB6FD978E` |
| DR18 Confirm/channel 0645 | `5C1E24129CF8A14C97C80D4A7027EAF7F434BFFFEC2FF58872A159FE824552A1` |
| Current public preparation-page 0718 | `D040516EB3AD9F488025493D6D91EAADC99CEE780C22E09F7FDDB7ECC3EFF501` |
| Current preparation Worker | `80FDCF78FDB3C7E4110D976A7B1C7C7935E4D3AC7029B14176E5C84286B4C527` |

These sources may receive the already-authorized DR-21 successor before DR-27 starts; root must use the then-current registered complete body. No parent economic invariant is permission to reconstruct money on a list read.
