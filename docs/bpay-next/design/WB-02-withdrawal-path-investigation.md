# WB-02 withdrawal-path investigation

Source-only investigation of the full normal r3 `02_WORKBENCH.md:120–151`, all nine WB-02 CSV rows, current final-definer registry, and the saved native owners. No product edits, SQL/runtime/test/business calls, or hosted reads were performed. Application acceptance is NOT_RUN.

## Immediate implementation boundary

The narrow WB-02 build is a withdrawal-aware successor of `refresh_work`, new `work_wait_state`, and finite refresh additions to the existing private ordinary trigger and private Source withdrawal guard. Public first-authorise and withdrawal owners already reach these native paths; their signatures, full bodies, security, actual receipts and admission ordering can be reused unchanged. Preserve all last applied positions, effects, holds, immutable revisions and accepted Rule-B choices/snapshots/pins.

The actual final POSITION_APPLY refresh is **WB-06, next sequence 51**, per the parent's current master-chain instruction. Record that precise seam now; do not create an early WB-02 successor of the position writer. Thus the WB-02 withdrawal slice may close independently, while final reauthorisation-to-Ready/linked-Case refresh remains an explicit WB-06 integration obligation. This is not a claim of complete later app acceptance.

## Exact checklist disposition

| Row | Kind | Exact object | Required current change/context |
|---|---|---|---|
| WB-02-D01 | CONTEXT | `private.bpay_next_publish_staged_revision_v1(p_work_id uuid, p_revision_id uuid, p_command_id uuid)` | Preserve immutable current revision/publication receipt and Candidate order; add exact dirty/Processing invalidation and finite projection hook, not history scans. |
| WB-02-D02 | CONTEXT | `private.bpay_next_ordinary_authorisation_trigger_v1` | Add exact withdrawal/reauthorisation projection and origin-case WAIT refresh without zero/debt; preserve NEXT gate/legacy unchanged. |
| WB-02-D03 | DELIVERABLE | `private.bpay_next_source_withdrawal_guard_v1` | Existing qualified Source withdrawal guard installed by exact metadata; add actual finite work/origin-case WAIT projection hook and boundary timing, no zero/recovery. |
| WB-02-D04 | DELIVERABLE | `private.bpay_next_work_payable_state_v1(p_work_id uuid)` | Coarse NOT_APPROVED/APPLY_PENDING/READY is real; replace no function with fake full classifier. New component admissibility/projection remains missing. |
| WB-02-D05 | CONTEXT | `private.bpay_next_workbench_refresh_work_v1(uuid)` | Complete exact old/new contribution replacement and priority; final applied-pointer not partial publication. |
| WB-02-D06 | DELIVERABLE | `private.bpay_next_workbench_refresh_case_v1(uuid)` | Exact case-component contributions and origin WAIT/control transitions; unrelated debt unaffected. |
| WB-02-D07 | DELIVERABLE | `private.bpay_next_work_wait_state_v1(uuid)` | Exact withdrawn-paid/Source unauthorised origin predicate; no Pay Query debt pause; frozen offer boundary inputs. |
| WB-02-D08 | CONTEXT | `WB-02 output-only reviewed card` | Specification exists/reviewed, not implementation/installation/acceptance. Build only the exact remaining child deliverables; preserve existing source/installed owners. |
| WB-02-D09 | DELIVERABLE | `public.weekly_source_first_authorise_v1(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text, p_actor_user_id uuid)` | Keep installed qualified first-Authorise NEXT tail calling stage_current and publish_staged_pair after genuine approval. Reconcile exact latest Source protected/query/root deltas separately; finite maintained projection/runtime and full queue path remain unproved. |

The CSV's historical MISSING state for refresh_work/refresh_case is superseded by actual WB-08 source at1252. D01/D02/D05/D08 are reference/context rows, not duplicate physical implementations. D03/D04/D06/D07/D09 remain the concrete revised deliverables. D04 is a correct coarse payable predicate; reuse it without pretending it is the full Workbench classifier. D09 is correct actual Source publication-tail reuse, not a reason for a whole shared-owner replacement.

## Current physical winners and exact entry seams

| Object | Current physical winner | Native behavior / narrow hook |
|---|---|---|
| `private.bpay_next_ordinary_authorisation_trigger_v1()` | `03102026_1530_banking_pay_next_ordinary_approval_v1.sql:608–684` | AFTER INSERT/UPDATE `timesheets_financials.authorised_at_utc`, existing trigger689–690. Fresh authorisation637–641 calls current ordinary publisher (winner1236). Withdrawal645–680 resolves exact booking/contract/Candidate, locks Candidate control652–653 then exact work654–655, excludes Source659, checks actual approved physical revision662–671, sets WITHDRAWN/current pointer NULL673–676, bumps Candidate financial revision677–680. Add refresh after that bump; retain every original transition check and early return. |
| `private.bpay_next_source_withdrawal_guard_v1()` | `03102026_1580_banking_pay_next_source_withdrawal_guard_v1.sql:11–73` | Existing BEFORE UPDATE `weekly_source_root_authorisations.withdrawn_at_utc`81–84, actual NULL→timestamp transition only. NEXT/module SHARE24–25, exact TS+contract/Candidate28–32, Candidate-control UPDATE37–38 then exact work UPDATE45–46, exact SOURCE/approval identity47–52. Existing financial-effect dependency refusal58–62 remains. WITHDRAWN/current NULL63–66 and Candidate revision bump67–70 precede finite refresh. No public trigger rebinding is necessary. |
| `public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)` | frozen `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql:2126` | Service/plain request/active actor checks2172–2194. Exact durable successful replay2202–2222 returns without fresh admission or work mutation. Conflicting replay admits before refusal audit2208. Fresh Source admission2227 precedes callback/module/family/business locks. Actual root-authorisation UPDATE2512–2517 invokes1580. Original signature handoff and `timesheet_unauthorise_atomic`2546–2550 follow; TF trigger sees SOURCE and returns at1530:659. Actual protected withdrawal2618 and factual contract2628–2631 remain intact. Add refresh in1580, not a second fresh public-tail call. |
| `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | Pure identity3561 then early Source admit3569, real family lock3572, expected physical identity3584, genuine core3592. Only successful NEXT outer tail3598 stages current3603, publishes staged pair3605 and initial metadata binding3609. Keep it. Its core winner is0806; private stage/publish and current1236 ordinary publisher remain independently registered. It deliberately does not publish from a nested intermediate Source core. |
| `private.bpay_next_workbench_refresh_work_v1(uuid)` | `10102026_1252_banking_pay_next_workbench_summary_v1.sql:283–459` | NEXT296, Candidate control UPDATE298, exact work UPDATE299, metadata300; current revision304 and applied=current proof305–307. Current withdrawal loses revision and falls into DATA_UNAVAILABLE342 before MISSING_APPROVAL344. Add dedicated WITHDRAWN handling before using current revision. Existing before/after contribution delta308/456–457 and summary counts444–449 must stay. |
| `private.bpay_next_workbench_refresh_case_v1(uuid)` | same1252:461–620 | Already exists: NEXT475, Candidate control UPDATE477, exact Case UPDATE478, real components/rules495–509 and native totals; before/after group contribution484/618–619. Linked Cases only from work_collection; actual pause classifier remains WB-41, not an early debt-owner rewrite. |
| `private.bpay_next_workbench_work_members_v1(uuid,uuid)` | `10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql:5` | Current approved-member/source-book/segment conservation reader. Keep exact removed-Source fixes and native1236 origin attribution. Withdrawal need not infer an approved current revision or rebuild segment credit through it. |
| `private.bpay_next_work_payable_state_v1(uuid)` | `03102026_1430_banking_pay_next_position_writer_v1.sql:650–660` | Correct stable text `NOT_APPROVED` / `APPLY_PENDING` / `READY`. No replacement required. |
| `private.bpay_next_work_wait_state_v1(uuid)` | absent from current registry/source | New private predicate, starting WB-02 definition WITHDRAWN and exact work realised-source sum >0. WB-41 later owns confirming/adjusting Source and recovery-pause semantics. No Pay Query fact, admission try-lock, retained-protocol or latest-run reconstruction belongs here. |

## Withdrawal facts and projection

Core1324:119–129 requires current_revision_id NULL whenever approval is not APPROVED. Its applied_revision FK185 and native position328–346 retain the genuine last approved financial basis. Read last approved **SUM(position.approved_source_ex_vat)** and realised **SUM(position.realised_source_ex_vat)** for the exact work's applied-position set. Do not substitute a zero revision, fabricate an approved line or financial event, or read current_revision_id as a financial authority after withdrawal.

Distinguish no positions/not-yet-applied from a genuine applied certified-zero revision. Unknown/incomplete application is not GBP0. Native rows may be partly moving to a later publication until final apply; exact per-position applied_revision_id compatibility and retained applied revision must be checked before asserting known display amounts. If last fully applied basis is unavailable, retain typed unavailable/Processing evidence rather than inventing it.

For complete withdrawn work, realised >0 gives ACTION/AWAITING_REAUTHORISATION; realised=0 gives BLOCKED/APPROVAL_WITHDRAWN. Preserve last approved amount even when nominal residual is zero or negative. No recovery row is created by this branch, and all entries remain nonselectable. Current1252 counts action only for actual emitted ACTION rows; an already-paid work therefore needs a retained display contribution even if its payable residual is zero. Do not let the ordinary member loop's `continue` for fully settled parents hide the required Action work.

**Concrete native schema support to check before implementation:**1252 migration item CHECK171 says EVIDENCE implies `not amount_known`; GROUP_EVIDENCE CHECK144–146 requires EVIDENCE; AVAILABLE CHECK165 requires positive nominal, ZERO166–169 requires all financial figures zero, RECOVERY170 requires positive recovery. Therefore a known nonselectable withdrawn-paid display with last-approved GBP160 and no new payable/recovery is not representable as an existing known GROUP_EVIDENCE/EVIDENCE row. Use a narrowly defined feature-owned withdrawal display shape if needed (e.g. a dedicated nonselectable informational portion with retained A/C/H and zero offered/recovery), not a fabricated ZERO/RECOVERY or relaxed global unknownness. Preserve existing per-member conservation checks and current native financial totals; withdrawal presentation must not double-count approved amounts by also emitting ordinary portions.

## Exact WAIT transition and lock closure

Before replacing the WORK group's item rows, take `before_wait = EXISTS(stored item for exact work/group with reason_code='AWAITING_REAUTHORISATION')`. AFTER the current native owner has changed approval/position facts, call the new predicate once for `after_wait`. Two predicate calls cannot represent before/after: both see the already updated work. Only when these booleans differ refresh distinct non-NULL `work_collection.case_id` rows with `work_collection.work_id = this exact work`, deterministic ID order. Native1519 unique(work_id,component_key)36, unique(case_id)24 and exact Candidate FKs39/44 preserve genuine origin; no Candidate-wide unrelated Case/debt fanout.

Refresh after the Candidate financial_view_revision bump so stored projection revision matches the actual completed native transition. Existing bounded refresh_many1252:624–659 discovers only exact linked Cases639, validates combined <=100 and16KiB631–643, then prelocks Candidate→work→Case→metadata before the common projection-revision update. Prefer retaining that existing finite lock/bound contract or equivalent same-scope prelocks; avoid first acquiring fresh Case locks after the singleton projection-revision lock while another refresh_many holds Case and waits on revision. No clock/admission/financial owner is added inside the classifier.

The Source guard inherits the true public owner's already-acquired Source admission key and module decision; it must not introduce a late first pay_query_admit. Its new refresh stays inside its existing NEXT-only branch. Record public0556:2227→actual UPDATE2512→private1580→new refresh as the Source SP-12 census call path. The ordinary existing trigger operates only on NEXT and its genuine ordinary/expense route; preserve public authorisation/unauthorisation/auth/locks and inherited admission unchanged.

## WB-06 final-application follow-through (explicitly later)

Current `private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer)` final1430:456–648 has genuine readback-only exits493–499 and506–511, original NEW/REMOVED loops, per-component native reconcile554/607, then final publication APPLIED613–616, work.applied_revision_id617–620, job DONE621–624, command COMPLETE625–626. It bumps Candidate financial_view_revision638–642 and returns without any Workbench refresh.

WB-06 must add the exact completed-position projection hook after that final financial revision bump, only for the genuinely completed transition, retaining terminal replay/stale-page returns as reads. That hook uses the new refresh_work before/after WAIT protocol and therefore refreshes linked origin Cases. Do not clear stored WAIT through an early reauthorisation publication while positions remain partly applied, and do not declare final reauthorisation app behavior proved before this later seam exists. Source reauthorisation uses its already-correct stage/publish tail and same native POSITION_APPLY completion.

## Boundaries and acceptance

The Source guard's `BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY` is a genuine existing restriction for ANY exact work financial_effect. Keep it: successful paid ordinary withdrawal is supported by the ordinary trigger; this investigation does not manufacture successful paid Source withdrawal in spite of that guard. Its successful eligible Source transition still reaches NEXT refresh. Existing accepted PREPARE/selection/pin/run/frozen rows and holds are unchanged; later withdrawal is next-run information only.

WB-41 owns actual origin-recovery pause and recovery classifier; WB-13 owns labels. WB-02 adds the predicate and real finite refresh linkage now, not a Case-allocation/financial rewrite. No Worker changes are named. All eventual app journeys (including post-Create, Source genuine guards, unpaid/paid and reauthorisation) remain NOT_RUN here.

## Saved-source SHA-256 snapshot

- `supabase/repeatable/03102026_1530_banking_pay_next_ordinary_approval_v1.sql`: `df943acaa6ddc4fa5bea877621762e96c93360d4030383f29b5bffca2d3aa301`
- `supabase/repeatable/03102026_1580_banking_pay_next_source_withdrawal_guard_v1.sql`: `90e1f9ad45ba1d64a9e02b009058da45841da2d0515b057eda29699349659ad1`
- `supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql`: `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933`
- `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`: `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`
- `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql`: `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa`
- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql`: `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b`
- `supabase/repeatable/10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql`: `4f07a5ab56fd01aa40ccdb9163f3458c66a838af8e056f35987be2c24cb70542`
- `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql`: `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`
- `supabase/migrations/03102026_1324_banking_pay_next_core.sql`: `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c`
- `supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql`: `924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3`
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql`: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`
- `docs/bpay-next/final-definer-closures.json`: `fc844c8d17ed8cc7b04f4e650e0e38ed418365233c7771424d07a910f398838e`
