# FC-10 current investigation — sequence 108

Read-only findings and narrow design recommendation, 11 October 2026. No product, checklist, SQL/database, business-row, fixture, hosted or screen-generation action was performed. All future compilation, joined application, recovery/queue, financial and performance checks are **NOT_RUN** by this investigator. Root decides and writes product.

Sources below use `PLAN` for the approved `BPAY_NEXT_BUILD_PLAN_20261009_codex_r3` directory in the ordinary backend's `codex_outputs/claude-stage2-continuation-20260925`; `BE` is the active `bpay-next-build-backend-20261009` worktree; `FE` is its frontend sibling. Saved current files and final-definer registry control over historical card filenames.

## Authority and protection decision

Read the full FC-10 card, `PLAN/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1217–1236`, every working checklist row (ITEM plus D01–D10, `BE/docs/bpay-next/IMPLEMENTATION_CHECKLIST.csv:1640–1650`; Markdown `:3190–3214`), R3 artefacts A265–A267, R2's connected Case/WAIT requirements, and the shared editor chain. Dependencies are FC-16, FC-03, DR-12, WB-08, WB-27 and F-16. Master position is after DR-12, before WB-41 (`PLAN/08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:43,192`; sequence `:108`).

**Recommended whole-case semantics: refuse while native protection remains.** The revised card `:1221` explicitly says a protected whole-case request reports required cancellation and does not silently pause the captured slice; `:1223` forbids COMPLETE while a reserved slice is outstanding. All working rows repeat that contract, and R3 A265–A267 repeat it at `review/R3.json:39500,39536,39572`. Current FC-16 1509 `:235–237,288–291` implements precisely this WHOLE_CASE refusal. Use that five-argument guard; do not use FUTURE_ONLY to claim a whole Case is paused while a protected slice remains.

The older Done example `:1234` still says the confirmed Draft deducts its frozen £30 and the result names the protected amount. That remains true for a refusal: the existing deduction survives and the result identifies its protected slice. It does not override the explicit revised prohibition on a whole-case COMPLETE. Record this interpretation/design difference explicitly, since supplied SCR-123b still advertises a Paused result despite a protected confirmed Draft. If root chooses future-only status mutation instead, it needs an explicit resolution of these contrary literal completion clauses; the existing guard does not provide that authorization by itself.

Pending selected accepted input before any hold is also protected. FC-16 1509 `:17–146` binds exact accepted choice/selection and typed completed release/expiry/discard/cancellation/zero/realisation dispositions. It never decides from a CANCELLED flag or elapsed deadline alone. Its summary `:259–285` returns protected amount and capacity **NULL** while any PENDING binding remains; never present unknown as £0. READY/PREPARING removal uses existing DR-05; a confirmed Draft requires Candidate cancellation, without building future EX-27 here. Native `next_action` values are `REMOVE_CANDIDATE_OR_DISCARD`, `DISCARD_PREPARATION`, and `CANCEL_CANDIDATE_PAYMENT` (`:149–164`).

## Exact current owners and reusable behavior

| Object | Current registered physical owner / evidence |
| --- | --- |
| `private.bpay_next_case_allocation_plan_v1(jsonb)` | `supabase/repeatable/11102026_1939_banking_pay_next_case_edit_v1.sql:1736` |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | Same 1939 `:2159`; accepted capture and original money-lock/reservation path retained |
| `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` | Same 1939 `:409` |
| `private.bpay_next_enroll_one_v1()` | Same 1939 `:737` |
| `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` | Same 1939 `:1145` |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` / current component page | Same 1939 `:2681` / `:2952`, existing CASE_COMPONENTS, no new page kind |
| `private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text)` | `supabase/repeatable/11102026_1509_banking_pay_next_case_change_guard_v1.sql:238` |
| Native protected bindings / reserved-slice page | Same 1509 `:10` / `:172`; bounded factual references, existing cursor and exact Case revision |
| `private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint)` | `supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql:4`; expiry final is alias-only 0910, not the helper's physical owner |
| `private.bpay_next_workbench_refresh_case_v1(uuid)` | `supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql:4234` |
| Current Case facts reader | `supabase/repeatable/04102026_0585_banking_pay_next_current_case_pages_v1.sql`; current component reader is 1939 |

**No allocation successor is presently required.** Current 1939 derives live current status for a new scope (`:1790`), but accepted memory input supplies the captured OPEN state and exact Rule (`:1791–1815`). Existing `:1958–1959` already zeros nominal collection/reservation for a non-OPEN Case or future due date. The earlier paid/unfunded and single row-kernel formula remain intact. Preserve those branches and accepted instruction/period/Rule facts. A status toggle does not require a new Rule, period amendment, selector, monetary estimate, floor source or frozen-input rewrite. D01/D09 are context/reuse evidence, not an instruction to replace a functioning money owner.

WB-08 already moves PAUSED Case components into WAITING with CASE_PAUSED (`0559:4334–4335`), after stronger accepted/reserved/review reasons; its current wrapper locks Candidate control then Case (`:4250–4251`). Existing FE Workbench renders “Paused – not collected” and greys paused items (`js/bpay-next-workbench.js:1306`). Refresh preserves that native priority and all money/held balances.

## Smallest remaining database contract

The card names one new `banking_pay_next_case_pause_v1` repeatable and two intake actions: CASE_PAUSE and CASE_RESUME with command UUID, Case UUID, expected Case revision and optional reason. Candidate is derived from exact native Case identity; do not require a CASE_CREATE receipt because legitimate ordinary collection Overpayment Cases have their own Work origin. Native recovery component membership identifies applicability; credit-only Cases are not recovery. The native status domain already admits OPEN/PAUSED/CLOSED/WRITTEN_OFF (`core1324:360`). Refuse CLOSED/WRITTEN_OFF and incompatible recovery shape; decide same-target-state behavior explicitly without silently bypassing a fresh expected revision or protection proof.

Reuse ordered command receipt/member/enrollment/job handling from 1939. A small private typed request/receipt support is justified to persist original action, actor/module/Case/Candidate, expected revision, optional reason and exact accepted identity/outcome across unknown replay. Do not introduce a parallel resolution store or generic workflow. First retry compares all original bytes/IDs/operands before fresh status checks; a fresh accepted job re-proves expected revision and protection before mutation. Later stale/protected state produces an honest typed refusal/BLOCKED result, never a successful status receipt. Persist or re-read a qualified terminal receipt through existing CASE_COMPONENTS; no new RPC/page kind is needed. Existing job lease, predecessor order, original enrollment clock, exact id/kind queue body and unknown retry rules remain controlling.

Use module → Candidate/control → own job → Case → ordered components. Reuse the same exact completed-replay admission first. **Before any fresh mutation**, call the current early DR37 Candidate financial fence with the actual job/lease/owner identity. 0904 `:21–28,63–71` is Candidate-wide and returns native 55P03 `BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY` for a different live owner. That is finite defer/retry through the existing queue, not terminal financial REVIEW/BLOCKED and not a new commit owner. The fence does not authorize adopting a NET lease.

Then FC-16 WHOLE_CASE reproof occurs under component UPDATE locks, and only an unprotected eligible Case may toggle status plus advance Case revision. Preserve current Rule, original age/schedule, principal, realised and held amounts, all instructions/results/holds and snooze basis. Advance financial-view revision before the same-transaction existing WB-08 Case refresh, so current context/list keys and readers invalidate honestly. No Work lock is taken by the existing Case refresh; do not add a Work-after-Case lock path for an ordinary collection Case. No public trigger, producer rewrite, bulk Case/history enumeration or new money formula is needed.

Native money-event kind checks do **not** include PAUSED/RESUMED (`0300:494–495`); legacy `public.pay_finance_case_events` is a different audit family. An immutable typed Pause/Resume request/receipt can preserve reason, actor and old/new status without inventing a money event or adding CORRECTED, which would incorrectly make an otherwise preactivity Case fail FC-09's “events beyond APPROVED” predicate. Do not expand the money-event enum merely to copy the legacy audit name.

## Reason, protection facts and response bounds

Legacy single writer `26052026_2100HRS_NEW_FUNCTIONS.sql:95457–95625` accepts optional `p_reason`, trims blank to NULL, changes only its public pay_advances status and appends public audit; it has no explicit reason length ceiling. Its applicable legacy enum names and no-outstanding refusal are historical guidance, not a reason to call that writer from NEXT. In particular, supplied SCR-123 also offers Pause recovery on an Advance not yet paid out with £0 current recovery outstanding, so do not transplant that balance-only refusal automatically.

Use a deliberately closed optional/null text operand with no NUL and a documented finite UTF-8 ceiling. Existing native Case text uses 2,048 bytes (`1536:36–48`, FE case-preview `:16`). General Worker command budget is **2,048 total JSON bytes** (`bpay-next-command.js:22–27`), so adopting the full Case text ceiling needs a narrowly scoped request ceiling for only these actions or a smaller reason ceiling. Do not claim a card-mandated maximum: the exact limit is a builder contract choice and must match SQL, Worker, Office and journal restoration, including encoded JSON size.

Protected result facts must come from the exact native binding/Case scope. Existing 1509 reserved-slice reader admits 1–100 rows, checks Case revision and returns a bounded native cursor (`:180–229`). Keep its existing continuation if several protected refs exist; do not publish a prefix total as complete or collect all pages in a browser array. Existing summary has exact count and unknown-safe amount. To name a human Draft date, point-join the actual returned run UUID to native `pay_run.pay_date` (binding DTO currently has run/channel/status/actor but no date), rather than displaying a UUID, making up a batch code, or using today's date. This is a small factual field support if needed by the card. Frozen deduction remains payable; Pause must neither release it nor certify it as paused.

## Worker and native Office seam

Current `broker/src/bpay-next-command.js` already has closed INPUTS/receipt, byte/timeout, replay/error and ENROLL wake machinery, including CASE_CREATE and CASE_EDIT. Add only CASE_PAUSE/CASE_RESUME exact schemas/results/refusals and their original ordered financial job maps. No auth proof, automatic retry or new HTTP route is named by FC-10. Existing `/api/banking/pay/next/command` is the admitted route; legacy `/api/banking/finance/cases/:id/pause|resume` remains fenced under NEXT (`bpay-next-legacy-fence.js:172–185`; index `:199381–199390`).

Existing FE native entry points are `confirmBankingFinanceCasePause` at `js/main.js:20589` and Resume at `:20746`; callers are `:102756–102777`. Their current bodies submit to legacy endpoints and report success from the HTTP return. They need a small NEXT owner-qualified branch before any legacy state/read/mutation, following the completed native create/edit pattern `main.js:12988–13008`. Preserve LEGACY behavior. Reuse the native frame adapter, actor/agency/epoch/parent stack fences and Office durable journal for original command/Case/revision/reason bytes, pending/readback/retry, and explicit stale Refresh. Do not clear an unknown outcome or allocate a replacement UUID. Acknowledge only a qualified terminal status/revision receipt; don't equate admission with Pause complete.

Use existing CASE_COMPONENTS/native Case state and FC-16 factual references; current edit context/card rendering and `openCaseProtectedDraft` are reusable native seams (`bpay-next-office.js:1964–2006`). Pause cannot reuse CASE_EDIT's editable/no-activity predicate: recovery after prior funding is exactly FC-10's intended use. No new Case list, FC-02 renderer, generic Case editor, release control or future audit/restructure/write-off UI belongs here.

## Supplied screen and required differences

Inventory `SCREEN_DESIGN_POLICY_20261010/SCREEN_INVENTORY.md:199` assigns **SCR-123** (B9, T04) to FC-10. Read the three supplied JSON source designs: SCR-123 pause confirmation, SCR-123b result and SCR-123c resume confirmation. Meaningful controls are optional Reason, Keep collecting/Keep paused to dismiss unchanged, and Pause recovery/Resume recovery to submit the same qualified native intent. Required current data are actual Candidate/Case label, actual outstanding/weekly facts where known, current status and protected result references. Background example list/report/new-case controls belong to existing or later owners; do not build them from this card.

SCR-123b says Paused despite a confirmed protected Draft; revised card forbids COMPLETE and requires cancellation. Its successful-Paused example is therefore only appropriate to an unprotected result; protected state needs a truthful refusal/result and next action. SCR-123c promises the next Draft takes the illustrated exact amount; actual native affordability, threshold, floor, capacity and selection still apply, so show a resumption consequence without certifying a future monetary allocation. Record each actual implementation/design difference and owner A2 appearance-versus-card data boundary in the builder decision; actual A4 screenshot review is later and **NOT_RUN** here.

## Source identities at investigation

| Current source | SHA-256 |
| --- | --- |
| 1939 Case edit/current shared owners | `7B723A61096AC50853DBC609ACBBA6437B403F11AA796A6F77E39A78B8308F66` |
| 1509 FC-16 guard/bindings | `087C3C5D3AE9CFAA94B66EA58B338358D0236F2B4953BE5EE6ADF7E3DFC60FD4` |
| 0904 Candidate fence helper | `FA3C73A83075F67246E62ADEA7BA1467C019BADA601D3DB880EC51B48CDD1A9E` |
| 0910 final expiry alias-only owner | `14803CD993EF6BB7E3C11835E73AD2219520CE5F1DCF55B0DABDB74088D9C654` |
| 0559 Case projection | `514FB6A069DFE860D06C76D98302554BF3ED93E8E027CDBAC460BF8CEA7DA9A6` |
| Worker command | `48BF0258B8D20E58138A463041EBB857BBC827699E5A2F234769781E6CD75B53` |
| FE Office | `67FBF22D2D90D56D3BB82B3A18C8FBEC886E8E10E84DA64F81781EFC24CDC7B6` |
| Final-definer registry | `42D574D1DAD002F82C37A20DCB630D1269129EEDBFA29C4F34324262F94194B5` |

The source catalogue is a dated observation; root must reread actual current shared parents before building. FC-09 A4/source activity was concurrent. This note authorizes no product installation or application acceptance and changes no checklist state.
