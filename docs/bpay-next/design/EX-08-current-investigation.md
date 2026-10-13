# EX-08 current investigation — sequence 102

Read-only checkpoint, 11 October 2026. Product, checklist, database, business data, tests, hosted actions and screenshots remain untouched. All compilation, source gates, application, queue and joined acceptance checks are NOT_RUN by this investigation. Root alone owns product edits.

## Authority and scope

- Full card: approved plan `04_EXECUTION_AND_OUTCOMES.md:318–342`; all eleven working rows: `IMPLEMENTATION_CHECKLIST.md:2833–2858`; sequence/dependencies: `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:186,487`.
- OR-46 permits a later Candidate pay-channel/Umbrella change while an earlier accepted Draft is unpaid. That Draft keeps its frozen channel, destination, money and reissue creditor (`01_BUILDER_RULES_AND_FOUNDATION.md:170`; `10_OWNER_COMMENTS_THIS_CONVERSATION.md:87`; R2 `addendum/R2_EXACT_CONNECTIONS.md:50`).
- Apply changes only `candidates.pay_method` and `umbrella_id`. Do not invoke the legacy refresh apply, rewrite rates/Contracts/Timesheet financials, reprice accepted Create, or blanket-refuse an unpaid Draft. Shared calculators remain the money authority. EXPENSE/MILEAGE preserve their reimbursement gross; Additional remains channel sensitive.
- The required new owner is `banking_pay_next_pay_method_change_v1`, exposing `public.bpay_next_pay_method_change_preview_v1` and `public.bpay_next_pay_method_change_apply_v1`. Exact signatures remain a design choice. Existing GET/POST paths and native `openCandidatePayMethodChangeModal` must receive NEXT branches.
- Dependencies named by the card are F12, F16, FC03, FC16, CM04 and WB08. Existing valuation-policy helpers are retained. Contract-move comparison work is excluded. EX08 emits accepted bindings and finite affected IDs to the later WB29/WB45/CM11 consumers; their resolution store and final classifier are not built early.

## Existing callers and exact gaps

`broker/src/index.js:159343` owns GET `/api/candidates/:id/pay-method-change-preview`; POST begins `:159644`; routes are `:200147,200153`. Both currently use legacy complete Timesheet/session scope and legacy refresh proof. GET calls `candidate_pay_method_change_refresh_scope_v1` at `:159438–451`; POST calls `candidate_pay_method_change_apply` at `:159975–998`. These are not NEXT implementation seams to invoke.

Both existing handlers reject a same-channel request (`:159406,159917`), so a real Umbrella-company change needs the NEXT branch to compare the target Umbrella identity too. POST currently accepts PAYE bank fields in `destination_patch` (`:159932`); NEXT must accept only its card-authorized method/Umbrella change and preserve bank-maintenance ownership. Existing admin qualification and unknown-response preservation (`:160001–008`) should be reused. Do not create automatic mutation retries or new operation UUIDs.

Frontend `js/main.js:289165` owns the native modal; the Candidate entry point invokes it at `:281009`. The old modal exposes raw Timesheet/session UUIDs and pay dates (`:289460–528`) and keeps pending operations in a memory-only Candidate registry (`:289222–286`). NEXT needs the closed impact categories and the original pending operation bytes/identity; it must not carry the legacy unbounded proof into the new reader or surface its technical IDs.

## Existing owners available for reuse

| Owner | Current physical source / evidence |
|---|---|
| Shared selected quote / target valuation | `06102026_1157_banking_pay_shared_selected_quote_v1.sql`: `private.pay_finance_selected_quote_v1(...)`; `private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` at 932 |
| Candidate metadata + bounded continuation | `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql:151–196`, metadata writer and existing Candidate trigger |
| Whole-page Work/Case projection | `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql:354–409`, `private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[])` |
| Current Work classifier | `11102026_0426_banking_pay_next_pay_hold_classification_v1.sql`, `private.bpay_next_workbench_refresh_work_core_v1(uuid)` |
| Native Work Processing | `10102026_1346_banking_pay_next_position_projection_boundary_v1.sql`, `private.bpay_next_workbench_mark_processing_v1(uuid)` |
| Current Case protection | `11102026_1509_banking_pay_next_case_change_guard_v1.sql`, `private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text)` |
| Current Case-create publication | `11102026_1536_banking_pay_next_case_create_intake_v1.sql`, current extended accept/apply owners |
| Frozen destination / transfer | `04102026_1827_banking_pay_next_candidate_destination_group_v1.sql`, capture destination; `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`, simple transfer |
| Legacy callback isolation | `04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql`: NEXT avoids legacy financial callback refresh |

The current registry is `docs/bpay-next/final-definer-closures.json`; final signatures/physical paths, rather than historical references, govern implementation.

## WB44 continuation is real, but not a resolution producer

Existing Candidate AFTER UPDATE metadata trigger covers `pay_method` and `umbrella_id`; no new public trigger is needed (`1429:176–196`). The writer updates cached current channel/VAT metadata and schedules the existing METADATA task in the same transaction (`:151–173`).

METADATA_GROUP already seeks at most 100 maintained native groups, discovers Work/Case identities, and preholds Candidate → Work → Case before the task lock (`:335–341,373–387`). When `control_changed`, it calls the existing refresh-many classifier; its source explicitly says no rate/NET/earnings valuation (`:471–491`). METADATA_SUMMARY seeks bounded exact maintained natural keys and updates display metadata without altering measures/counts (`:353–359,492–502`). Thus WB44 supplies finite continuation and reclassification, but does not itself persist an accepted conversion decision.

Refresh-many accepts at most 100 combined native Work/Case IDs and 16 KiB, prelocks Candidate → Work → Case, and queues the existing ORIGIN continuation for larger collection expansion (`1438:354–409`). Use these actual owners; do not synchronously refresh all Candidate history or add a new producer/task/queue protocol.

## Native receipt carrier investigation checkpoint

The legacy apply has an existing immutable audit receipt lookup keyed by Candidate plus `after_json.operation_id`, action `PAY_METHOD_CHANGE_PROSPECTIVE_ONLY` (`20260221_01_add_tms_ref_num_and_admin_rpcs.sql:199–222`). Its canonical operation JSON includes Candidate, expected method, target method, source sequence and destination patch, but omits reason (`:165–171`). The legacy body cannot be called; its audit carrier may offer a narrowly reusable accepted NEXT decision receipt if the new owner writes and replays its own closed native tuple.

Required proof before choosing that carrier: exact bounded indexed operation lookup; full actor/agency/epoch and economically relevant request equality; immutable native affected Work/revision and eligible Case/component identities; accepted quote/policy/target operands; same operation replay before currentness rejection after the Candidate mutation. A generic new scope table/protocol or browser-owned list of all history is not justified. This checkpoint does not yet claim the legacy receipt itself is a suitable complete NEXT resolution store.

## Smallest proposed implementation boundary

1. Existing GET obtains a bounded server preview for exact Candidate/current revision/target channel+Umbrella, policy/native source versions and its impact categories. Use raw native context, not a digest/signature framework. Do not calculate headroom or new money formulas.
2. Existing POST locks the Candidate/native money domains in original order, validates the exact accepted preview context, writes only the two current Candidate fields, and retains an immutable exact decision receipt. Accepted identical unknown retry returns the original result before rereading mutable current scope; altered bytes refuse.
3. Compatible eligible future Work must consume the accepted shared-calculator decision and stay Ready. A genuinely unresolved item alone becomes CHANNEL_RESOLUTION_REQUIRED. Protected captured slices keep their original accepted basis. This requires tracing the actual existing resolution consumer before declaring the producer path complete.
4. Trigger existing WB44 dirty/Processing → existing WB08 projection within the same transaction. Keep original protected Draft/transfer/reissue destinations immutable; future current Candidate change is not a Case mutation.
5. Native modal shows the supplied impact categories, allowed Umbrella choice, protected explanation and Apply change/Close. Its pending state preserves original POST bytes and operation identity after unknown outcome; no automatic retry.

## Supplied screen and remaining focused questions

B6 `SCR-74_pay_channel_change_impact.json` and its original PNG define “Change pay channel”, Candidate/current→target subject, Umbrella chooser, “Stays payable”, “Needs a decision”, “Not changed”, protected Draft/reimbursement explanations, Apply change/Close. Example values/rate decisions are illustrations, not authority to build WB29 early. Screen policy uses “pay channel”.

Focused closure and remaining implementation choices are recorded below.

## Focused source closure

**R4 governs the division of work.** Card lines 325 and 334–335 explicitly say compatible Ready work remains payable through WB29/WB45/CM11 and EX08 emits its accepted binding plus finite affected native IDs to those consumers. A1 money chart lines 187–192 and A2 classifier chart lines 84–90 likewise separate an exact unresolved EX08/unsupported identity need from an ordinary source/target channel difference. “Compatible” means the original approved native classification/units can use the existing automatic shared quote with an exact valid policy/window and target treatment; it does not mean a new rate decision, an assumed amount, or a blanket action requirement. Reimbursement gets no channel conversion. Unknown or incompatible authority remains an explicit unresolved need.

The current Work classifier is **0426**, superseding the 1438 core body; the 1438 public-private refresh wrapper/refresh-many support remains current. 0426 reads current Candidate target at lines 50–59, native residual/member facts at 124–156, and existing accepted Work identities at 158–163. Its new positive portion classifier retains Ready by default and preserves accepted and H/R portions (181–200). There is no accepted EX08 resolution lookup or CHANNEL_RESOLUTION_REQUIRED producer in that body. Do not claim the missing later WB29 classifier/resolution consumer already exists or implement it under EX08. The new receipt must expose enough exact native decision provenance for that later consumer, while the real current WB44 task can reclassify stored native facts now.

The pure helper is `private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` in 1157:932–990. It selects the original inclusive dated policy window and refuses missing/ambiguous authority; it delegates automatic suggested-equivalent arithmetic to the shared immutable quote. `public.pay_finance_component_resolutions_apply` in the same file instead updates **legacy** `public.pay_finance_case_components` (827+) with legacy saved resolution fields. It is not a reusable NEXT persistence writer. NEXT Case component RESOLVED/REVIEW fields exist at schema0300:91–127, but are a mutable target cache and are not an immutable Candidate-change operation receipt.

### Minimal genuinely required persistence

The inspected current source has no typed NEXT channel-change operation/affected-scope receipt. The legacy audit carrier is not adequate without inventing a new contract around it: it omits reason from canonical replay, and its operation lookup is a history search rather than a declared typed UUID point. **Recommend one small supporting migration with a private immutable channel-change receipt and its finite native affected-scope members**, owned only by the card-named preview/apply owner. This is one accepted operation binding for EX08, not a second WB29 saved manual resolution store.

Minimum header facts: exact operation UUID, Candidate/native actor and owner epoch, original and target channel/Umbrella identities, Candidate/current financial metadata version, exact raw accepted preview context/request including reason, native valuation policy/window operands actually used, and accepted immutable result/processing enqueue identity. Minimum scope facts: native Work/revision/approved line or segment identities or Case/component/revision identities, source classification and source basis, accepted compatible versus exact unresolved decision, protected versus unreserved portion, and server-derived quote operands/results only when authority is complete. Preserve protected identities separately from future editable capacity. Native FKs/uniqueness, immutable insert/replay guards, private ACL/RLS and exact operation/member seek indexes are required support. No browser amount, generic scope bag, history inventory, digest/signing framework or new financial job/command is justified.

Replay must first qualify the retained original operation/actor/epoch/request bytes and return the retained accepted result even after current Candidate metadata changed; altered reason, target Umbrella, preview/native scope or request UUID must refuse. A failure before acceptance must not masquerade as an immutable accepted receipt. A no-op current method plus identical Umbrella has separate honest handling; an actual same-channel Umbrella change must be allowed.

### Native bounds, locks and publication

- Existing maintained group index is `bpay_next_workbench_group_candidate_idx(candidate_id,group_key)` (schema1252:64). Existing METADATA schedule captures the terminal group/summary key (1429:98–103), and task pages seek exact Candidate groups/keys with limit1–100 (335–341,353–359). ORIGIN task is keyed by native Work ID and has expected current/applied revision, approval, FVR and wait state (1429:91–96,117–136,395–404).
- Native Work Candidate/week seek is `bpay_next_work_candidate_week_idx(candidate_id,week_ending_date,id)` (core1324:994–995); Case existence seek is `bpay_next_case_candidate_exists_idx(candidate_id,id)` (:1019–1020); Case component seek is `bpay_next_case_component_candidate_idx(candidate_id,case_id,component_ordinal,id)` (0300:129). Use native indexed exact membership, bounded pages and explicit complete/cursor semantics. Never silently truncate protected/future categories or echo a whole history array.
- Candidate update will invoke the existing metadata trigger, which locks Candidate control (1429:157–158). Apply must prehold the existing Candidate control before its actual Candidate UPDATE and preserve module → Candidate/control → Work → Case ordering when it touches money/projection domains; do not obtain a Work lock after a Case lock. Existing refresh-many enforces whole-page preholds and 100-ID/16KiB bounds. The existing task status/epoch/generation/cursor validation owns continuation replay; no arbitrary active lease adoption.
- Do not add another Candidate trigger or call both the existing trigger and an unconditional duplicate metadata schedule. Preserve original dirty/progress/count accounting. Emit exact native IDs for required Processing/ORIGIN support; use existing METADATA_GROUP continuation for bounded current group reclassification.

### Worker and native screen readiness

Exact route retirement is `broker/src/bpay-next-legacy-fence.js:185–186`. Add only the two card-named exemptions with actual handler qualification from the fresh owner-status adapter (same file4–32,59+): NEXT branch on NEXT; legacy behavior remains legacy; DISABLED refuses. The exemptions must never fall through to legacy apply. Service-only public SQL must independently enforce NEXT, active native admin and exact namespace; frontend can supply intent, not authority.

Original B6 SCR74 PNG was viewed directly. It has a native Close button, Umbrella company chooser, the three impact cards, fixed protected/reimbursement explanations, and Apply change. Close dismisses without mutation; chooser obtains a fresh preview for the actual company; Apply submits only the retained accepted operation. The illustrated Night-rate manual choice and converted-value chip belong to the later resolver consumers and cannot authorize their early implementation. Keep the current reader honest if its required source authority is missing; no fabricated row/value/chip or silent fallback to legacy scope. Native application A4 captures and full policy review remain NOT_RUN.

This investigation closes the current-source census and recommended supporting receipt boundary. Root still chooses exact public signatures, finite limits/paging and receipt column names in the design review before product work. No product implementation or item PASS is claimed.
