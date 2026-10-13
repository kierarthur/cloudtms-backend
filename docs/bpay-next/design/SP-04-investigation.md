# SP-04 current investigation and minimum design boundary

12 October 2026. Sequence138. **Design investigation complete; no implementation or A4 approval.** Read the full SP04 card (05_SOURCE_AND_PROTECTED_PAY.md:188–218), all ITEM/D01–D05 checklist rows, master sequence/dependencies/rulings, SP17's I3/derived Withdrawn record and current saved sources. Applicable repository AGENTS read; current task is notes-only. No product, pack, SQL/RPC, business data, fixtures/tests, hosted calls, rendering or commits.

## Exact scope and current gaps

SP04 makes existing single-list, selected-bulk, bulk-row and Contract Week Unauthorise reach the genuine Source withdrawal owner. Ordinary Timesheets retain ordinary ownership. Source unpaid withdrawal changes CURRENT approval only, no new revision/publication/job/debt; frozen Drafts/history stay untouched. Source actual financial dependency refuses. WB02/WB41 own subsequent Blocked/WAIT/recovery presentation. OR45 removes old-engine operational parity, not native security/admission/epoch, immutable history or obligations.

| Deliverable | Current exact source / gap |
| --- | --- |
| D01 genuine withdrawal | Final registry: public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid), supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql:2126. Reuse body; no proved SQL correction in this investigation. |
| D02 retained NEXT guard / retract | private.bpay_next_source_withdrawal_guard_v1() final10102026_1332:367 (not historical1580); private.bpay_next_source_current_retract_origin_v1(text,uuid,uuid) final04102026_0475:535. Reuse existing native mechanisms, no extra publication/retraction to implement routing. |
| D03 missing common router | broker/src/weekly-source/unauthorise-routing.mjs does not exist in current source. Mirror the existing authorise-routing.mjs's informational dispatch structure; genuine authority stays DB. Join actual handleTimesheetUnauthorise before ordinary/correction mutation; preserve ordinary branches. |
| D04 missing per-row Source result UI | FE main.js selected-bulk normalizes only generic success/failure and builds text/OK completion at261...; no explicit Withdrawn/Refused/STALE native result consumer. Implement only NEXT-qualified SCR139 result slice with own factual rows. |
| D05 request / availability | public.weekly_source_first_authorisation_withdraw_request_v1(jsonb):3447 and _withdraw_available_v1(uuid):2555, unique physical declarations in17092026_0600. Actual Worker command already exists at routes.js:46/709; stale frontend comment claiming no broker route is incorrect. Reuse exact helpers/receipt, separately verify final source/ACL before installation claim. |

## Actual caller and signature census

- Worker broker/src/index.js:75995 handleTimesheetUnauthorise is the common single handler, authenticated admin; it takes original requested/expected Timesheet and selected expected_row_signature. At76445 it calls guardCurrentTimesheetWrite, then correction-pair flow; ordinary timesheet_unauthorise_atomic at76518. Insert the Source dispatch before any ordinary/pair mutation so a managed root cannot reach either financial owner.
- Selected bulk endpoint index.js:95699 handleBulkTimesheetUnauthoriseSelected uses normalizeTimesheetLifecycleBulkSelections and handleTimesheetLifecycleBulkActionRequest. Actual Gold row dispatcher:94095 callGoldTimesheetLifecycleActionForBulkItem;94155 subrequest preserves expected physical identity and original selected signature;94186 reaches the common single handler. Missing signatures refuse EXPECTED_ROW_SIGNATURE_REQUIRED rather than silently reading a newer signature. Existing operation items persist that expected signature.
- Separate native Bulk row path index.js:96198 also reaches handleTimesheetUnauthorise, but its older subrequest only supplies expected_timesheet_id. It must pass the actual current user-view trusted backend_row_signature. Do not manufacture the token through a fresh worker preflight or use render-only signature.
- Native frontend selected items already carry original expected_row_signature/backend_row_signature/mutation_row_signature: main.js:197495 normalizeItems,197619 existing selected endpoint; runBulkTimesheetSelectionChunks:188... carries unchanged item identities.
- Native Bulk action row already resolves activeBackendRowSignature at264... independently of its renderSignature; use backend identity only. Existing Source view-model narrows canUnauthorise with managed_root/unauthorise_allowed, but it does not dispatch the mutation.
- FE ordinary helper unauthoriseTimesheet:276224 posts same original route at276665. Actual native Overview handler335290 reaches it; physical Timesheet identity can come from Contract Week. Repository caller census found no distinct Contract Week Unauthorise endpoint/RPC. Contract Week without a real physical Timesheet cannot invent one; existing native signature/expected identity remains required.
- FE bindWeeklySourceUnauthoriseRouting:179551 is a separate Simple-only capture listener. It already posts the genuine existing command but has toast/raw-error behavior and no retained original request across unknown outcome. It must not stand as proof that lists/bulk are routed, and a shared dispatch must not double-submit. Card says existing single control unchanged; avoid global native rework, but preserve its original physical/signature identity and truthful current outcome when adding common transport.

## Required finite routing / replay behavior

1. Use current F15 owner-status adapter for actual NEXT qualification; inactive/unavailable owner reports a deliberate refusal. Ordinary legacy source behavior outside this NEXT seam is not an implementation parity programme. Never infer managed ownership from browser flags, Client name or derived list category.
2. Use the installed withdraw-availability/context verdict as the DB managed-root probe. **Only exact WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT can mean ordinary fallback for a fresh request.** available=false / ok=false can mean paid, busy, integrity, unavailable or signature refusal; it is never generic ordinary permission.
3. **Original Source replay must not be lost after the probe becomes NOT_MANAGED.** Final0556:2196ff calls durable withdrawal_recorded_v1(original Timesheet, original signature, original expected Timesheet) before fresh admission/current checks. A successful withdrawal clears the live authorisation, so a later fresh availability probe no longer identifies it as managed. Retain original selected/request Source identity for unknown retries and reach the same installed receipt owner; do not replace signature, reconstruct a new request, infer success from current unauthorised flags, or switch to ordinary ownership. The server's original receipt remains authority. Any proposed replay probe/helper must reuse this native mechanism and be explicitly justified; no generic journal/protocol/table is required by this card.
4. Inject authenticated actor only server-side. Existing request helper accepts exactly actor_user_id,timesheet_id,expected_timesheet_id,expected_row_signature; do not add hours/amount/completeness/origin/bypass flags. Existing command route injects user.id and calls assertNoBrowserFinancialFacts.
5. Preserve native Candidate/family/head/Timesheet locks, permission/busy/epoch/frozen-selection guards. Under-lock original caller signature remains exact. Final0556:2500–2550 independently verifies before/after guard signatures and permits only weekly_source_category_basis to differ before its internal ordinary-owner call. This is the only qualified internal handoff, not permission for Worker refresh-and-resubmit.
6. Map only original definite native results to per-row WITHDRAWN, REFUSED or STALE; keep unknown distinct and recover by original receipt/request. Gold currently drops withdrawal-specific facts when normalizing generic results; minimally retain original Source outcome/refusal/replayed proof in existing result_json and response rather than a new reader/action/queue. Preserve existing ordinary UNAUTHORISED outcomes, selected scope/order/permissions and actual count rules.
7. Do not hide the Source-specific paid dependency behind generic raw-code text or a false success. Read native refusal fields via withdrawalRefusalText; do not invent “paid” from a status/code lacking actual cause. Original native stale/signature check gives REFUSED/STALE classification without silently updating the caller token.

## SP17 accepted derived Withdrawn is a read classification

SP17 reuses0512 operational-empty classification with exact I3 APPLIED, complete known-false duties and no recovery-completion dependency. Its accepted derived WITHDRAWN list category is **not** an imperative first-authorisation withdrawal and does not establish that a Source receipt exists. Do not treat a Stage string as already withdrawn/no-op, reroute it through ordinary ownership, or disable genuine unrelated money actions. Use the actual live generation/managed-root availability and original receipt.

The literal acceptedDerivedWithdrawn flag was not found in current saved broker/src or the inspected frontend; do not invent a new stored/UI boolean to implement this distinction. Its accepted semantics are the retained exact category + I3 proof described in SP-17-i3.md. Re-read the current SP17 final successor at build boundary; no classifier rewrite belongs here.

## Existing SQL safety evidence and limits

Current1332:405–426 locks Candidate control then exact Work, verifies SOURCE/Candidate/Contract/APPROVED, refuses an indexed exact-Work financial_effect EXISTS with BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY, then updates approval_state=WITHDRAWN/current_revision_id=NULL and refreshes the workbench. It never releases/reprices frozen selections or active holds, invents a zero revision/job/debt or mutates Cases. Original financial positions remain retained.

Final0556 preserves exact successful replay and original caller signature, marks the root generation/head pointer together, uses the qualified handoff, persists immutable withdrawal receipt/supersession and retains unrelated Source/invoice/Candidate contracts. No timesheet_financial_retention clear/delete is present in this withdrawal body. Availability is advisory; actual owner rechecks under native locks and may refuse. Full installed body/ACL/state proof is future required verification, not established by this source-only note.

## Required screen design and later A4

Exact supplied design is **SCR139**: C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010/batches/B10/specs/SCR-139_bulk_unauthorise_result.json and png/SCR-139_bulk_unauthorise_result_dark.png / _phone_dark.png. Both supplied PNGs individually inspected read-only. Medium native result window, title Unauthorise result, actual selected-count subtitle, Timesheet / Candidate / Result columns, Done and Refused sections, plain single reason under native status badge, header Close only, no artificial footer primary. Dialog list keeps input order and is sorting-exempt SDP34.

The picture's invented four people/counts/payment-history conclusions/background categories are not permission for new server values or the future SP18 whole-list redesign. Use actual retained selected row human Candidate/Client/week/ref if already present; no UUID labels or post-action live financial inference. Show genuine original withdrawn versus ordinary unauthorised versus native refusal/stale/unknown. Picture-only “Already paid” requires actual financial fact; otherwise truthful owner reason. No new pay date or financial action.

D04 is a real SCREEN deliverable, so **A4 is required after implementation** on native result host dark/device-light/390px phone, adjacent supplied SCR139 PNGs, full42 policy/manual policy.json wording and every difference/control purpose. Minimum states: mixed Source withdrawal + ordinary unauthorised + paid Source refusal; stale signature; unavailable/unknown original outcome and original retry if shown; empty/failed read only where owning native host genuinely provides them. Long result/last row phone should show all outcomes/Close reachable. Source financial/receipt state and ordinary paid WAIT are joined app NOT_RUN until later authorised acceptance; small local fixed-read screenshots prove presentation only.

## Source snapshot hashes

Paths BE-relative except explicitly FE. Current authoritative roots: C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009; C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009. No source edited.

| Saved source | SHA-256 |
| --- | --- |
| BE broker/src/index.js | 77673593D09145179B9855FFAB5563569B8408E7BEDE806F1691F576642CAA19 |
| BE broker/src/weekly-source/routes.js | EC1A819336DAEB552791AF5170433D3925C9FF48E7D8C422DD3B0D92D14E6805 |
| BE broker/src/weekly-source/authorise-routing.mjs | 587F0FF02E2DA0B29E6EF996C0B2EF94FD83CCA49287E9D9A1F3EDC47EF21DE9 |
| BE repeatable04102026_0556 | 64EF7E6D04E1B652AF722CB9DD050310FCD8E1CD249F59B9359428A310B70933 |
| BE repeatable17092026_0600 | 09FBE853B875393170EA2F538EDDD41A0E4BE41AB65CF5CB54CB663EAE2ADA71 |
| BE repeatable10102026_1332 | 9212749C48615FAC296A1AF071AD1B7BB187BFC19C92BCBA5021BF4FDC602391 |
| BE repeatable04102026_0475 | BECDEC3D6892A241DEC27995B1C1DBA592DA1372FB043C911DD11C55F94FCCAE |
| FE js/main.js | 87C66AC085C0A7C73413923C5FB853B841AD7FD4E777BCCA3BDDCF37CA30DD25 |
| FE js/weekly-source-presentation-v1.js | 76CAED36345211C316B1F3CDB420E49F5F0C3FC829FFB62CB5D2EE34998DE74D |

Read-only design findings saved promptly. No new SQL is presently demonstrated necessary; minimal required product is shared Worker routing/finite result adaptation and SCR139 native result consumer, with explicit original-request recovery. Root alone authors product. In-app/financial/installation/timing remain NOT_RUN.
