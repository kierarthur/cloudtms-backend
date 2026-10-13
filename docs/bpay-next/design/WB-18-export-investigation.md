# WB-18 export investigation — 10 October 2026

**Read-only scope investigation complete; CSV route and actual controls are missing. Existing paged EXPORT_ROWS is reusable but not the completed legacy CSV contract.** No product edit, test, SQL/DB/runtime/business call, browser or network was made.

## Exact scope

Read full card `02_WORKBENCH.md:727–746`, all five WB18 checklist rows, R2-F06/R2 WB18:212–214, master sequence66/dependencies WB09/WB21/F16, saved WB11 owner-screen constraints, WB21 choice-cut contract, and current reader/transport/store. D01 public Workbench service and D04 reviewed card are CONTEXT; required D02 is exact `POST /api/banking/pay/next/workbench-export`, D03 is actual header and Candidate CSV controls. No additional financial owner/producer/card is assigned.

The card explicitly requires: whole filter scope with **unticked rows retained**, one Included column and the exact19legacy columns, all sections in scope, Candidate export restricted to its exact Candidate, bounded server pages, same native list ordering, no bank fields, no browser monetary calculations, and explicit failure if export cannot complete. A correct empty export may have only the header; a partial export must never be offered as a complete successful file.

## Current source and confirmed gaps

- Registry final common `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` is2105; public `public.bpay_next_workbench_page_v1(jsonb)` remains1946. Preserve all WB16 detail and WB17 identity branches.
- Current2105 EXPORT_ROWS:409–448 returns finite maintained item identities, names, dates, classifications, decimal SOURCE values and revision fields; <=100 raw UUID identities,110000SQL byte envelope. Scope predicate183–195 supports Candidate/Client/channel/section/search. It currently sorts by `i.id`, not the visible Candidate/group list sort, and has no immutable complete choice descriptor or Included result.
- Current Worker typed EXPORT_ROWS contains42fields (300–342), including candidate/work/case/component/revision/member/portion IDs and native decimal strings. It omits `approved_line_id`, `approved_segment_id`, physical Timesheet ID/version, role/band and shift/break/detail display evidence necessary for exact legacy mapping. No CSV route exists in currentindex; legacy-fence162–178 does not exempt workbench-export. Existing GET workbench-page authenticates admin, binds context/cursor/actor and has32KiB request,128KiB reply,4second deadline; fixed RPC is in `bpay-next-workbench.js:522+`. Reuse the existing fixed RPC and validated page domain instead of invoking an old Banking export route or browser service RPC.
- Current FE shell actions have Filters/Refresh only (`bpay-next-workbench.js:617`); actual Candidate detail likewise has no Export control. Continuous native list/cache/selection state already exists and must survive this output action.

## Verified19column map

Read the actual published Git object `origin/main:js/main.js:221650` using local Git, not a network fetch. Exact headers are below. Published source reference is `887e7af7cd15d9f4034ad73806b44344f3eb0791`. Legacy body221551–221647 expands segments/recovery components and formats numbers in browser; retain its column names, not its old aggregation, allRows arrays or bank-engine execution.

| CSV column | Current native evidence / necessary narrow addition |
|---|---|
| Included | One immutable complete choice descriptor plus current native eligible/portion status; not just Candidate tick or nominal amount. Must evaluate all Candidate/Work/component/Case overrides, including NONE and subset, against exact IDs/revisions. |
| Line type | Native `section`, `component_kind`, Case/direction/stage and reason provide finite labels. Document mapping rather than copying old READY engine names. |
| Breakdown type | Actual `member_kind`/`portion_kind`: native Work segment/component, Case component or informational evidence. No invented shifts. |
| Candidate ref | `candidate_reference`. |
| Candidate | `candidate_display_name`. |
| Client | `client_display_name`. |
| Week / Date | Exact `work_date` else the native `week_ending_date`; date-only UK display, not UTC conversion. |
| Channel | Exact current `target_pay_channel`; preserve source channel only when clearly labelled source evidence, no guessed channel fallback. |
| Amount | Exact server-owned decimal amount appropriate to this actual member/portion and classification. Preserve NULL unknown; no zero substitution, browser sum, gross-to-net ratio or redistribution. See amount caution below. |
| Timesheet ID | Exact row revision physical Timesheet ID via bounded PK join; no booking-first/original fallback. Case-only blank if none. |
| Economic key type | Finite native Work/Case/component/segment identity kind, documented mapping; no manufactured legacy key. |
| Economic key value | Native actual component/member/segment or Case-component identity corresponding to that kind. |
| Role | Exact immutable revision `job_title`, after bounded seed. |
| Band | Exact immutable revision `band_label`. |
| Start | Actual approved segment→shift detail local clock or captured timestamp formatted London; no clock inference for Source local-only evidence. |
| Finish | Same exact shift detail authority, preserve overnight semantics. |
| Break | No scalar exists on current item/shift row. Genuine break rows are retained separately. Blank with an explicit deviation is permitted by the card if no bounded exact equivalent is projected; do not infer break as start/end minus worked hours. An exact bounded retained-break description is also possible, but must preserve row/time/byte limits. |
| Detail | Exact shift `detail_label`, approved-line `evidence_ref`, or genuine Case/component display label. Missing remains blank; UUID may be an explicit identity, not a fabricated description. |
| Preview row ID | Native `item_id`, documented name-equivalence deviation; there is no old NEXT preview-session row. |

Required factual enrichment can follow an existing MATERIALIZED item seed and PK joins to its exact approved line/revision/approved segment/shift detail, with composite Work/Candidate/revision/line/key consistency. Use <=100identities before details and preserve the byte envelope. Ordinary fields live in1324revision/line/shift schema; Workbench item already retains approved_line_id/approved_segment_id but its current export projection omits them. No native money/writer modification or public trigger is necessary. Existing item PK,1252 Candidate/scope/Work/Case indexes support present UUID paging; if list-equivalent Candidate/group sort is added, review its actual equality+seek/order and add only the demonstrably missing reader index. Do not claim existingUUID ordering already satisfies list order.

## Frozen complete intent and Included semantics

Current `createChoiceStore` (`bpay-next-workbench.js:726–759`) has the necessary finite local source:

- namespace: agency_id, owner_epoch, actor_user_id, official_pay_date, Candidate/Client/channel scope;
- intent: ALL_MATCHING or NONE plus complete Candidate overrides; Work overrides `(candidate_id,work_id,expected_revision_id,selected,mode ALL/SUBSET)`; component overrides `(candidate_id,work_id,expected_revision_id,approved_line_id,selected)`; Case overrides `(candidate_id,case_component_id,selected)`;
- local intent revision plus exact workbench_revision/adoption_revision;
- <=32KiB complete stored descriptor and <=1000entries perkind, no money/bank data.

Use `store.freeze(context)` once at the click, with exact initiating actor/owner/window/scope and coherent server context. `countIntent()` alone loses all Work/component/Case choices and cannot satisfyWB18. Keep the same descriptor immutable for all export pages. Candidate export narrows enumeration to its exact candidate_id but preserves that Candidate's choices in the same parent namespace; do not falsely retag the store's namespace or modify choices merely to export. Reject malformed/out-of-scope/duplicate/incompatible revision override IDs finitely rather than silently ignore them.

Included is descriptor intent **and** the actual member's native eligibility/current status. WAITING/RESERVED/InDraft evidence or unknown/nonselectable rows cannot become new Included earnings merely because its Candidate defaults selected. Positive payable Candidate eligibility and deductions-only E6 scope use existing native current facts; standalone all-deduction Candidate is not a new selectable payable Candidate. Do not count an informational GROUP_EVIDENCE zero as an economic payment. Parent Candidate/Work deselection takes priority, exact SUBSET line membership and Case-component selection remain distinct. The output can derive Included booleans from validated identities/intent in a finite SQL projection or finite Worker selector; this is selection presentation, never authority to calculate or modify money. No CREATE/enqueue/PREPARE command/run is needed for read-only export.

Amount must not turn every positive nominal value into today's payable sum. Current rows distinguish approved, included, reserved, deduction balance and residual; one member may have independent Available/Waiting/Reserved portions. Preserve real direction/stage and use a single owned amount per exported native portion without duplicating parent and segment money. Waiting/informational/unknown facts may legitimately have NULLamount. Exact CSV Included-total=headline acceptance requires the existing server-owned selectable projection and eventual selected-estimate ownership; do not implement a browser total or promote unsigned Case remaining balance to a concrete deduction. If a column has no exact equivalent now, explicitly document the deviation as the card permits, rather than inventing financial semantics.

## Minimum route/stream/UI implementation

1. Add only exact POST workbench-export route under current Office admin/environment/NEXT authority and exact legacy-fence exemption; normalize the path identically to the router. Keep other methods/routes fenced. Preserve TEST/LIVE environmentheader behavior and config-only gates.
2. Body carries one <=32KiB closed immutable scope/context/complete-intent descriptor, IDs/versions/booleans only, no bank or browser money. Header export explicitly enumerates allsections in filter scope (not only the currently loaded threepages); Candidate export enumerates only its exact Candidate across its required sections. Decide/record native list-equivalent server ordering; do not sort a whole browser CSV row array.
3. Worker walks only EXPORT_ROWS <=100pages, with existing perpage validated context/actor/schema/cursor/byte/deadline. Bind allpages to first exact agency/owner/date/workbench/adoption/scope/intent and native key. Refuse stale/repeated/nonadvancing cursor, empty nonterminal page, false complete, malformed reply or finite total byte/page/deadline limit. A changed context means whole export failure, not retry with a newer context midfile. Stream backpressure/cancellation must stop reads.
4. CSV formatting is finite presentation over exact strings: escape comma/quote/CRLF safely; preserve all19headers in order, UK date-only values and exact decimal text; prevent spreadsheet formula interpretation for untrusted text without changing true negative monetary values. No bank field or raw payload/log. Use explicit bounded output/total limits sufficient for ordinary1000row acceptance; crossing a limit returns/fails explicitly, not truncates complete=true.
5. On a preflight failure return typed HTTP refusal. On a later page failure error/abort the stream, not append a faux complete CSV. FE fetches/reads only the bounded CSV byte stream and offers the actual Blob/download only after successful fullbody completion. Never offer an anchor for partialbytes, direct early attachment download or apparently successful partialEOF. Keep partial chunks local/discard on failure. No monetary parsing/sum inbrowser.
6. Add Export CSV to the real shell header and Candidate native window. Capture immutable context/intent once; keep parent/store/ticks/sort/filter/page/scroll untouched. Visible pending, finite typed error and explicit Tryagain use the same output scope for that attempt; guard actor/owner/managed-frame and late promise/close tails. Disable stale export until existing explicit Refresh; no automatic list refresh or mutation. Export is the card-authorized finite page walk; it does not change continuous list scrolling into page/load-more buttons.

Owner screen constraints reused: dark Office chrome, native stackedwindow keyboard/phone behavior, server sorting on actual displayed data, continuous list bounds, Candidate InDraft onlyOpenDraft for payment actions, and no extra pay-date input. Export is not Create or another frozen payment viewer. Header export and Candidate label must truthfully describe their exact actual scope.

## Later joins remain separate

WB19 explicit refresh/progress; WB22storage refinements; WB23bulk/mixed unticks; WB24complete child membership; WB25snooze and WB27exact estimate/resolution ownership remain ordered cards. WB18 must support already-retained complete choice domains now, including domains their later UI will exercise, but cannot manufacture new deduction allocations/rates or rewrite those owners to make an export total. RuleB/acceptedquerysnapshot/currentWorkchoices remain unchanged; parkedF25, enroller, Source/pay-queryproducers, settlement and bankCSVowners stay out ofscope. Runtime/screenshots/1000row app journey remain NOT_RUN here.

## Current raw source SHA-256

| Source | Raw SHA-256 |
|---|---|
| BUILD-BE/supabase/repeatable/10102026_2105_banking_pay_next_exact_work_navigation_v1.sql | `dad87c105db7daa58b4b62433812d13bcab89aa86481a158e012791777d2c12e` |
| BUILD-BE/supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql | `c050f51fb7f132626bd479c87de127f78a41029ee16044d048d39af2e94b4e37` |
| BUILD-BE/supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql | `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab` |
| BUILD-BE/broker/src/bpay-next-workbench.js | `bc063d8205be961e18703dfa6d85dbc431a176f4bde7b37bcb484b4d49992232` |
| BUILD-BE/broker/src/bpay-next-legacy-fence.js | `3a3a467e70b35da1367376fa51909b098ba2160dda003e19a7b2740bfd3e0db0` |
| BUILD-FE/js/bpay-next-workbench.js | `aa81371c4a34486221215b2faff20a21289ef691ee80e5c4e791f65fdd8ba227` |

## SQL descriptor / eligibility supplement — exact current owners

Read current1946 public service body1063–1195, current2105 common export, native1252item schema67–158, Case component0300:68+, and final1332group contribution. This supplement refines the minimum supporting reader change; it is not a new accepted finance protocol.

### Own closed argument seam

The public service currently normalizes `count_selection` to JSON null and admits exactly14request keys (1946:1079–1084): actor_user_id,kind,candidate_id,client_id,channel,section,search,sort,group_key,item_id,reason_code,after,limit,count_selection. `v_scope=p_request - ['actor_user_id','kind','after','limit']` at1151 becomes exact cursor-context scope. Current public signature remains `(jsonb)`; no new service RPC or command is needed.

Recommended additive optional key `export_choice`, accepted only for EXPORT_ROWS; normalize NULL for other kinds but keep their actual old cursor shape unchanged. EXPORT_ROWS validates one closed immutable namespace/intent/localRevision/workbenchRevision/adoptionRevision descriptor from existing store.freeze. Compare actual service-auth actor, agency, NEXT epoch, official date and global workbench/adoption revisions; compare Client/channel exactly with parent namespace; Candidate export may narrow to its exact real Candidate under the existing parent filter, without changing namespace/store. Keep section ALL for whole-scope/Candidate export and explicit native sort/source-list discriminator if needed. `count_selection` remains HEADER-only and must not be repurposed to hide missing Work/Case choices.

**Request-bound trap:** embedding the entire up-to32KiB descriptor in v_scope would duplicate it in every next request (export_choice plus cursor.context.scope), exceeding current32768body bounds. Keep complete validated export_choice as the common reader's local input, while cursor scope carries a deterministic digest/finite identity of that exact choice instead of its full text. Compute from canonical server JSON, not an untrusted caller hash. Omit the full descriptor from serialized scope; include its fixed identity in context and compare each subsequent descriptor before read. Alternatively a larger explicitly EXPORT-only finite envelope is possible, but it must not silently enlarge all ten read kinds. The entire initial request still must fit its declared bound; maximum local-store32KiB is not a promise every near-limit descriptor plus request/cursor overhead fits. Refuse clearly before streaming if it does not.

Common signature need not change: current p_scope JSON can carry the validated export-only choice locally, while public response context exposes only original filter fields plus its exact export identity. Worker/FE export validators must use that exact closed EXPORT context; existing GET/list contexts remain untouched. Original module/admin/adoption-current/dirty counters,4second Worker page bound/5second SQLstatement and cursor version refusal continue to apply. No row locks, producer changes, metadata writes or accepted-run intake are required.

### Native list-order seam

Current public1946:1103–1107 allows only NAME_ASC for EXPORT_ROWS; current common uses UUID `(i.id)` regardless NAME. Thus a literal mode named NAME_ASC is not evidence of list-equivalent order. An own EXPORT_ROWS sort branch must use the actual server primary tuple for its initiating source list and append native itemUUID once to make it unique. Header Candidate list tuples already exist: NAME `(candidate_sort_name C, tms_ref-null, tms_ref C, candidateUUID)`; AMOUNT `(unknown flag,native exact amount,candidateUUID)`; DEDUCTIONS and STATUS five-part native tuples (2105:91–103). Candidate Banking group tuple exists separately: Payment `(date-null,date,presentation_kind C,presentationUUID)`, Client, Channel, Deductions, Amount and Reason (168–179). Preserve those exact null flags/C collation/date/decimal meanings and direction; no JavaScript sorting of the whole file.

Header enumeration still includes all scoped sections/Candidates, and Candidate enumeration includes exact native Work/Case/ROW groups across sections; do not accidentally reuse onlyREADY list WHERE to drop unticked/Waiting evidence. If a list's displayed sort key comes from its READY summary while export includes nonREADY rows, document the exact native source used for absent/list-invisible groups and retain all rows, rather than claiming an unsupported identical ordering. Use its existing maintained summary/display PK before exact itemUUID seek; add only a needed equality+seek index. Current1252 indexes include itemCandidate/section/group/member tuples, itemScope `(client_id,target_pay_channel,section,candidate_id,id)`, Work/component/segment and Case/component. They do not establish arbitrary NAME/AMOUNT ordering by themselves.

### Included hierarchy — no implicit re-enabling

Evaluate the complete descriptor against native identities, after a bounded item seed and exact scoped PK joins:

1. Global mode ALL_MATCHING inherits selected; NONE inherits unselected. Exact Candidate override changes that Candidate's base intent. An explicit Candidate false is terminal for every child; an individual child true does not secretly re-enable it.
2. For Work, exact `(candidate_id,work_id,expected_revision_id)` override must match the native Work/current item revision. Work false is terminal. Mode ALL inherits every actual line except explicit false component overrides; mode SUBSET includes only explicitly selected actual approved-line IDs. Component override must bind Candidate/Work/expected revision/line composite identity and cannot re-enable a false Work/Candidate. Row's segment shares follow their actual approved_line_id; no segment-money redistribution or new segment intent kind.
3. Case override uses actual `(candidate_id,case_component_id)` and cannot re-enable a false Candidate. The native Case component has positive `component_revision` (0300:74), but **current store's Case override has no version field** (only Candidate/Case-componentID/selected). Do not manufacture one. Existing exact global workbench/adoption context binds the read cut; if a later actual UI supplies an expected component revision, validate that real finite field against the native PK row. No fake revision constant or current-version retag on retry.
4. Included requires actual native sectionREADY, selectable, amount_known, portion AVAILABLE/ZERO and a real Work/Case economic member. Group evidence, WITHDRAWN/RECOVERY informational facts, ACTION/BLOCKED/WAITING/RESERVED or already-InDraft rows are not included merely through ALL_MATCHING.
5. Use native eligible Candidate fact `payable_count>0` in the actual READY client/channel scope, not `amount>0` guessed from gross money. That enforces E6 deductions-only Candidate suppression without rejecting native certified-zero/internal-zero semantics by an invented positive-money rule. If local overrides exclude all a Candidate's eligible payment members, its deductions cannot become an independent payable Candidate. A bounded indexed EXISTS over actual READY PAYMENT members with the same hierarchy can establish that remaining intent; do not infer it from only this exportpage or build an allagency ID array.
6. A stale Work/line/Case override scope is an explicit stale/input refusal before apparent completion, not silently ignored. Finite descriptor arrays permit bounded checks; absent currently rendered pages do not remove valid stored choices. Validate all relevant Work/component/Case choices, not only the first100emittedrows.

Included describes actual retained local choice plus current eligibility; it is not an executed/recovery allocation, accepted finance command, reservation or paid receipt.

### Truthful Amount / factual19columns

Actual final1332group contribution sums READY PAYMENT/GROSS_ADD `included_source_ex_vat` into header amount; NET_ADD, gross/net deduction balance and already-selected/reserved source values are separately retained. Current thin shell labels Ready amount and local selection counts; it has no new exact local-intent payroll deduction estimate. Case classifier1252:520–526 stores deduction nominal as available outstanding balance, not this-Draft allocated recovery. Therefore it is invalid to negate the entire Case balance into an Included payroll deduction and claim this equals the current headline.

For **factual current-row Amount** the already-reviewed WB15 display_value is the narrow existing semantic mapping (1946:34–40): unknown if amount_known false (or accepted is_selected with selected_amount_known false); actual selected_source_ex_vat if native is_selected; actual active_reserved_source_ex_vat if RESERVED; current READY PAYMENT included_source_ex_vat; WAITING PAYMENT/GROSS_ADD gross_due_source_ex_vat; else actual nominal_source_ex_vat. Direction may supply the existing display sign for a factual debt/balance label, but never converts a balance to a concrete payroll deduction. Do not aggregate parent and segment rows or add GROSS and NET as if one economic basis. A recovery row remains information if native direction is INFORMATIONAL. Unknown Amount stays empty, never0.

If the product intends Amount to mean *this chosen Draft payroll result* instead, exact allocated deduction/NET authority is not available from the current reader and must remain unavailable until its named estimate ownerWB27; do not extend a money owner here. The card permits documented columns without NEXT-equivalent; use precise Line type/Breakdown labels and record that deviation rather than fake financial parity. Ordinary Work-only1000row/3untick export can match its current native SOURCE gross facts. Mixed-stage/debt total acceptance remains a real later joined journey until a server-owned selected-total with matching basis exists, not a browser calculation or blanket exported-total claim.

Minimum new PK facts for legacy19columns: item approved_line_id/approved_segment_id/segment_key; exact revision physicalTimesheetID/job_title/band_label; exact approved segment→shift_detail_id under revision/line/component/key; shift_start_local/end_local or its paired capturedtimestamps, detail_label. Existing itemnames/date/channel/nativeIDs/money classifications cover the other columns. Native Case component provides case_kind/case_subtype/instruction_kind/direction/payroll_stage/tax/ordinal/revision, with a finite native label reused from WB15. It has no generic description string to invent. Its schema is0300, not a guessed legacycase JSON.

Break detail schema1324:281–298 has actual separate start/end/local clocks and break_minutes peractual shift, not a scalar on item. Retained break text can be an exact bounded factual child projection with completeness checks; blank plus disclosed missing equivalent is the smaller permitted mapping. No subtraction of approvedhours from duration, no rate or pay revaluation. Start/Finish for genuine nonshift components remain blank. Preview rowID maps native itemUUID, documented; TimesheetID uses the exact item's retained revision physicalidentity for CSVevidence, not the mutable current navigation pointer introduced byWB17.

All row packing remains after native seed and within current byte bound. These factualjoins and finite Included/descriptor logic are genuine WB18 reader support; Work/Case financial writers, Source producers, RuleBchoices/pins and all parkedF25 remain untouched.
