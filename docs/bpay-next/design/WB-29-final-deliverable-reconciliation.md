# WB-29 final deliverable reconciliation

13 October 2026. Read-only source investigation for sequence 140. The reviewer read the complete WB-29 card (approved `02_WORKBENCH.md:1073–1137`), WB-45 (`:1582–1602`), all working checklist WB-29 D01–D24 and WB-45 rows, R2 connections/review, the current final-definer registry and recorded implementation slices. This document changes no product or checklist. No SQL/RPC, business data, tests, fixtures, hosted calls or screenshots were run.

**Current conclusion:** the recorded foundation and subsequent WB-29 slices now implement the card's named server joins. No additional product owner is justified by the old checklist `MISSING` labels. The final Ready presentation/A4 receipt and final source identities are still being closed by Root and other reviewers. Joined application/queue/business journeys remain **AUTHORED_NOT_RUN**, and performance remains **NOT_RUN**. This reconciliation is neither an app tick nor installation/deployment authority.

The working checklist treats D01–D19 and D21–D24 as retained/shared provenance (`buildTask:false`); D20 is the distinct explicit-need join. These references retain their semantic requirements, even though they are not 23 additional physical implementation tasks. The authoritative full card also requires context, SAVE/CLEAR, complete publication, snooze advisory and accepted quote consumption; those are checked below rather than omitted because the CSV has no separate row for each.

## Source locators and evidence boundaries

All SQL paths below are relative to BUILD-BE `supabase/repeatable/`, except `M` paths under `supabase/migrations/`. Each shorthand names the actual saved physical file, not an imagined owner:

| Key | Current physical source |
|---|---|
| PUB | `12102026_2135_banking_pay_next_treatment_publication_boundary_v1.sql`; clone/payload support `12102026_2135_banking_pay_next_treatment_native_publication_v1.sql`; revision guard `12102026_2135_banking_pay_next_treatment_revision_guard_v1.sql` |
| CAP | `13102026_0005_banking_pay_next_captured_resolution_hold_v1.sql` |
| RATE | `03102026_1510_banking_pay_next_rate_rows_v1.sql`; current row helper `10102026_1009_banking_pay_next_canonical_rate_rows_v1.sql` |
| QUOTE | `06102026_1157_banking_pay_shared_selected_quote_v1.sql` |
| CORE | `12102026_2146_banking_pay_next_treatment_workbench_query_identity_v1.sql` |
| WRAP | `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` |
| W2 | `12102026_2320_banking_pay_next_component_resolution_v1.sql`; prerequisites `12102026_2313_banking_pay_next_resolution_exact_carry_v1.sql` and `12102026_2316_banking_pay_next_resolution_publication_scope_v1.sql` |
| DEC | M `12102026_2044_banking_pay_next_independent_component_resolutions.sql`; M `12102026_2100_banking_pay_next_resolution_remaining_basis.sql`; `12102026_2100_banking_pay_next_resolution_remaining_quote_v1.sql` |
| NEED | `12102026_2146_banking_pay_next_treatment_resolution_need_continuity_v1.sql`, preserving the explicit 2116/2123 producer/target transition chain |
| PIN | `12102026_2227_banking_pay_next_accepted_resolution_pin_v1.sql` and M `12102026_2227_banking_pay_next_accepted_resolution_refs.sql`; current Create/eligible-member successor `13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql` |
| COMPOSE | `12102026_2357_banking_pay_next_captured_resolution_composition_v1.sql`; current seven-argument shared entry/facade `13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql` |
| READ | `13102026_0028_banking_pay_next_frozen_selected_treatment_v1.sql`; current W1 `13102026_0117_banking_pay_next_chosen_display_read_barrier_v1.sql`, retaining `13102026_0055_banking_pay_next_selected_treatment_estimate_v1.sql` |
| READY | `13102026_0113_banking_pay_next_chosen_display_refresh_quote_v1.sql`, `13102026_0132_banking_pay_next_ready_chosen_member_reader_v1.sql`, date repair `13102026_0118_banking_pay_next_chosen_display_date_repair_v1.sql` |
| SNOOZE | `12102026_2344_banking_pay_next_resolution_snooze_warning_v1.sql`, live applicability helper `12102026_2302_banking_pay_next_resolution_live_snooze_v1.sql` |
| CLAIM | `11102026_2259_banking_pay_next_prepare_retry_v1.sql` |

Registry `docs/bpay-next/final-definer-closures.json` supplies the exact signature-to-file mapping. BUILD_LOG:1359–1365 records WB-45 foundation; :1367 onward records WB-29's successive joins; :1391,1393,1395,1397,1399 record captured/advice/estimate/display/final Ready reader closure. Root's sole-local-PG compile/catalogue/source-gate/export-seal claims are attributed to those records, not independently executed by this reviewer. Saved source reviews cover each slice; earlier OPEN paragraphs remain historical where a later receipt closes them.

## D01–D24

`SOURCE BUILT` means the concrete source integration is present and has a recorded source review. `REUSED` means the current earlier owner is retained. Neither means a joined journey ran. All rows' app acceptance is AUTHORED_NOT_RUN.

| Row | Exact object / current physical evidence | Reconciled status and minimum next action |
|---|---|---|
| D01 | `private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid)`, PUB:5; complete clone/native payload assertion in PUB support | **SOURCE BUILT.** Treatment publication retains complete original earnings, native Source-event uniqueness, actual publication/command/member and new physical child IDs. No partial delta publisher remains to build. Retain original source-approval root separately from actual treatment applicability. |
| D02 | `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)`, CAP:5 | **SOURCE BUILT.** Source authority uses positively qualified canonical approval/event/line while RunWork/Position/current/expected/query/lease identities remain actual. The latest E2 amendment governs changed Work after Create: hold back/system release, never substitute/reprice. Historical row wording is not a new F25 project. |
| D03 | `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)`, CAP:430, accepted refs :635; composed quote :686–728 | **SOURCE BUILT.** Accepted quote supplies all Target line/hold/Position/worker operands; unchanged valuer handles no-decision path. Recorder CAP:781–864 consumes the same native prefix composition and retains exact Source/Target conservation. No full-component or guessed proportional replacement. |
| D04 | `private.bpay_next_exact_rate_v1(text)`, RATE:8 | **REUSED.** Exact nonnegative/six-decimal native validation remains. New W1/W2 transports preserve decimal text and validate raw operands before numeric conversion; no parser replacement needed. |
| D05 | `private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric)`, current RATE:5 | **REUSED.** Canonical standard/Additional/mileage rows keep actual code identity and independent PAYE/Umbrella/charge availability; NULL is unavailable. No generic Additional bucket or new rate producer required. |
| D06 | `public.bpay_next_financial_intake_v1(uuid,text,jsonb)`, current `12102026_1646_banking_pay_next_case_channel_resolution_v1.sql` | **REUSED.** Financial intake is not the work-resolution route. Existing actor/receipt/FC family ownership remains; WORK SAVE/CLEAR is W2. Do not add another financial command for this card. |
| D07 | Historical 1610 Source-capture delta; actual CAP:5 and current treatment Source readers/collection successors 2146/2155 | **SOURCE BUILT, shared reference.** The Source qualification port is in current owners, not the old installed 1610 copy. Keep actual captured/current/query/financial guards and canonical original approval qualification separate. |
| D08 | Historical 1620 Source-hold delta; actual CAP:430 | **SOURCE BUILT, shared reference.** Canonical book tuple joins and exact selected Target consumption are integrated; realised/held/book/source scalars and equality guards remain native. No additional Source money rewrite. |
| D09 | `private.pay_finance_selected_quote_v1(public.pay_finance_component_resolution_mode_enum,numeric,text,text,numeric,numeric,numeric,numeric,numeric,boolean)`, QUOTE:10 | **REUSED.** One shared calculation and three native modes. New qualification/composition delegates to it; no cloned converter. |
| D10 | `private.bpay_next_workbench_refresh_work_v1(uuid)`, WRAP; actual classifier CORE:6, need branch :319 | **SOURCE BUILT via retained wrapper/current core.** Explicit need affects only exact taxable unresolved portion; accepted/reserved and priority remain. Applied-pointer/native ORIGIN completion makes resolved work Ready. Source totals remain authorised Source values. |
| D11 | `private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[])`, WRAP | **REUSED.** Existing bounded exact sets/related Case preholds/continuation remain; W2 preholds the finite full explicit scope before publication/refresh. No replacement broad refresh framework. |
| D12 | `public.bpay_next_workbench_command_v1(jsonb)`, W2:324; private resolution transaction W2:8 | **SOURCE BUILT.** SAVE/CLEAR join original typed immutable receipt/replay/readback and ordered Candidate/Work/Source/Case/publication admission. Stale refuses; identical unknown retry retains original bytes. The earlier RETRY/snooze branches are retained. |
| D13 | `POST /api/banking/pay/next/workbench-command`, `broker/src/bpay-next-workbench.js:879–900` | **SOURCE BUILT.** Closed manual intent and result contracts, authenticated actor, original readback/unknown outcome semantics and bounded receipts are present. No route or browser financial calculation to add. |
| D14 | `private.bpay_next_component_resolution`, DEC migration:8–70 | **REUSED WB-45 D05, integrated.** Independent Work/physical/economic/component/family/bucket identity, version, source basis, Target/Umbrella/policy and based-on→published mapping. Current carry guard 2313 preserves exact independent siblings. Not unique Work/component alone. |
| D15 | `private.bpay_next_component_resolution_event`, DEC migration:72–105 and native append-only guards | **REUSED WB-45 D06, integrated.** SAVED/CLEARED immutable audit links and exact EX08 need event/transition authority. No competing store or money event. |
| D16 | `private.bpay_next_component_resolution_active_v1(uuid,uuid,uuid,text,text,text,uuid,boolean,uuid,date)`, DEC:189 | **REUSED WB-45 D07, integrated.** Exact independently compatible mapped decision lookup; Create PIN:9 consumes it once and guards immutable refs. No later ACTIVE substitution at capture. |
| D17 | WB-45 shared residual contract; COMPOSE, CAP and READ | **SOURCE BUILT.** Captured native complete A/R/H/Included/Excluded membership and exact bucket prefix conservation feed unchanged QUOTE. Native ambiguous residual attribution refuses typed. Current selected estimate uses same core/facade; frozen detail/advice use original refs. No separate money kernel needed. |
| D18 | Historical 1530 tax-assignment reference; `12102026_1419_banking_pay_next_ordinary_umbrella_components_v1.sql` | **REUSED DR-38.** Additional taxable; expenses/mileage fixed non-taxable reimbursements, no ERNI conversion. Current quote/estimate retains fixed Source ex and native VAT. Do not reopen that producer. |
| D19 | Canonical arbitrary Additional rate-row support; current RATE | **REUSED WB-01.** Actual arbitrary native Additional identities survive independently; standard bucket rates, used approval evidence, available rates and charge evidence stay distinct. Source-unbound physical evidence is a typed individual refusal, not invented authority. |
| D20 | EX08 finite event/scope → NEED:6/:50–53 → CORE:319; 2116/2123 event and target transitions | **SOURCE BUILT.** Explicit channel/Umbrella event and current own-treatment mapping produce RATE_/GROSS_RESOLUTION_REQUIRED and CHANNEL_RESOLUTION_REQUIRED; reimbursement none. A→B→A/U1→U2 changes deterministically replace/clear scope. Compatible ordinary/Source/move mismatch alone is resolver Ready. Final Ready presentation receipt is the remaining close-out, not a new need producer. |
| D21 | Accepted Create/capture timing reference; PIN and current eligible Create 0016:97, CAP/COMPOSE | **SOURCE BUILT under current Rule B/E2.** Actual expected Work revision, captured Candidate/target/policy/date and exact accepted refs govern. Live money remains under original locks. Explicit [] prevents later decision adoption; no parked2153 table/resolver. |
| D22 | Output-only reviewed WB-29 card | **Specification reference.** Full-card obligations are mapped here; historical plan review alone is not construction, installation or acceptance. No separate product object. |
| D23 | `createBpayNextFinancialCommandRpc`, `broker/src/bpay-next-command-rpc.js` | **REUSED WB-19 transport.** The resolution command uses its original W2 adapter/route with action-specific closed receipt. Do not treat old local provenance as current missing code; full runtime/minute/business verification is still NOT_RUN. |
| D24 | `private.bpay_next_claim_prepare_job_v1(uuid,integer)`, CLAIM:1045; earlier-command barrier :1101–1104 and captured selection target :1114–1123 | **REUSED current PF08/Rule B owner.** Earlier Candidate command and exact active execution/epoch/lease fences remain. Accepted target comes from original Run/worker boundary rather than delayed current Candidate state. Do not install F25's parked protocol to satisfy the historical note. |

## Full-card joins beyond the duplicated rows

| Obligation | Concrete current evidence / remaining boundary |
|---|---|
| W1 available/used/suggested/saved context, genuine typed zero and reasons | Current public W1 READ retains RESOLUTION_CONTEXT; private context 2302 replaces only live snooze qualification on 2241's native closed request/current-authority context. Decimal text, native standard/arbitrary identity and mode/NULL semantics are source reviewed. |
| SAVE complete publication and exact independent carry | W2/2313/2316; all requested Works staged completely before publication; native Source UNBOUND proof with exact full R/H witnesses precedes pointer change, READY book retained. Exact expected ID/version, stale refusal and immutable replay are present. No accepted treatment changes original earned pay/debt. |
| CLEAR future treatment only | W2 exact WORK owner IDs/versions; append CLEARED then publish compatible complete treatment and preserve unrelated siblings. Finance clear remains the FC-owned route. No blanket work clear or rate-case fallback. |
| Warning before opening snooze | SNOOZE:169 plus Worker `bpay-next-snooze.js` and existing FE `bpay-next-snooze-window.js` expose card's saved-rate advisory/local date consent. SNOOZE_CLEAR does not erase decision. Saved screen/source receipt remains separate from app acceptance. |
| Accepted attachment, residual and all consumers | PIN and 0016 exact original LEAF admission; COMPOSE once native capture; CAP selected hold/origin-share integration; READ frozen RATE_DETAILS/original advice; current selected estimate 0055 retained by 0117. Full positive and zero scopes require original accepted identity. |
| Owner F chosen Ready amounts and server Amount sort | READY keeps authorised Candidate/header totals, chosen eligible WORK group/component/leaf ex amounts, real rate-set channel badge, original accepted/reserved/Case amounts and indexed group sort. Date sensitivity/read barrier/native two-lane rollover repairs via existing ORIGIN. Final A4/source-identity close-out remains in progress; no broader summary rewrite. |
| Optional linked Timesheet set / later screens | W2 accepts a disclosed explicit finite multi-Work set; it never discovers hidden Candidate scope. WB-30 owns linked-set disclosure/exclusions/confirmation; WB-30–34 own rates windows and adoption presentation. Do not build their screens early under WB-29. WB-45 full screen assertions likewise remain attached to those named children. |
| CM-11 receipt precedence | It is a later named Contract-move consumer seam, not authority to invent a CM receipt now. Approved IMPLEMENTATION_SEQUENCE.md:272/280 places CM-11 at265 and CM-19 at273, after WB-29:140. Current active-decision/no-decision resolver paths are built; CM-19 later preserves explicit ACTIVE → exact CM receipt → shared resolver precedence. No future move producer/selection rewiring is required to close current WB-29 work. |

## Minimum close-out

1. Finish the ongoing final Ready A4 review and record the final FE/BE source hashes and local commit identities in the same slice evidence. The latest BUILD_LOG:1399 records 0132 local compile/catalogue/gates as Root evidence and explicitly leaves screen/final FE commit pending.
2. Reconcile the working checklist statuses with these current sources and WB-45's referenced runtime joins when Root closes the item. This reviewer does not change those ticks. Preserve later WB-30–34/CM ownership and genuine typed evidence refusals; do not create another product batch merely to erase old provenance labels.
3. Keep every full card Done-means journey—including manual GBP17 versus suggestion, DAY/SUN independence, fixed mileage, mapped reopen, partial residual, queued-before/after-Create and whole Worker/minute path—**AUTHORED_NOT_RUN**. Root's compile/catalogue checks and local fixed-response pixels prove neither economic first use nor hosted application timing.

This is a source reconciliation: no newly identified essential product gap beyond the current close-out, and no claim that WB-45's later windows or future Contract-move owners are implemented. No new store, producer, command, route, trigger, proof framework, calculator or acceptance programme is recommended.

## Saved-input identities

The final focused reread confirms W2:33–43 retains closed SAVE/CLEAR/zero intent, W2:310–317 performs native refresh before original immutable receipt, RATE:44–62 retains arbitrary Additional codes and honest shared native Additional pay, and READ:113–129/:389–405 attaches only exact captured refs. These are source observations, not executable checks.

| Saved input | SHA-256 at this reconciliation |
|---|---|
| `docs/bpay-next/final-definer-closures.json` | `444E17CED7038F1CBE097E45306A3F92D06595DC4B9CBF2BC8927F5977B78C72` |
| Working `docs/bpay-next/IMPLEMENTATION_CHECKLIST.csv` | `93C5A655207860C899DD52FFB837308A719F728DD89B04745925DA56A222BE12` |

Later Root changes require their own final identities. This checkpoint intentionally leaves ongoing pixels/commit close-out distinct from these saved-input hashes.
