# DR-17 current build investigation

12 October 2026, sequence123, source-only findings. Read full card03:449–471, every current DR-17 ITEM/D01–D08 checklist row, R3:156–162, controlling R2:11–13/84/146/158/330–336, master08:207 and current final-definer registry. No product/checklist edits, SQL/database/business calls, tests, fixtures, hosted actions or captures. All future joined/queue/timing acceptance NOT_RUN. Root alone writes product.

## Exact owning boundary

D01 is the existing public frozen-detail reader; D02 is the unconditional immutable-effect guard and four existing attachments; D06/D07 are the native review/detail controls. D03 capture-position, D04 ordinary publisher, D05 fresh-hold and D08 specification are **context-only/buildTask:false**. They retain financial/cutoff semantics; this card does not assign another publisher/capture/hold rewrite. Policy X/OR44/Rule B preserve original accepted selection/revision, immutable details/rates/labels and downstream frozen amounts. Existing E2 later-work failure/system-release behavior must not be removed merely to restate older OR44 evidence. No parked2153 objects, new freeze store, calculator or producer protocol.

DR31's beneficiaries tree is a later item (master sequence158). Publish/reuse the existing detail launcher seam now; do not build that tree early. Historical/paid/cancelled detail remains the separate state-independent History/SP22 owner, not a reason to broaden Draft reader guards.

## Current registered owners

| Exact signature | Current physical owner / useful anchor |
|---|---|
| `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)` | `11102026_1005_banking_pay_next_frozen_client_origin_v1.sql`:84–196 |
| `private.bpay_next_payment_client_origin_v1(uuid)` | same1005:5–80; immutable revision labels/Daily authority, no current Client lookup |
| `private.bpay_next_simple_frozen_line_guard_v1(uuid)` | `04102026_0025_banking_pay_next_frozen_detail_pages_v1.sql`:7–35 |
| `private.bpay_next_simple_shift_page_v2(uuid,integer,integer)` | `04102026_1134_bpay_next_frozen_shift_page_v2.sql` |
| `private.bpay_next_simple_break_page_v1(uuid,uuid,integer,integer)` | 0025:71–109 |
| `private.bpay_next_simple_rate_detail_page_v1(uuid,text,integer)` | 0025:111–137 |
| `private.bpay_next_simple_rate_schedule_page_v1(uuid,text,text,integer)` | 0025:141–182 |
| `private.bpay_next_effect_immutable_v1()` | `03102026_1342_banking_pay_next_guards_v1.sql`:188–198 |
| `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` / `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `12102026_0005_banking_pay_next_source_tax_classification_v1.sql`:732 /1140 |
| `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql` |
| `private.bpay_next_publish_ordinary_from_financial_v1(uuid)` | `11102026_1001_banking_pay_next_daily_frozen_approval_context_v1.sql` |
| `private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric)` | `10102026_1009_banking_pay_next_canonical_rate_rows_v1.sql`:5–75 |

Current DR13 observations/reader are0552; they do not own these Work detail pages. Native LINES remains the existing preparation reader/route. The public frozen service preserves service/admin/NEXT checks, closed four kinds, exact required identity, 1–100 rows, byte-short continuation and owner-only/browser-denied ACL (1005:104–155/170–196). Draft guards require parent REVIEW/DRAFT and Worker READY (0025:21–32/162–171). Available schedule is read through `run_work.captured_revision_id` (0025:162–177); shifts/breaks/used standards through the immutable `run_line.approved_line_id` guard. No current Timesheet/Contract pricing is involved.

## Captured data already present

The capture/hold owners bind approved-line/physical-revision identities; they do not need to copy entire detail tables again. Source capture0005:828–850 follows the selected native run-work revision and exact approved line; run_position1042–1058 and run_line1385–1398 retain those identities, source consumed and frozen target money. Its version/freshness refusal is existing E2 admission, never a new-rate fallback.

Ordinary publisher1001:301–342 freezes revision labels and canonical available schedule, including independent PAYE/Umbrella/charge and actual approved pay/charge rates. Weekly uses current canonical helper1009. Contracted and contractless genuine Daily consume frozen Daily approval evidence/settings/date and merge native nonstandard identities (1001:220–259), then persist the Daily evidence and rows. Additional used-only code with unknown availability is deliberately stored as all-NULL pay/charge availability (269–274). Actual Additional quantity/unit/rate is frozen on its approved line (700–709); mileage likewise743–745. Used standard detail is frozen per approved line (433–441/544–552). Source/protected current0005 freezes typed revision `source_kind`, labels and schedule (374–400), immutable worked shift/break facts and actual used standard rate/quantity (513–620). This is existing evidence; absent authority stays NULL. No new calculator or producer edit is justified for DR17.

Canonical1009 returns **all five standards**, arbitrary Additional codes up to245 characters, and the native mileage row even when availability is all-NULL (29–42/50–74). The three rate-channel fields remain separate. The reader must consume this authority rather than use old1510's all-NULL filtering or infer availability from used-rate evidence.

F06's current0718:42–60 attaches the same unconditionally rejecting1342 guard BEFORE UPDATE/DELETE and BEFORE TRUNCATE to each of `run_work` and `run_line`—the four required attachments, no whitelist/depth exception. There is no source reason to edit/recreate that guard for D02. Root can verify its installed catalogue under the authorised release checks; this investigator has not queried a database.

## Existing native surface and real remaining gaps

FE Office already exports `openFrozenWorkerDetail(options,workerId)` (1700/2747). Native review READY row offers Draft detail and row/phone open (2156–2168). The detail window already has Lines, Shifts, Breaks, Used rates and Available rates (1762), exact original line → shifts → break identity, available schedule via captured run-work, bounded three-page window, preserved scroll/focus, explicit missing/error state and a native frame (1714–1785). DY09/DY05 Client/No Contract display and original summary facts are reused. Keep actor/NEXT/native frame checks and honest no-detail states. It is substantial current implementation, not only the obsolete thin Sheet at2625–2630.

Concrete required corrections/support:

1. **Legitimate available-rate rows are rejected today.** Worker `broker/src/bpay-next-frozen-read.js`:157–158 caps arbitrary rate code at128, while canonical1009 permits245. FE Office498 has the same128 ceiling. Worker306 and FE886 reject all-NULL non-STANDARD availability, contradicting valid native mileage and used-only Additional observations above. Correct only these existing finite validators/cursor admission to the native authority; do not fabricate rates or republish Work.
2. **Used-rate display omits owned stored detail.** The current frozen RATE_DETAILS reader0025:130–135/service1005:152–154/DTO Worker89–92 expose only five-standard bucket, approved hours and source pay rate. Native Office1770 displays exactly those three columns. The card requires actual used quantity/rate per line, while nonstandard actual unit/quantity/rate is already on immutable approved_line. Frozen approved charge is also available for ordinary at its exact schedule row1001:323–339. If used-rate/charge detail is shown, extend the same named frozen reader by exact approved-line/revision/schedule points with truthful nullable fields; preserve current request identities/cursors and expose only genuine stored facts. Do not infer Source charge or compute a rate from money/quantity. No “Rate set by” metadata exists in this contract.
3. **Typed Source label is not exposed at the current detail boundary.** Revision0005:375/382 already stores SOURCE/PROTECTED. Public origin1005:66–80 and its closed Worker72/FE340 DTO omit `source_kind`; LINES DTO only has component_kind and maps WORK to Timesheet (Office1765–1766), so SOURCE versus ordinary cannot be proven in the window. The narrow card-required support is read the exact captured revision's existing typed source_kind through the same frozen subject contract and consume it with closed Worker/FE parity. Do not infer from current Work/Source labels or build SP22's broader historical/advice/producers early.
4. **Complete the native controlled presentation/launcher naming.** Reuse the existing detail window for the five named views and explicit “approved quantities, not paid hours”; standard hours and nonstandard units must remain distinct, never browser-paid-hours/amount calculation. Keep recorded shift/break facts and exact original Client/role/band labels. Register the same exported launcher for future DR31 without a new route/store. Source/Protected wording must use the frozen typed evidence above. There is no reason to rewrite current prepareCase/DR13 or allocation owners.

Worker transport already exists: `broker/src/index.js`:199013–199019 dispatches exact POST `/api/banking/pay/next/frozen-detail` to `handleBpayNextFrozenRead`; fixed RPC module targets only `bpay_next_frozen_detail_page_v1`. Worker rows/decimal strings/bytes/cursors and FE `createTransport().frozenPage()`864–886 are the existing integration seams. The card's Worker:none means no new feature route; the genuine closed-validator/output parity corrections above are required supporting integration, not another transport protocol.

Binding sort exception, corrected after reading the complete policy: `SCREEN_DESIGN_POLICY.md`:172 (SDP-34) explicitly exempts “expanded child rows (days, shifts, expense lines), result lists, fixed business order (Day / Night / Sat / Sun / Bank holiday) and dialogs.” DR17's existing native read-only five-tab detail dialog is exempt. Preserve the current detail_no/break_no/bucket/family+code continuations and fixed canonical rate presentation; **no new sort system, sort arguments, indexes or sort readers are required for this dialog**. The earlier suggestion to design whole-column sorts here is withdrawn. This exception does not permit local partial-page sorting to masquerade as a whole-list server sort elsewhere, and does not change future DR31's own list requirements.

## Supplied design and scope discrepancies

Read pack DESIGN_INDEX:62, SCREEN_INVENTORY:98/110 and B4 audit176–180/250–252. Own direct design is **SCR47/47b/47c/47d/47e**, and SCR54 is future DR31's tree. Pack labels Lines, Shifts, Breaks, Used rates, Available rates and the approved-hours warning; actual pixels/independent A4 are future NOT_RUN here.

Pack says available rates “five buckets only” and removed Additional/mileage; full card455 expressly includes **each Additional and mileage code**, with all available channel/charge rates. Follow card data authority and record this visible deviation rather than hide genuinely captured native rows. Pack Used rates “how set” was an explicit reader addition subsequently removed from inventory (“no Rate set by”); do not add unsupported metadata. Original source/charge facts only. Historical state-independent detail is a later owner; preserve current Draft guard instead of broadening it to fill SCR54/history pictures.

## Delivery map and saved provenance

D01: existing reader reuse plus only proved output/nullable native-contract support above. D02: code/catalogue evidence of existing unconditional guard/four attachments, no proposed SQL. D03–D05/D08: retained context and source references, no money-owner rewrite. D06/D07: native detail/launcher completion, genuine frozen facts and exact closed parity. D05 DR13 WAIT does not become a DR17 Work expansion. All real mixed/Source/protected/Daily/later-edit/immutability/queue journeys remain NOT_RUN.

Saved SHA-256 checkpoint: public1005 `CCB57080D3119372BE6408C1CED497C95783EB06521D2727D4BE3FCE4436EA53`; private0025 `D7433FD91345A02EA75A596577C66514465C007D622DD0D225CA7EDF96708CE9`; Source0005 `DF49B9FC467AE9806EFFB19EC3B39820059FA46980A2F761F62C3F2EF8A96111`; ordinary1001 `B5F2F9EC742A11A88EDE05AE0EB433725B410907827BF10300B3540824A92A72`; frozen Worker `34A5DB0ACB56068E9509E42FDA3428658B3E0B77F16297A62BDCA53877E644BA`. These identify inspected saved source, not hosted installation or whole-item PASS.
