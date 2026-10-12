# FC-02 current investigation — sequence 117

12 October 2026. Sol 6.1 Extra High, independent read-only source/design investigation. No product, checklist, database, business call, test or capture changed. This is an implementation census, not acceptance or a claim that later action owners are built.

## Binding scope and supplied views

Full approved card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1038–1059` and local checklist FC-02-D01–D04 require the NEXT Banking “Payment Advances / Snoozes” host, both independently paged lists, all filters/counts/actions, last-loaded time, Refresh/Clear/Retry and Candidate drill-down using CURRENT_CASES/CASE_COMPONENTS. FC-01-D08 remains this screen's owning completion dependency. Dependencies are FC-01, F-16, WB-36/38/39. Latest owner continuous-scroll, whole-scope server sorting, phone targets and no displayed pre-execution pay date apply. Internal official week/date operands remain native authority. F-16/RQ-234 fences an action whose NEXT owner does not yet exist: show unavailable, never enter its old engine.

Read-only supplied pack is `cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`, DESIGN_INDEX:127–130. Individually inspected B9 PNGs: SCR-112 Cases dark; SCR-113 Snoozes dark; SCR-114 Candidate cases dark; SCR-115 Case snooze dark; SCR-112p/SCR-113p phone; SCR-112b filters. Matching JSON specs were inspected for labels, columns, actions and reader additions. No pack bytes/specs/render were modified.

SCR-112 shows eight tiles, but the card/FC-01 requires nine distinct categories; retain the separate stale category. Cases headers are Case, Candidate, Created, Status, Original, Outstanding, Weekly due, Actions. SCR-113 headers are Candidate, Timesheet, What is snoozed, Amount ex VAT, Snoozed until, Note, Created, Actions. Amount is picture-only and absent from WB-39's card/reader: omit it with recorded rationale, rather than calculate or add a financial owner. Both lists need their own current cursor/retained page window, sort and error state. Phone uses the same server sort, wrapped labelled cards, explicit Open and 44 px controls. No page-button substitute for owner continuous scroll.

## Exact current native reuse

| Native owner/consumer | Current source and contract |
|---|---|
| `private.bpay_next_case_list_context_v1(uuid)` | 11102026_1738; agency/admin/NEXT, London date/internal official week, policy pointer, existing Workbench/adoption currentness. |
| `private.bpay_next_case_list_page_v1(uuid,text,jsonb)` | 11102026_1738:235–402; CASE_LIST and CASE_SUMMARY, complete filtered native inventory, no capped history JSON. |
| `private.bpay_next_case_list_row_v1(uuid,jsonb)` | Current 11102026_1939:3022–3159, not the older 1738 body. Original/reference, native principal/funded/recovered/write-off/outstanding, separate payout/due/hold, schedule, gaps and snooze facts. |
| `private.bpay_next_current_case_page_v1(uuid,uuid,integer)` | 04102026_0585:6–39; exact Candidate OPEN/PAUSED cases, Case-ID cursor. It intentionally does not include closed history. |
| `private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer)` | Current 11102026_2041:1861–1930; exact Candidate/Case, ordinal page, native components/current terms, original reference, edit/pause state; closed Case identity is permitted. |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | Current final registry owner 11102026_2259. Preserve its complete current dispatch/auth/actor contract if a supporting read extension is needed; do not restore 1536/1939/2041 public bodies. |
| `private.bpay_next_snooze_management_row_v1(uuid)` / `page_v1(uuid,jsonb)` | Current 12102026_0041. Fresh current/applied/supplied native labels, active lists; separate exact-ID point admits inactive solely for original CLEAR recovery. Existing Workbench route. |
| Worker/Office | `broker/src/bpay-next-preparation.js:311–459` owns CASE_LIST/SUMMARY closed contract; Office CURRENT_CASES/CASE_COMPONENTS schemas at190–218 and decode qualification754–767. `js/bpay-next-snooze-actions.js:73–100` owns the three WB-39 handlers. |

CASE_LIST limits 5/10/20/50; 32 KiB request, 120000-byte reply, 90000-byte row, 16 KiB cursor, two-second read deadline. CASE_SUMMARY scans at most100 native Case keys per chunk and emits filtered count deltas, scanned/matched counts and cursor. Preserve zero-match raw advancement. Publish summary totals only when all chunks finish under one unchanged context; an unknown nullable category stays unknown, never zero. Native first-three component inspection detects unsupported membership; no prefix balance or guessed tax/Client.

Nine categories: Payment Advances, Overpayments, Underpayments, Manual Debts, Manual Credits, mixed taxable/non-taxable Cases, unresolved Cases, stale Cases, Cases with applicable active snooze. Categories overlap; their sum is not the Case total. The full filtered Case count is the completed summary matched_count, not rendered row count or the sum of nine tiles.

## Remaining reader support genuinely required by this screen

1. **Whole-scope sorts.** Cases currently admit only Created/Candidate/Type. Add both directions for the actual Status/Original/Outstanding/Weekly due primary fields. Candidate drill-down's Approved/Paid out/Recovered/Written off/Outstanding headers also need server sorts if built as supplied; reuse Case native counters, not child/client sums. Snoozes currently admit only Created: add Candidate, Timesheet's chosen week/reference primary value, What, Until and Note. Actions have no sort. Sorting a fixed descendant component list or a confirmation/result dialog is exempt; the Candidate Case list itself is a real independently paged list.
2. **Preserved Snooze counts.** Legacy native UI still names `timesheet_snoozes_count` and `timesheet_expense_snoozes_count` (`main.js:10866–10867`) plus full table total (`10893–10908`). WB-39 has no count field. A small exact filtered count addition in the existing SNOOZES reader/closed envelope is necessary to preserve the card's all-counts clause. Count native Work controls by actual scope; do not add Case controls to the Timesheet table or show loaded-page counts as totals. A bounded scan/chunk alternative must finish before publishing totals and retain only counters/continuations, never all DTOs. Do not add a new counter/protocol/table solely for these two counts.
3. **Candidate drill-down.** CURRENT_CASES is OPEN/PAUSED only and lacks creation/schedule/outstanding sort facts. Pair its required current facts with the existing Candidate-qualified CASE_LIST for list/history/enrichment, and CASE_COMPONENTS for the explicitly opened exact Case. Do not let a closed history row silently disappear because CURRENT_CASES excludes it. Show only returned component amounts/stage/status and current terms; no browser money aggregation. A stale component read must clear its old subordinate facts and offer exact retry.

## Minimal honest native index path

1738 migration already has exact two-direction Created and Type indexes, Candidate 512-character prefix indexes and Candidate/Client/subtype point support. Case Candidate ordering preserves the full raw name after prefix, original Created DESC/Case-ID ties, canonical Base64 UTF-8 cursor only and explicit byte bounds. A shared-prefix cohort needs residual sorting; it is not a constant-cost/full-pathkey proof. Keep this honest strategy for wide labels.

For new Case numeric sorts, the values are already native: Original is Case principal_approved; Outstanding is the exact existing1939 Case-counter expression (funded for Advance/Loan, otherwise approved, minus recovered/write-off/paid credit); Weekly due is the exact current rule's stored weekly_due. Native table expression/column indexes plus literal source-driven branches can support these keys without storing balances or inventing a calculator. A current-rule weekly index must join the actual current rule and retain the missing-rule NULL tail. Ties/cursor must follow the actual chosen indexed order; do not claim that a simple joined point index satisfies a different mixed full ordering. Status should sort the same owned primary label/code actually displayed; live protected/Draft explanation remains a subline, not a certified count/sort operand. Adding a computed live-hold sort would require unnecessary lifecycle invalidation.

Snooze Until/Note/Created can use native control columns (Note prefix plus full residual if necessary). Candidate/week/What are fresh joined labels, including R2 last-applied withdrawal fallback. A snooze-only fixed-label shadow cannot satisfy this contract. Prefer explicit current/applied/supplied native join branches with the actual revision/pointer and label indexes used by that query, full-value residual ordering and bounded reply/deadline. If the exact chosen query genuinely requires a small maintained sort key, its refresh coverage must include those real private native pointers/labels; do not introduce public Candidate/Source triggers or a broad projection framework. Builder must record the actual branch/index matrix before claiming A3e complete; correctness of a global SQL sort alone is not evidence that an unused label index provides its path.

## Existing action and host seams

Use `CloudTMSBpayNextUi.createStandardWindowAdapter` and the actual NEXT owner/admin/actor/parent guards before any legacy cache/fetch. Existing main entrypoints are `openBankingPaymentAdvanceModal`12993, ManualCredit14516, ManualDebt15583; all already pass current native parent/owner-qualified options to FC-04/05/06 modules, including edit seeds. Pause/Resume use `openBankingNextCaseStatus`20595 and FC-10. Audit/report use `openBankingNextCaseFinance`49731 and FC-14's native existing readers. Protected Open Draft is `Office.openCaseProtectedDraft`2130; keep its exact run/Candidate/parent fence. Available create/edit/pause/audit owners are reused, not copied into a second form.

Restructure/write-off or report actions whose named NEXT screen owner is not yet built must be visibly unavailable with the reason until their scheduled owner connects them. `openBankingFinanceCaseRestructureModal`16936 is still an old-engine entry; do not call it directly from NEXT. The screen must not build FC-11/12/13/15 or an old-engine compatibility path early.

WB-39 opens new Case snooze only for the actual RECOVERY component of Payment Advance/Manual Debt. Credit has none; a new Overpayment uses Pause, while an existing carried-over Overpayment snooze can be managed by actual native control ID. Obtain ID/version from native SNOOZE_CHECK for the exact component or a bounded own-Candidate SNOOZES read qualified by the exact returned Case/component pair; never assume Case UUID equals component UUID. Whole Timesheet and exact segment are mutually exclusive; no same-date sibling fan-out. Expense More → Unsnooze all reuses the original WB-38 finite MANY window/current authority and journal, not repeated fresh single CLEARs or a replacement command. Withdrawn rows retain single exact-id Unsnooze and cannot Amend.

Pending CLEAR reattachment is mandatory: on reload reconnect the existing TEST/agency/NEXT epoch/actor/typed-scope-qualified journal to WB-39 `unsnooze(id)`, including its inactive exact-ID point. Validate the retained request and match actual scope/ID; offer explicit Try again, preserving bytes/UUID/readback-first behavior. Do not depend on the control still being in the active table, generate a new request, rewrite another actor's storage, or auto-dispatch an unknown result. MANY progress remains its original native request/page/retry journal; never substitute live new membership.

## Picture-only gaps to record rather than guess

The current contracts do not provide Snooze money, masked one-off bank account, “Repayment 3 of 6”, all-Candidate outstanding/to-pay summary, or a manufactured PB-style human Draft number. Use native schedule weeks/start/next due, actual protected run/channel and returned financial counters. FC-14 Candidate report already supplies its own three explicit server summary facts (Advance outstanding, Overpayment outstanding, Snoozed items); these do not equal the picture's all-debt/to-pay totals. Do not compute substitutes from loaded pages. Client is frozen/unassociated/unknown as returned. Unknown Case tax/kind stays a visible gap. The nine card counts and native principal/status/component facts are mandatory; picture-only additions are not permission for new server money or provenance.

## Snapshot evidence

| Source | SHA-256 |
|---|---|
| Case list1738 | `ab912386daf210e25a2d5b6f37038b00aac8081ca0ba27e059deffdb17f47231` |
| Current Case row/edit1939 | `7b723a61096ac50853dbc609acbba6437b403f11aa796a6f77e39a78b8308f66` |
| Current components/pause2041 | `80675b1f77af852977da427d0ce0702bfb2a2da6fafcea2a5e05b731810de44b` |
| Current public page2259 | `1a57552c42d63d0dc885a4e7294472c03319537da9e913a96cbbadc10c2ab879` |
| Snooze management0041 | `58d05480da8e138d89233044e985b34782035e86d0cc16e271d237afd0a8a599` |
| BE preparation transport | `a60f226a920ebbad650cf67f4abb281c71b7ffe1759b846eae084efe5c1b96cb` |
| FE Office | `0f505fb70f79d5d9b740632c6c50cf85ebe99aabffd6a69c55e544e8fea46262` |
| FE Audit/report | `15d133f75e6e2d2c14eb6253cf0c4436e0821db98f6bfe28f7f42cf278c4adf5` |
| FE Snooze actions | `1a58a2158e46296d3c76297ebaca6041c3333bc981ce287e4322d82f47e5bc6f` |

No in-app, business, queue, timing or pixel acceptance is implied. The concrete builder design must close the exact native sort/index and count choices above before product completion; no additional financial owner, producer or protocol is justified by this census.

## Concrete sort/count amendment — current 0058/0101 draft

12 October 2026, read-only design/source follow-up. The builder has now authored `12102026_0058_banking_pay_next_case_screen_sort_indexes.sql`, complete Case refresh/page successors in `12102026_0058_banking_pay_next_case_screen_readers_v1.sql`, and the Snooze page successor `12102026_0101_banking_pay_next_snooze_screen_reader_v1.sql`. These are drafts at this inspection, not installed/accepted claims. The following replaces the earlier open-ended index recommendation with a finite native mapping.

### Cases: four exact fields in the existing maintained read key

The 0058 choice is narrow and valid in scope: extend the existing Case read key with native status, original principal, the exact existing1939 outstanding expression and the exact current rule weekly amount. The existing deferred native Case/rule invalidator refreshes those four values at commit; its existing Workbench revision qualifies every list/count continuation. This creates no financial owner or new lifecycle framework. The draft refresh at0058:13–36 copies the current native operands, retains original rule-revision1 creation time and the frozen Client owner. Missing current rule makes weekly amount NULL. An indexed copy cannot be presented as a live financial authority independently of that same currentness gate.

| Header | Exact value / indexed order | Literal page branch |
|---|---|---|
| Status | `list_status = finance_case.status`; null rank, status ASC/DESC, created null rank, Created DESC, Case UUID ASC | STATUS_ASC/DESC; show the same primary state (OPEN/PAUSED/CLOSED/WRITTEN_OFF translated consistently). Draft/hold explanation remains a subline and is not this key. |
| Original | `list_original = principal_approved`; same null/Created/UUID ties | ORIGINAL_ASC/DESC, native numeric comparison, never numeric text ordering. |
| Outstanding | `(funded for LOAN/ADVANCE, otherwise approved) - recovered - written_off - paid_credit`; same ties | OUTSTANDING_ASC/DESC, unchanged1939 formula copied only as a read key. |
| Weekly due | `current_rule.weekly_due_source_ex_vat` with full Case/Candidate/rule-revision point qualification; same ties | WEEKLY_ASC/DESC; unknown/no schedule sorts in the NULL tail, not as zero. |

The eight actual indexes at migration0058:12–19 and eight fixed ORDER BY branches at repeatable0058:145–198 match those values/directions/ties. Existing Created, Candidate prefix/full residual and Type branches remain their owning1738 orders. Raw key windows precede the original full row/filter evaluation; zero-match windows advance the raw cursor. A3e does not permit sorting only the loaded rows. Candidate drill-down cannot introduce additional counter columns as sortable headers without the corresponding native reader keys; reuse this bounded reader or keep the actual required headers within its server contract.

### Snoozes: native key paths, not immutable label shadows

R2's fresh current/applied fallback is mandatory. For Work controls, resolve exactly `coalesce(work.current_revision_id, work.applied_revision_id, snooze.expected_revision_id)` to an own sealed Work revision. For a current matching expense/segment, use its same component/segment key; otherwise retain the exact captured approved child. Do not substitute public Candidate names or cache a fixed label per control. Case control labels retain their real Case/component association. This is already the0041 row owner's behavior.

All final branches share the exact Candidate/Client/scope/view/date-mode predicates and a unique Snooze UUID tie. For non-Created keys keep NULL last, primary key ASC/DESC, native Created DESC, UUID ASC. Created ASC retains Created ASC/UUID ASC; Created DESC retains Created DESC/UUID ASC. Store dates/timestamps losslessly, including native microseconds. Native comparison occurs before JSON serialization.

| Header | Genuine native path / minimal support | Required query shape and honest limit |
|---|---|---|
| Created | Existing0034 active Created ASC/DESC indexes;0058 `(active, created DESC, id)` supports inactive DESC and each active equality arm. Add the matching history ASC path if keeping UUID ASC in both directions. | Literal `active` TRUE/FALSE arms; ALL merges their exact firstN results. Backward scanning the DESC/UUID-ASC index does **not** supply ASC/UUID-ASC. |
| Until | Native control `(active, until-null-rank, until DATE ASC/DESC, created DESC, id)` at0058:22–23. | Order/seek on the native DATE; serializing the key as ISO text is fine. An ORDER BY on `until_date::text` is a different expression and does not establish this date-index path. |
| Note | Native control `note COLLATE C`, then Created DESC/UUID. Original1100:21 limits Note to500 characters (at most2000 UTF-8 bytes); a full native text key needs no shadow/prefix storage. | Fixed literal Note branch. A `left(note,512)` expression index can serve only a query which actually includes that indexed expression in its order/seek; the planner cannot infer its equality with Note merely from the CHECK. Full note ordering still governs ties. |
| Candidate | Actual effective sealed revision's `lower(candidate_display_name) COLLATE C`;0058 revision512-character prefix index plus actual own Work/control lookup. Case scope can use the existing Case-key prefix index. | Current, applied-only-when-current-NULL, supplied-only-when-both-NULL arms can be driven from the native revision prefix, then own Work/Snooze indexed join. Order by that prefix **and the full native name**, then exact ties; residual shared-prefix cohort cost remains. Scope/work joins alone do not supply the name order. |
| Timesheet | Actual effective revision `week_ending_date`, a native DATE;0058 revision-week index plus exact own Work/control joins. | Same three pointer arms and native DATE order; no fabricated Timesheet identity. The printed reference is a subordinate fact if week is the declared primary key. Historical revisions not selected by the actual pointer are filtered before a branch LIMIT. |
| What | Native primary labels: Whole Timesheet, Shift, All expenses and mileage, and current/captured Expense/Mileage/Travel/Accommodation classification; Case labels only in an actual Case scope. | Finite category arms with literal same primary label and native control Created/UUID ordering are possible without storing labels. Expense category predicates use the exact current/captured approved-line point. If the UI makes shift date/clocks part of the primary key, that requires the actual approved-segment date/clock key too; generic `Shift` must not be advertised as sorting that composite value. A plain scope-kind index does not alphabetically order the mapped business labels. |

WB-10 `02_WORKBENCH.md:449` requires an index-to-kind/order mapping. A finite joined SQL topN sort is correct whole-scope ordering and can truthfully have residual cost, but a label index that supplies only a point/filter join is **not** proof that it supplies the sort order. Twelve literal branches are useful only if the indexed native expression is actually exposed in each branch: ordering a materialized all-label CTE still makes unused indexes no more effective. No constant-cost, universal plan or two-second measured claim is justified here. Do not add public triggers, a pointer rewrite, a stored-label projection or a general proof framework to avoid that honest limitation.

### Exact History and two-count scope

Native legacy semantics at FE `main.js:163349–163369` are precise: Timesheet Snoozes in ACTIVE view are empty, SNOOZED contains active controls, HISTORY contains inactive controls, ALL contains both; date/indefinite mode and Candidate/Client still filter them. Native cleared/WOKEN/CANCELLED controls are history rows, not fabricated new events. The existing inactive exact-ID path remains separate for original CLEAR retry. Inactive rows cannot Amend or initiate a new Unsnooze; the retained original journal can still read back/retry its own command. Hide-completed and selected View must be normalized together consistently, rather than a browser filter changing counts after the reader.

The existing SNOOZES `count_only` read is sufficient: seek at most the original finite page of native control keys, emit only integer deltas and the same scoped continuation, and keep no row/identity inventory. `timesheet_snoozes_count` is the count of all actual Work controls; `timesheet_expense_snoozes_count` is its EXPENSE_LINE/EXPENSES_ALL subset, preserving the named legacy counters. They overlap and must not be added together. Case controls are excluded from both. Each page's counts are deltas, not totals; publish the final total only after a complete unchanged scope walk. The count walk may have its own Created cursor even when the displayed list uses another sort, but its Candidate/Client/view/date/scope and native agency/actor/owner/Workbench/adoption/control-cut operands must match. `count_only` and sort differ intentionally in their own immutable cursor context; do not incorrectly require their complete context object to equal a list context with another mode/sort.

Current0101:66 already makes ACTIVE empty and HISTORY literal inactive. Its bounded count-only branch at84–98 retains just counters. In the initially inspected0101 query, conditional all-label ordering still needs the actual branch/index amendment above. Its raw8192-byte `sort_value` cursor can exceed16KiB when JSON escapes control characters; reuse the existing canonical Base64 UTF-8 scalar with strict decode/re-encode/issued-cursor bounds. These are transport/read corrections, not new authority. The final product/source review must check the saved replacement bytes; this amendment is a scope recommendation, not a final source PASS.

### Fresh draft follow-up

The saved0101 successor inspected at SHA `a8498f28c1d12cf011a91262b750f6ffaff18d6366636a7f62357b6fc8422b7e` now has twelve fixed independently limited branches, a NOT MATERIALIZED native CTE, native DATE ordering, canonical Base64 UTF-8 decode/re-encode, decoded8192-byte maximum and issued16384-byte cursor refusal. These close the conditional-expression/key and escaping correctness findings. Migration0058 at SHA `9a8ba79f02d421c0245efcf233c484d952ef8ad8225c093cdd500cd1043cffff` now adds the history ASC index and uses full native Note indexes. The page Note branches still put a nullable rank and prefix ahead of Note; remove those for the genuinely NOT NULL500-character column if claiming the full-Note ordered path. The joined Candidate expression is still `lower(coalesce(revision.name,case_key.name))`; split Work/Case label sources if claiming the existing revision-prefix ordered path. What still needs its literal native category order rather than treating a scope-kind index as an alphabetical mapped-label key. The honest index recommendation above remains open until those saved branches actually match it.

A concrete transport issue was separately reported: the new `srRowKey` calculates the Candidate operand with JavaScript `toLowerCase()`, whereas PostgreSQL `lower()` depends on native locale and can differ for legal Unicode labels (for example dotted capital I). A valid native page/cursor can therefore be rejected. Carry the server's actual native operand, or choose raw display-name C ordering consistently in SQL/index/Worker; do not implement an approximation of PostgreSQL locale rules in the browser. All source/runtime closure remains pending; no new schema/command/proof framework is needed for this fix.

## Exact query amendment for uninstalled0108 and locked0058

Read `12102026_0108_banking_pay_next_snooze_screen_sort_keys_v1.sql` SHA `675848af3ea50b5e4c253d7168b46017adca0b32b5752cee7da8eff992977923`; locked0058 migration remains `9a8ba79f02d421c0245efcf233c484d952ef8ad8225c093cdd500cd1043cffff`. This is proposed query pseudocode only: no product bytes, database or tests changed.0108's returned native `sort_key` at110 and NULL point-read key at61 are the correct narrow mechanism for the Unicode parity finding; validate/compare that operand instead of JavaScript lowercasing. Canonical Base64 belongs only to the continuation scalar, not calculator inputs or a new token.

### Shared rules to keep exact

`N = limit + 1 <= 51`. Each branch applies all original Candidate/Client/scope/view/date-mode qualification and the exact full cursor predicate **before** LIMIT. Only the bounded native key union is merged; materialize DTOs for the final original page. Every branch uses the same statement MVCC snapshot and original actor/NEXT/Workbench/adoption/control-cut context. Count-only still uses Created ordering and stores only count deltas. The separate inactive exact-ID CLEAR path remains before list branches and retains all its original authority/recovery semantics.

In the snippets, `Q` is shorthand for those existing predicates, not an added SQL function/table. `T` is cursor timestamp and `I` cursor UUID. For any non-Created primary key `K`, the full continuation is: NULL rank increases; otherwise primary compares `>` for ASC / `<` for DESC; if the full primary is equal (including both NULL), Created decreases, then UUID increases. For Created ASC use timestamp increases/UUID increases; Created DESC uses timestamp decreases/UUID increases. Do not insert provider Work/revision UUIDs into the semantic tie: the last Snooze UUID is already globally unique.

### A. Created, Until and Note: expose direct native order

Replace their `from native n` branches with direct control branches, retaining only the Work/Case-key point joins required for Client filtering. Do not resolve revision/approved-line label facts merely to sort or count these columns.

```sql
-- Representative active Created DESC arm. The inactive arm has NOT s.active.
-- ACTIVE view contributes no arm; SNOOZED enables only active,
-- HISTORY only inactive; ALL enables both and merges their top N.
select s.id, s.created_at_utc, s.scope_kind, null::text as sort_value
from private.bpay_next_snooze s
left join private.bpay_next_work w on w.id = s.work_id
left join private.bpay_next_case_list_key k on k.case_id = s.case_id
where v_sort = 'CREATED_DESC'
  and s.active
  and p_args->>'view' in ('ALL','SNOOZED')
  and Q
  and (v_key is null or s.created_at_utc < T
       or (s.created_at_utc = T and s.id > I))
order by s.created_at_utc desc, s.id asc
limit N;
```

Use these exact literal orders per active/inactive equality arm:

| Branch | ORDER BY | Locked/native index |
|---|---|---|
| CREATED_ASC | `s.created_at_utc ASC, s.id ASC` |0058 history_created_asc, or existing0034 active ASC. |
| CREATED_DESC | `s.created_at_utc DESC, s.id ASC` |0058 history_created, or existing0034 active DESC. |
| UNTIL_ASC | `(s.until_date IS NULL), s.until_date ASC NULLS LAST, s.created_at_utc DESC, s.id ASC` |0058 until_asc. |
| UNTIL_DESC | `(s.until_date IS NULL), s.until_date DESC NULLS LAST, s.created_at_utc DESC, s.id ASC` |0058 until_desc; see physical NULL-tail qualification below. |
| NOTE_ASC | `s.note COLLATE C ASC, s.created_at_utc DESC, s.id ASC` |0058 note_asc. |
| NOTE_DESC | `s.note COLLATE C DESC, s.created_at_utc DESC, s.id ASC` |0058 note_desc. |

`active` is the leading equality column of those0058 indexes; viewALL must merge independently limited TRUE/FALSE arms to preserve global ordering while exposing each native ordered path. The View OR on one generic joined branch does not supply that equality for ALL. Until seeks/casts are native DATE, with the existing canonical serialized scalar only at the boundary. Note is NOT NULL and at most500 characters (`1100:21`): remove its null-rank and `left(...,512)` ORDER BY terms entirely. Keep full Note C comparisons. These changes require no new index or stored label.

Locked0058 Until DESC declares `until_date DESC` with PostgreSQL's default NULLS FIRST after the explicit null-rank column. The semantics remain NULL-last because rank is first. For a literal physical path without depending on planner equivalence of the redundant null ordering, split the NULL and non-NULL arms: non-NULL rows order `s.until_date DESC` (the indexed default), NULL rows order Created DESC/UUID and fix `s.until_date IS NULL`; merge rank-ordered firstN. ASC can use the same split. An alternative tiny successor index with explicit desired NULL direction is possible, but query-only split is the smallest support and keeps locked0058 untouched.

### B. Work Candidate and Timesheet: native effective-revision arms

Use inner own revision/Work/control joins for the three disjoint native pointer cases. Each selects a real non-NULL immutable label/week and makes the source index expression visible. This preserves the exact original fallback; it does not edit pointers or reconstruct history.

```sql
-- Current arm; duplicate only with the two pointer predicates below.
select s.id, s.created_at_utc, s.scope_kind,
       lower(r.candidate_display_name) collate "C" as sort_value
from private.bpay_next_work_revision r
join private.bpay_next_work w
  on w.id = r.work_id and w.current_revision_id = r.id
join private.bpay_next_snooze s
  on s.work_id = w.id and s.candidate_id = w.candidate_id
where r.sealed_at_utc is not null
  and s.scope_kind <> 'CASE' and Q
  and v_sort = 'CANDIDATE_ASC'
  -- expose the index-start range, plus the full original cursor below it
  and (v_key is null or
       left(lower(r.candidate_display_name),512) collate "C"
         >= left(v_key->>'sort_value',512) collate "C")
  and FULL_NATIVE_NAME_AND_CREATED_UUID_CURSOR
order by left(lower(r.candidate_display_name),512) collate "C" asc,
         lower(r.candidate_display_name) collate "C" asc,
         s.created_at_utc desc, s.id asc
limit N;
```

The applied arm uses `w.current_revision_id IS NULL AND w.applied_revision_id = r.id`. The supplied arm uses both `w.current_revision_id IS NULL AND w.applied_revision_id IS NULL`, then `s.expected_revision_id = r.id` plus the same own Work/Candidate binding. Do not use supplied as a fallback when an actual current/applied pointer exists but its authority fails. Original native FK/sealed-pointer guards supply valid identity; a missing authoritative tuple must not be repaired or replaced by a newer/older guessed revision. The cursor comparison uses the returned PostgreSQL lower result; it never calls browser lowercase.

For DESC reverse the prefix range (`<=`) and both prefix/full-name directions only; Created remains DESC/UUID ASC. For TIMESHEET use the same pointer arms, `r.week_ending_date` in SELECT/seek/ORDER BY, native cursor DATE comparison and Created DESC/UUID ASC. Because native Work revision name/week are NOT NULL, do not prepend their generic joined NULL-rank to these Work arms. NULL rank is needed only for an actual nullable source, not a constant FALSE expression that prevents matching the indexed first key.

0058 revision_candidate exposes the 512-character prefix only; reverse scan can expose its DESC prefix. Full names sharing a prefix, plus the joined Created/UUID ties, still need exact residual cohort sorting.0058 revision_week exposes the week first key in either direction; same-week joined ties need residual sorting. Existing revision/Work PKs and0058 `(work_id,scope_kind,id)` control index provide the genuine join probes. Do not claim the revision index covers joined Snooze ties or all history filtering, or that every planner will choose this path. All predicates precede LIMIT so sparse controls do not silently shorten whole-scope membership.

Keep Case Candidate as a separate own Case/control/key branch, ordered on native `k.candidate_sort_name` prefix/full value with the same Snooze Created/UUID ties; existing1738 Case-key prefix indexes expose that prefix. Qualify actual Candidate and exact Case association. The original Case Timesheet primary is NULL, so its NULL-tail arm needs only actual control Created/UUID ordering. A missing required authority is a named refusal, not an invented NULL label or quietly dropped corrupt row. Merge the limited Work/Case pointer arms using the same final semantic key; their joins are disjoint, so there is no native row duplication.

### C. What: finite typed label/category arms

Do not ORDER BY a CASE expression and claim `(scope_kind,case_kind,created,id)` is its alphabetical key. Each native category arm emits one literal primary label, fixes native scope/kind, applies its exact category predicate, and orders only Created DESC/UUID. This uses0058 scope_label's first equality fields then its actual order. The bounded outer merge orders label C ASC/DESC, Created DESC/UUID ASC. Advance and Loan are separate arms with the same label; their topN are merged together.

| Constant primary label | Literal native scope/kind | Additional exact own-line predicate |
|---|---|---|
| Whole Timesheet | WHOLE_TIMESHEET; `case_kind IS NULL` | None. |
| Shift | SEGMENT; `case_kind IS NULL` | None. Date/clocks remain subordinate native facts, not this primary key. |
| All expenses and mileage | EXPENSES_ALL; `case_kind IS NULL` | None. |
| Mileage | EXPENSE_LINE; `case_kind IS NULL` | Effective current/captured line's component_kind = MILEAGE. |
| Travel | EXPENSE_LINE; `case_kind IS NULL` | Original line CASE classification gives Travel (Mileage takes priority). |
| Accommodation | EXPENSE_LINE; `case_kind IS NULL` | Original classification gives Accommodation (Mileage then Travel take priority). |
| Expense | EXPENSE_LINE; `case_kind IS NULL` | Original classification gives Expense; no substitution of another live line. |
| Payment Advance repayment | CASE/LOAN and a separate CASE/ADVANCE arm | Exact original Case association. |
| Manual Debt recovery | CASE/MANUAL_DEBT | Exact original Case association. |
| Overpayment recovery | CASE/OVERPAYMENT | Exact original Case association. |

For expense rows use original0108:70's exact own `revision_id/component_key` line point or captured `expense_approved_line_id` fallback, under the already selected effective revision; classify with exactly the0041 row helper's priority. There is no need for a new approved-line classification index because the native scope/Created path drives controls and each classification is an existing unique/PK point. Historical controls remain classified from the required current/applied/captured facts, never a fixed control label shadow.

```sql
-- Representative fixed Shift primary; CASE/component display facts stay native.
select s.id, s.created_at_utc, s.scope_kind, 'Shift'::text collate "C" as sort_value
from private.bpay_next_snooze s
-- original Client point joins only, if filtered
where s.scope_kind = 'SEGMENT' and s.case_kind is null and Q
  and v_sort in ('WHAT_ASC','WHAT_DESC')
  and (v_key is null
       or (v_sort='WHAT_ASC' and 'Shift' collate "C" > v_key->>'sort_value')
       or (v_sort='WHAT_DESC' and 'Shift' collate "C" < v_key->>'sort_value')
       or ('Shift' collate "C" = v_key->>'sort_value'
           and (s.created_at_utc < T or (s.created_at_utc=T and s.id>I))))
order by s.created_at_utc desc, s.id asc
limit N;
```

These eleven scope/kind arms produce ten actual labels (LOAN/ADVANCE share one). At most561 intermediate native keys forN51 is a fixed small merge, not an all-agency identity inventory or new continuation protocol; only the final at-most50 DTOs are returned. The existing request/row/reply/cursor bounds and deadline still apply. The source gate/card receipt must map each **actual ordered leg** to its native index and separately name unavoidable residual prefix/week/category filtering costs. Source eligibility is not measured query-plan/latency evidence; no database/runtime acceptance is claimed.
