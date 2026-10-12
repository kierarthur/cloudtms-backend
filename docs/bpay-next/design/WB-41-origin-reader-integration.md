# WB-41 exact originating Timesheet presentation

12 October 2026. Read-only native source investigation for the A4 D09 omission: the paused automatic recovery has a real originating Timesheet, but its Case row has NULL physical identity and is rendered as a generic deduction. This note authorises no product change or acceptance; root owns implementation. No SQL/business calls, tests, fixtures or captures were performed.

## Actual owner and point binding

The common row owner is `private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)`, current full body in `supabase/repeatable/11102026_0113_banking_pay_next_case_choice_readers_v1.sql:258–971`. The private1634 named page wrappers delegate to this function; current public0207 dispatches them at1426–1435. There is no separate item-to-JSON helper. ACTION_REQUIRED uses `i.physical_timesheet_id` at592. Ordinary GROUP_LINES also uses that item field at737. ISSUE_DETAIL instead emits the exact revision/source tuple from `r` at680–685; its bounded seed only joins `w` through the Case row's NULL `i.work_id` at903–911. Candidate-group metadata and the special export/ROW presentation arm likewise use `r` at516–521 and363. Correct the exact affected serializer paths rather than replacing wrappers or inventing a new field.

`private.bpay_next_work_collection` supplies the actual origin: `case_id` and `case_component_id` are each unique, with composite foreign keys to their Candidate/Case, and `(work_id,original_revision_id)` is foreign-key-bound to the immutable Work revision (`04102026_1519_banking_pay_next_work_collection.sql:9–50`). `original_revision_id` is NOT NULL. The Work row is bound to the same Candidate; the immutable original revision stores NOT NULL physical Timesheet ID/version, week, and nullable Timesheet reference (`03102026_1324_banking_pay_next_core.sql:111–165`). This is exact native ownership, without a booking guess, latest-history fallback or current Source pointer.

Use the collection's **original revision** to obtain the coherent physical ID/version/reference/week/source tuple. Do not combine one revision's reference with a different current physical Timesheet or infer a current approval from these factual identity fields. The native Open Timesheet caller accepts the existing physical ID; no new route is required. A read-only link to the original paid Timesheet does not calculate entitlement or make the Case selectable.

## Smallest safe integration

Within current refresh_case0207's positively qualified AVAILABLE `ORIGIN_UNAUTHORISED_WAIT` branch at568–580, resolve the own collection/Work/original revision by the exact Case/component/Candidate tuple, paid-basis qualification and READY qualification. Assign the existing `physical_timesheet_id`, `timesheet_reference` and `week_ending_date` item fields from that original revision. Place these assignments **after** saving `v_base` at565. RESERVED continues to clone the untouched base at583; accepted rows never enter the new branch. Keep Case `work_id`, `revision_id`, approved-line identity, member keys, rule/basis, eligibility and all money fields unchanged. Do not disguise the Case as a Work member.

For the r-backed read projections, retain the original Work path and add a nullable identity-only origin arm, conceptually:

```sql
left join private.bpay_next_work_collection origin
  on i.reason_code = 'ORIGIN_UNAUTHORISED_WAIT'
 and i.section = 'ACTION' and i.portion_kind = 'AVAILABLE'
 and i.member_kind = 'CASE_COMPONENT' and not i.is_selected
 and i.accepted_run_id is null
 and origin.case_id = i.case_id
 and origin.case_component_id = i.case_component_id
 and origin.candidate_id = i.candidate_id
 and origin.paid_basis_qualified and origin.qualification_state = 'READY'
left join private.bpay_next_work origin_work
  on origin_work.id = origin.work_id
 and origin_work.candidate_id = i.candidate_id
left join private.bpay_next_work_revision origin_revision
  on origin_revision.id = origin.original_revision_id
 and origin_revision.work_id = origin_work.id
```

Apply this factual enrichment after the original <=101 seed, using the unique native point keys, and select the origin revision only for this exact Case arm. ISSUE_DETAIL must supply a coherent existing `physical_revision_id`, `physical_timesheet_version`, `source_kind` and source-head/root/week tuple along with physical ID; the closed decoder cannot accept a physical ID paired with an otherwise missing or unrelated revision tuple. Any Source-head enrichment must join against that same origin Work's Candidate/contract and original revision week. Preserve all ordinary Work joins byte-for-byte except the finite fallback needed for this arm. No new helper schema, public trigger, selection or accepted-input operand is justified.

The existing Worker/Office interfield rule also ordinarily requires the physical revision tuple to be NULL when `work_id` is NULL. If that check is touched, allow only this exact ACTION / ORIGIN_UNAUTHORISED_WAIT / AVAILABLE / CASE_COMPONENT Case navigation tuple, retaining the Case's NULL Work membership. Do not weaken the global rule. EXPORT_ROWS has its own independent rule that a NULL Work member cannot carry physical/line/shift evidence (`broker/src/bpay-next-workbench.js:614`); its navigation evidence is not needed for D09. Keep that unrelated export physical tuple NULL rather than accidentally feeding the origin fallback to every serializer and broadening the export contract.

The identity read should not adopt a new money/admission WAIT decision. A maintained WAIT row can remain while reauthorisation is applying; its exact original Timesheet remains a valid link. The separate informational-estimate first-use fence in0207:1269–1287 still refuses positive derivation on that stale processing state. Unknown/missing original identity remains NULL/refusal, never a guessed UUID or unrelated Timesheet.

## Cursor and group invariants

Do **not** annotate public `v_page.rows` with a new Timesheet reference after paging. Common reader PAYMENT ordering, cursor construction and search use the maintained `i.timesheet_reference` at584,840–842,870. Populate the existing item field before its indexed native sort so output, cursor and search use the same value. NAME and money keys are unaffected. The existing Workbench revision advance/rebuild and original cursor context invalidate pre-change pages normally; no token or cursor protocol is needed.

Candidate-group PAYMENT keys use maintained group-display week at558, while its DTO emits `g.week_ending_date` at526. The current display reducer obtains week from `g` and reference from item members (`10102026_1946_banking_pay_next_candidate_group_reader_v1.sql:20,30,55–56`). Thus setting item week alone fixes issue/detail facts but does not populate the parent group's week. If the parent actually displays that week, either retain its existing unknown week and show the already-read native child fact, or populate the existing coherent Case-group week under the same exact origin and rebuild its display projection before paging. Do not retroactively fill only the parent DTO week while leaving the native group-display sort key NULL. Do not broaden unrelated Case, accepted or RESERVED presentation.

The exact reason plus the qualified automatic recovery Case is sufficient to label this row “Overpayment recovery” and offer Open Timesheet. It is not sufficient to invent a new overpayment or change a financial amount. Existing generic Case labels for other reasons remain unchanged. Final successor/source review and A4 are still required after root applies this narrow D09 integration.

## Actual0230 supporting source review — essential reachability failures

Freshly compared the full public Workbench0230 successor against0207. Raw SHA-256:0230 `135428875aa2fb40e137e975455bdf86686a75781b40af2401657044ec47ac31`; retained0207 `15518a516d7e4e1264e44b949ee432b694418daafd9df9fdaf6fed22abca9f27`. The full original public body is retained apart from three local declarations and bounded post-reader identity enrichment. No reference, week, sort key, cursor, Work/Case membership, financial result, selection, estimate or export field is changed. That narrower physical-only approach is safe for existing cursor keys and does not require the earlier suggested item reference/week maintenance. Final reply120,000-byte refusal and service-only ACL remain unchanged. However, **SOURCE FAIL for the navigation addition: its qualifying condition is unreachable for all three legitimate automatic Overpayment reader shapes.** Root was notified immediately.

**WB41-NAV01:** ACTION_REQUIRED and ISSUE_DETAIL have no `selectable` property in their closed native DTO (`0113:585–629,673–723`; Worker schema89–135,185–232).0230:825–827 requires `v_wait_row->>'selectable'='false'`, which evaluates NULL and prevents enrichment for both kinds. The Worker621 and FE620 exception also require `row.selectable===false`; therefore merely correcting the SQL would still reject a populated ISSUE_DETAIL physical tuple. Do not add a schema field solely to cure this. For those item readers, qualify the original native item by the existing `item_id`, matching Candidate/Case/component, exact ACTION/WAIT/CASE_COMPONENT/AVAILABLE, not selected and accepted-run NULL, then join the exact collection/original revision. Their DTO already has member/portion/reason fields for a finite corresponding validator exception; the native SQL point supplies the unavailable selection facts.

**WB41-NAV02:** current registered group-display1946 emits automatic OVERPAYMENT as `presentation_kind='CASE'`, with `case_component_id=NULL` at21–23.0230 requires non-NULL component before entering the origin join, so CANDIDATE_GROUPS cannot enrich that real parent. Its existing validator628 deliberately requires NULL component for every non-ROW group. Keep that native Case identity unchanged: the finite group branch should qualify the exact unaccepted ACTION/WAIT CASE parent and use `origin.case_id` plus Candidate to obtain the **unique** native collection/component internally. The collection schema's unique Case and composite ownership keys prove this without a guessed UUID or returned component membership change. Use a separate CASE-parent validator exception, retaining the ordinary ROW rule and all three physical identity fields/non-NULL coherent source tuple checks.

Reviewed Worker raw SHA-256 `bedc6b03039570d43f665dbaff48eb358d0a0cd821b74f2f3331e54fa81f23df` and FE Workbench `8a4f4a805926debf220b993b6f734382aa48b5c62c78cb292b11422e50b12b00`. These findings concern actual existing reader shapes and the missing D09 remedy, not financial safety or an invented protocol.0230 is reported installed/immutable by root; any correction must use a new successor, leaving those installed bytes untouched. No compilation/runtime/A4 was run by this reviewer; final source closure remains pending the two finite fixes.

## Final0235 closure — SOURCE PASS

Freshly read `supabase/repeatable/12102026_0235_banking_pay_next_origin_wait_issue_navigation_v1.sql`, raw SHA-256 `b37228bf39a62c1423ca446c93736495cb3a49a4f7bd7dbd158aaa21b157be4c`, and compared its complete public function with0207. Only the three local navigation variables and the bounded post-reader navigation block differ. The original body, native sorted values/keys/reference/week, estimate/selection/admission, Case Work/revision/component identity, all money owners, exports, original reply limit and service-only ACL are retained. This is the narrower physical-only presentation path; no indexed display operand is retroactively changed.

**WB41-NAV01 CLOSED.** `selectable=false` is required from the DTO only for CANDIDATE_GROUPS. Item readers instead require the matching original native `item_id` under the exact Case/component/Candidate, AVAILABLE/ACTION/ORIGIN_UNAUTHORISED_WAIT, NULL Work, not selected and not selectable predicate. The exact native collection, original revision and current WAIT are then required in the same STABLE snapshot. Thus ACTION_REQUIRED and ISSUE_DETAIL can now qualify their real closed DTOs without adding a field; unavailable/unqualified origin retains the original NULL identity.

**WB41-NAV02 CLOSED.** The exact unaccepted ACTION/WAIT CASE parent may retain its NULL component. The unique native collection is resolved by Case/Candidate and its actual component is qualified internally; the same affected AVAILABLE item must exist. No returned `case_component_id`, group kind, presentation/member key or Case/Work identity is filled or reinterpreted. The native five-kind automatic recovery tuple and original Work/revision composite binding remain required. Existing source-head enrichment matches that same origin Work Candidate/contract and original revision week.

The Worker `broker/src/bpay-next-workbench.js` raw SHA-256 is `6ce738957d590069f6a778791a01fa319b720b6b8e1c8faa06426ceabc57a137`; FE `js/bpay-next-workbench.js` is `f7b26a6df3bb3623ff985689ff398a71a86f965d2e4277dba7faa57108fb5f95`. Their navigation exception predicates match: only the existing ISSUE_DETAIL/CANDIDATE_GROUPS branch, NULL Work with a real Case, exact ACTION/ORIGIN_WAIT, the original component or the unaccepted CASE parent, and all three physical IDs non-NULL. ISSUE_DETAIL no longer relies on its nonexistent selectable property; group rows retain selectable=false. The original coherent source/root tuple checks and ordinary CASE-versus-ROW/group key/component invariants remain intact. Inputs, kinds, closed schemas and unrelated NULL-Work/export constraints are unchanged.

**SOURCE PASS for0235 and the matching closed navigation validators; no essential finding remains in this supporting addition.** This supersedes the earlier0230 SOURCE FAIL and pending closure, while preserving0230/0234 as immutable history. Root reports local compilation/catalogue evidence separately; this reviewer performed source-only comparison and does not claim runtime, joined business acceptance or A4.
