# WB-07 query classifier investigation

Read-only source investigation, 10 October 2026. Read the complete WB-07 card (02:281–314), all five deliverable rows, R2 WB-07 and its ownership connections, current final-definer registry, SP-13 display contract, current 1332 classifier, 1321 member authority, Rule-B 0151 PIN/seal consumers, and 1236 capture/hold owners. No product edits, SQL, tests, fixtures, business calls, hosted or runtime execution.

## Required smallest successor

The current final defining `private.bpay_next_workbench_refresh_work_v1(uuid)` is `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql` (SHA `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391`). A full-body successor of **this function only** is the necessary classifier change. Preserve its WB-02 withdrawn branch, dirty lifecycle, native member conservation, summary delta, accepted selection, snooze, UK-week eligibility and linked-Case refresh behavior. No migration or new public/private gate, receipt, financial owner, Source producer, trigger or protocol is needed.

**Confirmed gap:** lines 192–195 qualify exact positive Source WORK and set `query_state='UNAVAILABLE'`, but never call `weekly_source_pay_query_gate_v1`, change the reason, or remove Ready selectability. Lines 196–201 can therefore store an eligible positive Source row as READY despite unavailable/open query facts. The existing query state flag is a placeholder, not an implemented blocking classification.

Use the existing Source-owned STABLE gate at `supabase/repeatable/05102026_0308_weekly_source_pay_query_gate_v1.sql:12–16`. Its body returns qualified facts minus only `hours_incidents` and `manual_reviews`. The exact five keys are `{ok,code,scope,blocked,query_state_sha256}`; there are no reason/opener/time display fields. Never call the VOLATILE `weekly_source_pay_query_admit_v2` from this reader/classifier. This item adds nothing to the Source writer-admission census.

## Exact eligible portion and sibling isolation

Keep the existing current physical Source qualification at 1332:192–194: actual SOURCE work, revision kind SOURCE/PROTECTED, WORK/PROTECTED_WORK component, real source event and physical Timesheet, known complete amounts, and **positive included residual**. A current normal/protected Source revision's event is native persisted provenance; protected seed 0345:360–365 and 390–396 shows its actual approval event and WORK/PROTECTED_WORK distinction. Source member authority remains the current 1321 definition (see below).

The qualified money is `max(A−C−H,0)` at 1332:160–168, after exact native complete/attribution validation. Query evaluation must not change A, realised C, held H, approved pay, invoice, recovery, signed zero/negative evidence or the actual accepted Draft. EXPENSE, MILEAGE, ADDITIONAL, ordinary work, unknown money, fully paid, fully held-away, withdrawn display, ZERO and RECOVERY portions remain outside this query classification. An effective snooze has already moved its positive amount to excluded and included to zero (177–181), so that excluded amount is not newly offered pay. Future-week positive included work may still have query facts; the declared query priority precedes FUTURE_WEEK.

**Important merge seam:** apply the query reason/state only to the exact positive AVAILABLE row, not its RESERVED sibling. Existing code clones `v_base` for RESERVED at lines 207–217 and then clears only query flags at line 211. If the query reason is applied before `v_base` is captured, a rank 60/61 query reason would survive the later RESERVED priority test (`v_priority>111`) even though that sibling resets `query_qualified=false`. That would wrongly put actual existing H into PAY_QUERY_OPEN/UNAVAILABLE.

The smallest safe placement is inside the positive AVAILABLE insertion branch, after the generic `v_base:=v_item` at line 197 and before the INSERT at line 201. Preserve that base with its pre-query section/reason/priority for the later RESERVED copy; use `v_item.reason_priority` for the AVAILABLE query override rather than mutating the generic `v_priority` used by sibling construction. ZERO/unknown rows reach the same insertion branch but remain `query_qualified=false`, so they do not invoke the gate. Existing RECOVERY branch remains unchanged. Recompute the AVAILABLE row's selectability after the query override. Keep all conservation expressions and amount operands intact.

## Minimal finite call/response handling

Use local variables for the raw gate, a loaded boolean and a resulting query-state discriminator. Every eligible member of this one work revision has the same physical Source scope. Load/cache the gate once per refresh, at the first qualifying AVAILABLE member, rather than re-running full Source facts for each child. This is ephemeral local state; no gate reply/digest/display evidence is persisted on the item.

Documentation pseudocode for the insertion seam:

```text
if item.query_qualified:
    if not gate_loaded:
        raw_gate = NULL; query_state = UNAVAILABLE
        begin
            raw_gate = existing_STABLE_gate(revision.physical_timesheet_id)
        exception when others:
            raw_gate = NULL
        end
        gate_loaded = true
        validate raw reply with non-throwing JSON/type/text predicates
        exact successful OK + blocked true -> OPEN
        exact successful OK + blocked false -> CLEAR
        known typed unavailable, malformed/mismatched reply, or caught failure -> UNAVAILABLE
    item.query_state = query_state
    if query_state == UNAVAILABLE and item.reason_priority > 60:
        reason = PAY_QUERY_UNAVAILABLE; priority = 60; section = BLOCKED
    else if query_state == OPEN and item.reason_priority > 61:
        reason = PAY_QUERY_OPEN; priority = 61; section = ACTION
    item.selectable = (section == READY and amount_known)
insert exact item
```

The exception block surrounds **only the gate call**, as required by card line 299. It must not swallow an unrelated native conservation, work/Case identity, projection delta or insertion error. Post-call validation should avoid casts of untrusted JSON; compare UUID/date/version fields as their actual canonical text/JSON values. Do not use a truthy `blocked`/`ok`, missing `blocked=false` fallback, catch-to-CLEAR, or a different Source family as an alternate lookup.

The existing PIN validation at 0151:462–519 supplies the finite response contract: exact five-key object; `ok` boolean and `code` string; success `ok=true/code=OK`, boolean blocked, actual ScopeV2 and lowercase 64-hex V3 digest. ScopeV2 is the nine-key current physical identity `{root_timesheet_id,family_booking_id,target_family_id,candidate_id,client_id,contract_id,week_ending_date,root_version,family_bound_version}`. Match available root, Candidate, Client, Contract, configured week, booking family and physical version to this current work/revision; preserve native nullable target-family/bound-version fields. A failed identity/shape is UNAVAILABLE, never CLEAR. The two installed typed unavailable results are `WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE` (scope NULL) and `WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE` (scope retained); both have blocked/digest JSON NULL (qualified_coverage.inc:24–26 and 168–170). A gate exception also becomes the item's UNAVAILABLE state without aborting the writer that requested the refresh.

Priorities remain the already agreed WB-08 finite table: PAY_QUERY_UNAVAILABLE 60/BLOCKED, PAY_QUERY_OPEN 61/ACTION. Existing missing facts/apply pending/accepted IN_PREPARATION priorities 10–50 retain precedence. An accepted selection remains governed by its retained boundary and can keep its current IN_PREPARATION presentation; a later current gate state is next-run information and cannot amend that accepted record or add a new refusal to its capture/hold owners.

## Existing owners to preserve/reuse

| Object | Current physical owner / evidence | Disposition |
|---|---|---|
| Work member authority | `10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql:5–95`; SHA `4f07a5ab56fd01aa40ccdb9163f3458c66a838af8e056f35987be2c24cb70542` | Preserve exact native A/C/H, immutable Source classification/binding/book qualification, removed component evidence and segment conservation; no new calculator |
| Source gate | `05102026_0308_weekly_source_pay_query_gate_v1.sql:12–16`; SHA `fb70e23a80c7686606b5cbf8f17e2878ca37224f5752067d903e9a253a3cbfa8` | Reuse STABLE five-key facts; Source qualification/WAIT exemptions and V3 digest stay Source-owned |
| Source ScopeV2 | `05102026_0057_weekly_source_pay_query_scope_v1.sql:29–79` | Requires actual current canonical physical root/configured Contract week; target family may genuinely be NULL; do not invent one |
| Accepted Rule-B seal/PIN | `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql:58–68,453–532` | Preserve raw gate capture at selected expected revision plus exact retained gate validation/pin; no current gate reread after Create |
| Capture | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:2984`; exact retained query block 3243–3281 | D01/D03 reuse: only genuine positive Source WORK consumes existing retained pin; BLOCKED/UNAVAILABLE becomes explicit Candidate REVIEW; no mutable fallback |
| Hold | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:3403` | D02 reuse existing exact residual/share/hold authority; no gate call added here |
| Source display | `10102026_0956_weekly_source_pay_query_open_display_v1.sql`; SHA `e0c83140e923ab4d7fd856abb09ca6419c43c0c6d1c9056f03a7013e86f74a9b` | Reuse only for later WB-14 ISSUE_DETAIL; do not call it during refresh or store prose/actor/time on item |

The display reply is `{ok,code,scope,queries,complete,next_cursor}`; query rows have `{query_kind,query_id,reason,opened_by_user_id,opened_by_name,opened_at_utc,work_date,evidence_available}`. Raw relevant OPEN rows are informational and can include an incident exempted by qualified WAIT; only the gate decides blocking. Real absent incident opener remains NULL. WB-14 must finish every native kind/UUID page before claiming complete query detail. WB-13 list labels, WB-14 display and WB-17 Source navigation are later actual consumers, not early competing Worker/UI work here.

## Completion and source acceptance concerns

WB-07 can define the classifier now with only that named full-body successor plus its registry/provenance/ACL closure. Record exact gate-call isolation, strict unavailable mapping, positive Source-only scope, declared priorities, preserved sibling branches/amounts and retained accepted PIN ownership. Update only the finite existing source-gate helper call census as needed for the one legitimate STABLE call; do not weaken dynamic/history/financial transport rules. Preserve original signature, SECURITY DEFINER/search path, logical owner mapping, private revoked ACL and schema reload. No Source producer/protocol, F25 resolver or new test tool is called for.

SP-15 owns actual OPEN/RESOLVE/same-event clear producer refresh attachment; WB-44 owns complete producer/adoption/fanout closure. Later WB-13/WB-14 presentation must show real query state/detail, not a field invented on the gate. Paid GBP100 + new GBP20, same/different-event Final, query unavailable/exception, accepted later-query change, sibling H/recovery and complete detail paging remain joined NOT_RUN acceptance until their real owners are built and exercised. The earlier R2 accepted-input/protocol paragraphs are superseded by the binding Rule-B decision; retain expected_revision/captured Candidate/raw query snapshot/existing pin, with parked F25 producer/version/resolver work untouched.
