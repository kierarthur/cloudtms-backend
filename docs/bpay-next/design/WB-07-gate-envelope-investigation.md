# WB-07 finite gate envelope and classifier seam

Read-only, 10 October 2026. Read full WB-07 card02:281–314, all five child rows, `WB-07-query-classifier-investigation.md`, current1332 classifier, actual0308 gate, its final1417 qualified facts,0057 ScopeV2, existing Rule-B0151 validation and SP-13 display0956. No product changes, SQL, tests, runtime, fixtures or business calls. This supplements the existing investigation; it does not alter the Source predicate or accepted-input authority.

## Smallest implementation

Only the named full-body successor of `private.bpay_next_workbench_refresh_work_v1(uuid)` needs this finite classifier addition. Use local `jsonb` gate/scope, `gate_loaded boolean:=false`, and a cached discriminator initially `UNAVAILABLE`. Do not add another validator function, Source admission call, display read, persisted digest/receipt, producer hook or broad error trap.

Retain1332:192–194's genuine Source/Protected WORK qualification, native complete/known amounts and positive **included** residual. Keep A/C/H, snooze/excluded amount, actual accepted selection, ZERO/RECOVERY and current applied-pointer authority unchanged. Inside the AVAILABLE insertion branch, after `v_base:=v_item` (`1332:197`) and before INSERT (`201`), invoke the gate on the first qualified contribution only. Cache that one exact work/revision physical scope for all its eligible children. Other works have separate refresh invocations/local caches.

```text
if exact qualified AVAILABLE item:
    if not gate_loaded:
        gate = NULL; cached_state = UNAVAILABLE
        begin
            gate = private.weekly_source_pay_query_gate_v1(revision.physical_timesheet_id)
        exception when others:
            gate = NULL
        end
        gate_loaded = true
        validate the finite success envelope below
        valid successful gate -> OPEN if blocked JSON true, otherwise CLEAR
        every other result -> retain UNAVAILABLE
    item.query_state = cached_state
    override only this item at the existing 60/61 priorities
    item.selectable = (item.section = READY and item.amount_known)
```

Only the **gate call** belongs inside the exception block (card299). Native reads, conservation, row inserts, delta application and Case refresh must retain their original errors. The post-call validation below uses no conversion of returned strings to UUID/date/integer/boolean and no decode of its digest.

## Closed, non-throwing success validation

Use sequential PL/pgSQL shape guards rather than placing object-type validation and JSON key removal in one AND expression. JSON key-removal operators can reject a scalar; an optimizer/evaluation-order assumption must not let malformed gate data throw outside the isolated call trap.

1. If `jsonb_typeof(gate)` is not `object`, leave UNAVAILABLE. **Inside the object branch**, require every one of `['ok','code','scope','blocked','query_state_sha256']` using `?&` and require `gate - that_array = '{}'::jsonb`. Missing and extra keys both refuse classification.
2. Require actual JSON boolean `ok`, actual JSON string `code`; permit success only for `gate->'ok' = 'true'::jsonb` and `gate->>'code' = 'OK'`. Require actual JSON boolean blocked, actual JSON string digest and exactly lowercase `^[0-9a-f]{64}$`. No truthy text or missing blocked=false fallback.
3. If `jsonb_typeof(gate->'scope')` is not `object`, leave UNAVAILABLE. **Inside that object branch**, require all and only the nine ScopeV2 keys: `root_timesheet_id,family_booking_id,target_family_id,candidate_id,client_id,contract_id,week_ending_date,root_version,family_bound_version`. Use the same `?&` plus object-minus-array equality.
4. Require strings for the seven non-null identity fields: root Timesheet, family booking, Candidate, Client, Contract, configured week and root version. Match their JSON values directly to `to_jsonb(native_value::text)` from current `v_revision.physical_timesheet_id`, `v_work.booking_id`, `v_work.candidate_id`, existing `v_client`, `v_work.contract_id`, `v_work.week_ending_date`, and `v_revision.physical_timesheet_version`. Do not parse or normalize the returned identity or date, choose another root, use a Sunday/report cutoff, or substitute today's Candidate for captured native provenance. A final composite condition must be `IS TRUE`, so SQL NULL cannot become available.
5. Keep the native optional family pair: either both target_family_id and family_bound_version are explicit JSON null, or both are strings. A non-null family ID must be canonical lowercase UUID text; its bound version must be positive canonical decimal bigint text. Actual schema `15092026_1534_weekly_source_plan6_schema.sql:2109` declares bound_version **bigint**, not integer. A noncasting finite range check is `^[1-9][0-9]{0,18}$`, then length<19 or a19-character `COLLATE "C"` comparison <=`9223372036854775807`. Never require a target family for ordinary Source or manufacture it. Mixed null/non-null, wrong type, malformed UUID/version or overrange is UNAVAILABLE.

The final successful blocked value is already type-checked boolean. `blocked='true'::jsonb` yields OPEN; its other actual boolean value yields CLEAR. Every malformed envelope, mismatched scope, unknown code, ok=false or caught gate failure remains UNAVAILABLE. There is no distinct money behavior to gain from branching on the two unavailable codes.

If documenting those typed failures, preserve their actual closed shapes:1417:24–26 returns `ok=false/code=WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE/scope=null/blocked=null/digest=null`;1417:168–170 returns `ok=false/code=WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE/scope=<ScopeV2>/blocked=null/digest=null`. Neither is an empty CLEAR result. No unavailable or malformed return needs a cast just to choose the same blocked outcome.

## Current physical scope and optional family ownership

0057:29–43 already resolves the exact requested root, requires canonical/current family, validates current physical WEEKLY/HOURS/non-adjustment/non-revoked/non-archived root and exact booking identity. Lines44–57 resolve actual Contract/Candidate/Client, Source route and one configured Contract week. Lines58–70 qualify any exceptional target family; its genuine absence remains NULL (72–79). The0308 gate calls those final Source facts and strips only the two detail arrays. That Source owner supplies factual family identity; the Banking classifier should validate its finite shape while matching the available exact native work/physical identity, without a second scope resolver/family lookup or a new qualification framework. Existing0151 PIN uses its genuine source-origin family/bound-version authority when consuming a **retained** raw snapshot; that accepted consumer remains unchanged here.

## Portion priority and conservation

Use `v_item.reason_priority`, not generic `v_priority`, for the AVAILABLE override: UNAVAILABLE at60/BLOCKED if current item priority>60; OPEN at61/ACTION if>61. CLEAR adds no query reason. Existing missing facts/apply pending/IN_PREPARATION priorities10–50 continue to win; future-week100 can correctly yield to query60/61. Gate state may remain visible next-run information on a selected row without changing accepted-run terms.

Keep `v_base` captured **before** applying the query reason, and do not mutate it. RESERVED at1332:207–217 must clone the original generic reason/priority, clear query_qualified/query_state to NOT_APPLICABLE, retain original H and generic RESERVED/IN_PREPARATION priority. Resetting only its query flags after cloning a query reason would be insufficient: that sibling would wrongly inherit PAY_QUERY_OPEN/UNAVAILABLE. Preserve all original separate H/recovery operands and per-component sum checks. No query evaluation modifies approved A, realised C, held H, invoice, recovery, accepted Draft or amounts. Recompute selectability on the AVAILABLE item after its reason/section override; unavailable/open cannot remain selectable Ready.

## Scope and acceptance

D04 is the one current classifier successor. D01/D02/D03 reuse actual1236 capture/hold plus existing0151 raw snapshot/PIN authority, with their named later Source/DR38/CM/boundary chains preserved. D05 remains specification evidence, not application proof. WB-07 adds no Source write/admission-census participant. SP-15 later owns real OPEN/RESOLVE/same-event refresh attachment; WB-13/14/17 own actual list/detail/navigation. Display0956's informational OPEN rows must not be used as a blocking predicate, and reason/opener/time are never stored on this Workbench item by this change. No Worker/public API change, parked F25 resolver or protocol is needed. Joined application/detail paging and WB44 high-fanout acceptance remain NOT_RUN.

| Read source | SHA256 |
|---|---|
| Current1332 | `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391` |
| Gate0308 | `fb70e23a80c7686606b5cbf8f17e2878ca37224f5752067d903e9a253a3cbfa8` |
| Final facts1417 include | `45a8a0bc2364318d734c1ec678d79fa617d549a62c3a532ef851c928773aee6e` |
| Scope0057 | `18317d4152fa8e8df4c17ac7895046453a3e26d534b10c45b1fafb50b91a983d` |
| Existing classifier investigation | `d6a13e139c3488860a54182f1326bcc1a23ab761aafac05e65e50f3661ce5c78` |

Finite design findings: sequential object guards avoid accidental malformed-envelope exceptions; family bound version is actual bigint; the pre-query base copy protects RESERVED priority; one gate call per exact work refresh is sufficient. No broader owner rewrite is indicated.
