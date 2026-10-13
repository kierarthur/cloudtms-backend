# WB-12 Candidate count and Status sort investigation

10 October 2026. Read-only source/design investigation. No SQL, test, gate, application/runtime, data, hosted or network invocation; no product edits. Root alone implements. This is the bounded supporting read contract, not whole WB-12 or screenshot acceptance.

Read the full WB-12 card (`02_WORKBENCH.md:499–539`), all checklist rows, seq60/dependencies, current scope investigation and final-definer registry; current1634 reader,1252 summary schema/delta,1332 contribution final, and strict Worker/FE mirrors. Read SCR-04's actual T01/T06/T04d JSON sources. Parent explicitly clarified E6 and the actual visible columns: Candidate with TMS subline, Deductions, Amount ex VAT and Status; Timesheets View remains WB17. A3 uses continuous bounded scrolling, highlight versus double-click/Enter/phone Open, no displayed pay date, and server sorting for actual data headers.

## Current facts and minimum objects

`private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer)` and `public.bpay_next_workbench_page_v1(jsonb)` currently resolve to1634. The unchanged HEADER/CANDIDATES wrappers can pass their existing JSON scope to the common reader; no signature/new kind is required. A new complete successor of those two definitions can add HEADER count support and CANDIDATES Status sorting, preserving all1601 issue fields,1634 native group-date seek, ten kinds,5s config/service authority, current/adoption barrier, all other projections, receipt/keyset validation, ACL and NOTIFY. One matching Status index is supporting schema; no new table, publisher, producer, classifier or financial owner is needed.

The natural Candidate-summary PK is `(candidate_id,client_scope,pay_channel,section)` (`1252` migration183–220). Name is non-null native text; bank warning is nullable and restricted to `BANK_DETAILS_MISSING` / `BANK_DETAILS_CHANGED`. Both uses one channel-ALL summary; no Client uses client_scope ALL. Never union channel rows to count or sort Both.

`1634:50–55` already shows Ready Candidates when `payable_count>0 OR deduction_item_count>0`. Keep this predicate: E6 requires deductions-only explanatory rows to remain visible with disabled tick and “Nothing to pay this week”, with debt outstanding. Bank warnings are informational and do not disable earned pay.

`1252` contribution:61 counts selectable PAYMENT members; its deduction count is separate at67. The sole delta:126 currently updates scope `candidate_count` when Candidate `item_count` crosses zero, which includes deductions-only Candidates. Do not rewrite that maintained delta for the screen. Derive the **READY HEADER Candidate count** from exact natural Candidate summaries with positive `payable_count`; retain native counts for other section headers and all existing money dimensions.

## E6 exact count universe

Documentation-only query shape, over maintained summaries, not business history:

```sql
select count(*)::bigint
from private.bpay_next_workbench_candidate_summary s
where s.section = 'READY'
  and s.pay_channel = v_channel
  and s.client_scope = v_client_scope
  and s.payable_count > 0
  and (v_candidate is null or s.candidate_id = v_candidate);
```

The PK admits exactly one row per Candidate in this scope. A filtered Candidate contributes1 only when that actual summary has positive payable_count, otherwise0. Genuine deductions-only rows contribute0 even while visible. Do not substitute amount>0, bank state, loaded rows, `selected_count`, raw item/segment counts, another current Candidate sweep or recalculated cash. Positive payable_count is a native count predicate, not a claim of positive final bank cash.

Existing HEADER `selected_count` / `selected_amount` remain retained accepted reservation facts. Add exactly `selected_candidate_count` as nullable integer text to the HEADER row. With no count intent it is null; with admitted intent it is the exact selected number over the above complete eligible universe. Update READY `candidate_count` to the eligible denominator; other fields retain existing provenance. True empty current scope returns0; unavailable/dirty/read failure is a refusal, never0.

## Proposed closed HEADER-only intent

Parent's chosen shape is sound:

```json
{"count_selection":{"mode":"ALL","candidate_ids":[]}}
```

`count_selection` is optional/default null. Non-null is an exact two-key object: `mode` is ALL or NONE; `candidate_ids` is a finite array of unique canonical lowercase native UUID strings. ALL means all eligible Candidates except those IDs; NONE means only those eligible IDs. Empty exceptions therefore count all or zero. IDs outside the current filtered/eligible universe contribute nothing; they must never manufacture current membership. The count is Candidate intent only, not work/component choice, a lease, financial command, stored choice journal or a proof of immutable Create membership.

Reject malformed/extra keys, wrong mode, missing/non-array IDs, null elements, non-UUID strings and duplicates. Keep existing32KiB request envelope, duplicate-query-key and duplicate-JSON-key refusal, and all three layers' matching finite array cap; an explicit512-ID cap is a reasonable proposed local envelope if root chooses it. Record the actual cap. If retained intent exceeds that cap/byte envelope, refuse the count visibly and keep truthful local intent; never truncate IDs or fall back to a fabricated exact count. Larger saved choice/upload semantics retain WB22/WB23 ownership.

Accept non-null count intent only for `kind=HEADER, section=READY`, with existing empty search/null after/read constraints. Confirmation/count callers explicitly send READY. Other kinds/sections reject non-null intent; ordinary HEADER remains accepted without it. The response must have null `selected_candidate_count` with null intent and valid non-null integer text with intent; in the same READY header it cannot exceed eligible candidate_count.

For ALL, count eligible rows whose candidate_id is not in the unique array; for NONE, count eligible rows whose candidate_id is in it. Empty UUID arrays have the intended ALL/NONE behavior. Keep this inside the same STABLE read snapshot/current barrier as the header money and revision. Count read work is over the complete indexed native summary scope, not a page sum or O(1) claim.

## Concrete serialization and context seams

* SQL public1634:543–547 currently requires exactly13 keys. Permit the one optional count key while preserving the original required fields and rejecting every other extra; do not accidentally require the optional key from existing service callers. Validate object/null shape before processing nested fields.
* SQL `v_scope` currently removes actor/kind/after/limit only (:594); Worker `SCOPE` derives COMMON excluding only after/limit (:379); FE mirrors this at381. Keep count_selection out of ordinary public context/cursors because it is a HEADER-only, nonpaged intent, not a scope filter. Pass it explicitly in **HEADER's private read scope** after constructing the unchanged public context; otherwise the helper cannot see it. Do not silently add it to every existing cursor or kind.
* FE `createWorkbenchClient:489` serializes only `after` as JSON and uses String(value) otherwise. Explicitly JSON-serialize count_selection. Worker GET parser:486 currently parses unique JSON only for `after`; apply the same exact duplicate-key parser to count_selection. String(object) would be `[object Object]` and is a concrete failure.
* Normalize the optional field to null, forbid it on non-HEADER/non-READY inputs, and add the exact nested type in SQL → Worker → FE. Mirror `ROWS.HEADER.selected_candidate_count` as nullable integer text. Retain existing strict exact row keys and all other schemas.
* Capture count args/local intent generation before await, and compare actor/current scope/revision on return; suppress a stale intent result. Public context still binds agency, actor, NEXT epoch, official date, workbench/adoption revision and natural scope. No count-on-every-checkbox money read or automatic business retry is required. Between submits display truthful “All except N” / “Only N” intent; actual confirmation/estimate owners consume the server capability later.

## Exactly one needed new sort

TMS is a subline of Candidate, not a separate data header. Current NAME sort already binds name/ref-null/ref/UUID. Do **not** add unused TMS or BANK sorts. Current Status header uses genuine Ready versus Nothing-to-pay and a native bank-warning badge; add `STATUS_ASC` / `STATUS_DESC` for CANDIDATES only. No new status/progress field or per-Candidate Updating/Failed authority is needed here; WB19 owns real progress.

Parent's proposed tuple is complete and deterministic:

```sql
(s.payable_count = 0,
 s.bank_warning_code is null,
 coalesce(s.bank_warning_code,'') collate "C",
 s.candidate_sort_name collate "C",
 s.candidate_id)
```

ASC places genuine payable rows before deductions-only; warning codes precede null within that group and use explicit native C order. DESC reverses every key, as existing sorts do. Document this factual ordering; no inferred bank eligibility/severity rank. Final UUID is the unique native tie-break. Name is non-null by schema; bank uses a null flag plus coalesced native code, so tuple comparison has no SQL NULL gap.

Use exactly this tuple for seek, explicit scalar ORDER BY, membership probe and emitted cursor key `[boolean,boolean,text,text,uuid]`. Public key arity/type checks at1634:616–625, Worker `validKey`/`rowKey`/`keyCompare` at406–423, and FE mirrors at408–425 all need the same five operands. Existing boolean and UTF8/C comparison logic can compare this tuple; do not use localeCompare or browser row sorting. Restrict STATUS to CANDIDATES explicitly: widening the global sort enum alone would incorrectly admit it on issue/picker kinds. Preserve the exact unknown-money tuple behavior for AMOUNT.

Add one native B-tree matching:

```sql
(section,pay_channel,client_scope,
 (payable_count=0),(bank_warning_code is null),
 (coalesce(bank_warning_code,'') collate "C"),
 candidate_sort_name collate "C",candidate_id)
```

Backward traversal supports DESC when every key direction reverses. Existing1634 NAME/AMOUNT/DEDUCTIONS and prefix indexes remain. For eligible counting this Status prefix can restrict its nonpayable boolean tofalse alongside the authoritative payable_count>0 predicate (non-null/nonnegative native count); do not claim index-only/latency proof without actual execution. A separate count index is only justified if the implemented predicate genuinely requires it; no new maintained counter is needed.

## Boundaries and evidence

Strict service/admin/actor authority, exact route fence exemption, current/adoption barrier,32KiB request/120000-byte SQL/128KiB transport response,4s caller/5s RPC bounds, native financial tuples and RuleB stay. Later actual Create/WB21, persistent/child choice WB22/WB24, Timesheets View WB17 and progress WB19 remain required at their proper chain positions. The count cannot itself certify a frozen command's membership or manufacture a mixed child row.

Source SHA256:1634 reader `391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d`;1252 schema `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`; BE Worker `3234c8640877dd5d1dac8ffdddee675f8d9e3e2003f73bc52f191b3150f64c31`. Spec SHA256:T01 `3feb615e970639be63a6e5b6f23d74848e3fa02b8b5e19c81458a53abd86fade`; T06 `6b433f3efce7765f6a16049c32ffc36a465c8d8d6bf024d40941d89322c04fb0`; T04d `17b6463b594ddee4a22265e62846ea771c46de887093ccb4631361a9e78bc504`. Parent may change saved source concurrently; re-read before code review. No business counts, timing, rendered screen or first-use result is claimed.
