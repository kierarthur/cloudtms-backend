# WB-29 captured composition — registry and source-gate review

13 October 2026. **Scoped SOURCE PASS — 0 open findings.** Independent read-only review of root-authored `scripts/bpay-next-source-gates.mjs` and `docs/bpay-next/final-definer-closures.json`, against the actual 2357/0005/0016 CREATE signatures and call sites. This is the finite registry/gate-support receipt; it does not supersede the separate money-owner/composer/eligible-leaf reviews or approve the whole WB-29 item.

## Exact registry closure

A parsed comparison with HEAD finds exactly 14 changed/added routine records: ten promoted current owners and four new signatures. Every old allowed physical source remains, its current final parent is retained as a predecessor, and each promoted record now names one exact new finalRoot/finalPhysical/finalClosure. The four new records have an empty predecessor list and only their actual new physical file. No duplicate or removed signature was found. Format version, foundationComplete, policy, editor chains and release base are unchanged.

| Exact private signature | Current parent → new final physical source |
|---|---|
| `bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | 1419 → 0005 |
| `bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | 1419 → 0005 |
| `bpay_next_record_work_segment_origin_shares_v1(uuid,uuid)` | 1236 → 0005 |
| `bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint)` | 1236 → 0016 |
| `bpay_next_segment_fact_guard_v1()` | 2223 → 0016 |
| `bpay_next_materialise_choice_segments_v1(uuid,uuid)` | 1832 → 0016 |
| `bpay_next_selection_memory_capture_v1(uuid,jsonb)` | 2306 → 0016 |
| `bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)` | 2227 → 0016 |
| `bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid)` | 2302 → 2357 |
| `bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric)` | 2218 → 2357 |
| `bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text)` | New → 2357 |
| `bpay_next_captured_resolution_operands_v1(uuid)` | New → 2357 |
| `bpay_next_captured_component_resolution_quote_v1(uuid,text)` | New → 2357 |
| `bpay_next_choice_leaf_eligible_v1(uuid,uuid,uuid,uuid,uuid)` | New → 0016 |

Here 2357 means `supabase/repeatable/12102026_2357_banking_pay_next_captured_resolution_composition_v1.sql`; 0005 means `supabase/repeatable/13102026_0005_banking_pay_next_captured_resolution_hold_v1.sql`; 0016 means `supabase/repeatable/13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql`. Their actual declarations/argument types and owner/revoke footers agree with the 5 + 3 + 6 mapped routines. Existing four-argument callers remain distinct from the new six-argument overload; no unrelated current owner is displaced.

## Finite source-gate additions

The gate diff adds precisely four exact new signatures to `ROUTINE_WORK_SIGNATURES` (lines 62–65), one eight-signature `WB29_CAPTURED_WORK_CALLEES` object (1472–1490), and that object's per-signature list to the existing approved union (1499). It does not add a glob, filename/history skip, dynamic-execution exception or new generic delegation category.

The new actual private call edges are limited to the four-argument input-quote delegate → its six-argument overload; six-argument quote → captured operands/native basis/unchanged selected quote; captured operands → original eligible-leaf predicate; captured component wrapper → input quote; capture → treatment approval root; common hold → treatment root/eligible leaf/captured component quote; recorder → captured component quote. The remaining listed names are finite native aggregate/JSON/array/numeric operations used in those exact bodies. The eligibility predicate has no delegated private helper. No latest NEED/history reader or new financial calculator is admitted.

Recorder 0005:818 has the actual static typed recordset declaration:
`jsonb_to_recordset(v_selected_quote->'prefix_targets') AS x(segment_key text,run_work_segment_id uuid,source_ex_vat numeric,quote jsonb)`.
The scanner's apparent `x(...)` call is admitted only for the exact recorder signature, alongside its actual `jsonb_to_recordset` operation. `x` is not added to the global native set or any other routine allowance. This classifies the existing typed alias syntax; it does not introduce dynamic or history delegation.

The original assertion still requires one classified exact routine body, rejects the named history readers and any EXECUTE token before callee checking, and rejects every unapproved delegation. Its history-point logic and pre-existing finite exceptions are otherwise unchanged. Raw diff shows no new history-expression waiver or frozen-file exemption.

## Current bytes

| Source | SHA-256 |
|---|---|
| Source gate | `c95ac3654b0fd8ed3a2782c8260070d22e2c27f3c1ffb47f86f69b1c5c036a54` |
| Final-definer registry | `9752f6858e1e131a4069a05cf8c36322106177031f9a013ab533a5f3986f5165` |
| 2357 captured composition | `e45c85d73006720caaa39622dd7cc6560ededbccdec830b709fec8e91f773052` |
| 0005 capture/hold/recorder | `5dd92083243dde0bd41418da6436d28d1566c32c1b0a0a540d9a8d4ae742f66a` |
| 0016 original eligible leaf | `35648fde73f982466ed72c065eb9b6916d6ef998364bd64af0f491625a73cfd3` |

Root reports existing source-gate PASS for 1549 SQL / 30 RPC / 168 routine / 37 typed checks; this reviewer did not execute or independently reproduce those checks. No product edit, source-gate/test/compile execution, SQL/RPC/database invocation, business action, fixture, browser or hosted call was performed. Catalogue/install/current runtime and joined business acceptance are outside this source-only verdict.
