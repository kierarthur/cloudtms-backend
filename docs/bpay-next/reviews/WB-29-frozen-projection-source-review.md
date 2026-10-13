# WB-29 frozen selected-treatment readers — SOURCE review

13 October 2026. **SOURCE PASS for the two 0028 public reader successors; no essential finding remains in this slice.** Reviewed saved, uninstalled source. No SQL, business RPC, DB, fixture, test, browser or hosted action was performed. Compile, installed catalogue, closed transport and business acceptance are **NOT_RUN by this reviewer**; whole WB-29 remains open. Root alone writes product.

Scope: `supabase/repeatable/13102026_0028_banking_pay_next_frozen_selected_treatment_v1.sql`, both complete current parents, the saved frozen projection design and the previously reviewed 2357 captured facade. This reviewer authored the projection design; this is the independent review of Root's concrete implementation, not an independent authorship claim for the design.

## Parent preservation

Literal source comparison found exactly the new local variables, the RATE_DETAILS metadata block and the conditional return-envelope addition in each function. Removing those precise additions restores **both full function definitions exactly**, with line endings normalized, to:

- `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)`: 0620:85–204.
- `public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)`: 2018:2610–2863.

Consequently the original actor/NEXT checks, request-shape refusals, fixed source queries, row columns, cursor operands, native ordering, exact-full-page behavior and byte-short continuation remain intact. The saved final-definer registry maps exactly these two signatures to 0028. No private SRF return type or public membership is added.

## Frozen binding and quote

0028:112–127 and 388–403 use the real line's `(run_work_id,component_key)` RunPosition point and native Worker/RunWork/Run IDs. Missing identities refuse; null-safe tuple equality binds worker, Work, captured revision, approved line, Candidate, Source channel and target channel. The facade then independently qualifies the actual captured position/accepted descriptor/eligible LEAF and original frozen target/policy/pay-date context through the reviewed 2357 gather and input composer (2357:15–36, 134–144, 225–231).

Each public branch calls `private.bpay_next_captured_component_resolution_quote_v1(actual_position_id,NULL)` once (0028:128, 404). Exact returned accepted refs, Included component Source, target ex/VAT/inc and policy/window must equal the frozen line (129–139, 405–415); a mismatch raises `23514 BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED` rather than returning source-only metadata. The actual 2357 return fields at 451–462 and 485–486 match the consumed paths. This adds no valuer, current Candidate/Contract/policy lookup, live Position read or financial mutation. Shared 1157 remains the quote authority.

Historical NULL refs and original `[]` never call the composer (113–114, 389–390). They retain the old rows and return `selected_treatment:null` on RATE_DETAILS. Non-RATE_DETAILS envelopes remain unchanged. Original source RATE_DETAILS rows and `used_line` are retained, including empty Standard rows for nonsegment components.

## Bounded public projection

The captured metadata has 1–5 unique `(rate_family,rate_code)` entries and each entry's policy/window equals the enclosing frozen line (135–143, 411–419). The explicit object constructors (144–164, 420–440) expose only the designed native identities, channels, frozen component quote and bucket facts. Money, units, rate operands and native decision version are extracted/cast as decimal text; nullable Source attribution/manual operands/quote rate remain NULL. Private working JSON and prefix arrays are not exposed. The PINNED/NATIVE_DEFAULT ID/version pair comes directly from the qualified 2357 bucket facts, without inventing a decision.

The complete metadata encoding is charged once before the original row packing loop (167–173, 443–445). The original 512-byte envelope reserve and 120000-byte ceiling remain; each row is charged before append. A too-large first row still refuses, and a later byte-short page remains incomplete at the last emitted row. Original source bucket cursors do not filter the composed metadata; that metadata is the same full frozen component on every source page.

## Advice admission and security

Advice keeps its own original CSV instruction → original BANK transfer → immutable projection/Worker/Run and frozen destination proof (273–344), then requires the exact actual WORK transfer-member line (368–375). It **does not call the READY-only frozen-line guard**. Thus the new metadata does not import today's readiness, bank version, worker/transfer outcome or active NET-projection eligibility into issued advice. The standalone frozen reader retains its original frozen-line guard at 93–107.

Both original fixed search paths/security modes and advice STABLE/UTC configuration are retained. 519–524 retain postgres ownership, revoke browser/public access and grant only service-role EXECUTE; the frozen reader now explicitly revokes service-role before regranting the same privilege. EOF schema notification is present. No money owner, producer, command, table, current/accepted eligibility guard or parked F25 object is changed by this file.

## Reviewed byte identities

| Saved source | SHA-256 |
|---|---|
| 0028 two public successors | `bb2bf3e049eb042036cb414e4801dbd163ac382b85123f50b2561df07d0746c7` |
| 0620 parent | `96a26c83328dbb7253e8526c789dd11544d04d1b8a9dcf1b421e946fcd7dbc65` |
| 2018 parent | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| 2357 captured composer dependency | `e45c85d73006720caaa39622dd7cc6560ededbccdec830b709fec8e91f773052` |
| Frozen projection design | `69d3df477f00aa6a0e69020650e53320998392068998fd0cb3e95a4ce5ff05ff` |

Public metadata is bounded, while the captured facade may perform its already bounded complete native working-set qualification on each page. This source review establishes neither runtime timing nor joined behavior. Closed Worker/Office transport and remaining WB-29 joins require their own receipts.
