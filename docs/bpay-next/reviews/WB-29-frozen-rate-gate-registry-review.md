# WB-29 frozen reader gate and registry review

SOURCE PASS — scoped gate/registry delta; 0 open scope findings.

Reviewed saved files against `c003cbfb0f9ae9a40f1a09bb6166eefbcaadbdba`. This receipt covers the source-gate and final-owner registry changes for SQL0028, not whole WB-29 completion or business/runtime acceptance.

## Registry

All 1,021 entries retain their original signature order. Only these two entries change final ownership to `supabase/repeatable/13102026_0028_banking_pay_next_frozen_selected_treatment_v1.sql`:

- `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)` — previous physical owner 0620.
- `public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)` — previous physical owner 2018.

For each, finalRoot/finalPhysical/finalClosures agree; the former final owner is retained in allowedPhysical and appended to predecessors, and 0028 is appended to allowedPhysical. Baseline source is unchanged. No private frozen SRF, other reader, chain membership, or chain order is promoted by this delta.

## Gate scope

The final diff changes only the exact constructor lists for those two signatures. Existing constructors remain. New constructors explicitly enumerate bucket identity, decision/version, treatment/mode, native units and Source operands, manual operands, quote fields and policy/window; the enclosing object explicitly enumerates captured Line/Work/Worker/Candidate, revision, channels/Umbrella/VAT, valuation date, consumed Source and frozen component quote. Numeric values retain explicit native text projection. Nested quote constructors and the selected_treatment envelope are individually listed.

Constructor checking still compares the complete extracted sequence with the complete signature-specific expected sequence. There is no wildcard JSON constructor allowance or removal of original constructors. Existing opaque-row, SELECT-star, floating boundary, dynamic-query, typed-projection, history, and workset classification checks are unchanged.

SQL0028 has the two corresponding captured-facade calls and bounded one-to-five bucket admission, directly inspected in saved SQL. No latest-need/ACTIVE/history reader edge is added.

Final amendment: the two inert public-reader WB29_CAPTURED_WORK_CALLEES entries were removed. These readers remain in TYPED_TRANSPORT_SIGNATURES, not ROUTINE_WORK_SIGNATURES. No call-graph framework or signature membership extension was introduced. The exact typed constructor checks apply; the captured-facade calls remain direct source-review evidence only. No new automated callee coverage is claimed. Removing the inert whitelist rows introduces no boundary relaxation.

## Evidence and limits

Read-only saved-source/diff/registry inspection only. No gate, SQL, database, business RPC, fixture or runtime test was executed by this reviewer. Root separately reports source/db checks and exact local catalogue/security checks; those reports are not independent execution evidence from this review. Broader SQL economics/reader correctness and joined business acceptance remain covered by their own receipts.

| Saved source | SHA-256 |
| --- | --- |
| scripts/bpay-next-source-gates.mjs | 2BABFE5F58CF05407412BCA94EA80FE4395A94BFF9C25AE12326AC267E2208DE |
| docs/bpay-next/final-definer-closures.json | 2C93380BA82C8BBE94826FFC3FEA6C816D34C351ADFE57E5261B2BD332E0C358 |
| supabase/repeatable/13102026_0028_banking_pay_next_frozen_selected_treatment_v1.sql | BB2BF3E049EB042036CB414E4801DBD163AC382B85123F50B2561DF07D0746C7 |
