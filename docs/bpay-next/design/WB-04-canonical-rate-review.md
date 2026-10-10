# WB-04 canonical Daily rate evidence review

Read-only source/design check, 10 October 2026. Read WB-04's full card and all nine child checklist rows, the current Daily reader, native resolver and rate-contract comparison helper. No product edit, test, SQL/database/runtime call, fixture or network action. **Preserve current 0018 and 1009 now.** The named master editor order for 0018 is **DY-02 → DY-08**, and WB-04 explicitly completes its upstream/route/core seam through the DY owners. WB-04 ITEM and D07 remain OPEN until genuine contracted/contractless Daily closure; this note is not installed Daily capability.

## Confirmed omission and exact later correction

Current `private.bpay_next_daily_approval_evidence_v1(p_financial_id uuid) returns jsonb` is registered in `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql:228`. Its final five-code STANDARD `VALUES` projection applies a WHERE requiring at least one non-NULL PAYE/Umbrella/charge rate. Consequently an actually resolved all-NULL bucket is absent from its `rates` array, contrary to the requirement that every bucket be frozen with missing authority remaining missing.

The smallest genuine **DY-02 successor requirement** is a full copy of only this function with that final WHERE removed. Preserve the existing five codes DAY/NIGHT/SAT/SUN/BH, deterministic order, native rates, nullable values, nonnegative six-decimal precision checks, current financial/physical/context qualification, selected-channel stale comparison, full returned provenance, STABLE/security/search_path/owner/ACL. Do not replace NULL with zero or invent a source ID. Do not redefine the other initial-TSFIN routines in 0018. No publisher, route, core, policy, command or protocol alteration is required for this isolated row-completeness fix.

**Impact correction from fresh current source:** 1009's Daily combiner calls `bpay_next_contract_rate_rows_v1('{}'::jsonb, v_contract.bucket_labels_json, v_contract.additional_rates_json, v_mileage_pay_rate, v_mileage_charge_rate)` at lines 210–212. Its missing STANDARD-code fallback therefore contributes an honest NULL standard row, not live Contract standard rates. Current 1009's empty p_rates must not be described as live-Contract rate inference. The omission remains real in 0018's own canonical evidence array and removing its WHERE is still the minimal later fix; no wrong standard cash rate was demonstrated by this combiner read.

## Native channel context and source-ID authority

`public.tsfin_resolve_rates_batch(p_items jsonb)` is defined in `05012026_tsfin_resolve_rates_batch.sql:20` (the source definition census found no later replacement). Both Daily requests carry the same real Candidate UUID, Client UUID, role, exact band, worked date and key, differing only in PAYE/UMBRELLA rate_type.

- Candidate override selection uses native `rates_candidate_overrides.id`, exact Candidate/Client/role/rate_type, strict exact-or-NULL band, and the dated window. PAYE and Umbrella can legitimately have different override IDs/pay rates; a channel with no override keeps its genuine default or missing authority.
- Client default selection uses native `rates_client_defaults.id`, exact Client/role/band, enabled row and dated window. **It has no rate_type predicate.** Thus the same native default row is selected for both channels, including when one channel has a Candidate override. Default supplies Client charge in both channels; override supplies only channel-specific pay.
- Current 0018's channel comparison removes `k`, `rate_type`, `source_kind`, `override_id` and the five pay fields. It deliberately retains Candidate/Client/role/band/date, **default_id** and all five charge fields. Retaining default_id equality is correct for this actual native resolver; removing it would weaken real shared Client-charge/context proof. No confirmed context-comparison change is needed.
- `private._candidate_daily_rate_contract_v1(p_rate jsonb)` in `07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql:4` normalizes genuine source_kind, UUID override_id/default_id, rate_type and all five pay/charge values. 0018 compares the selected current resolver row against the real financial snapshot's `rate_source_refs_json.kind/override_id/default_id/rate_type` and captured pay/charge columns. A native source identity or selected rate mismatch still raises `40001 / BPAY_NEXT_DAILY_RATE_AUTHORITY_STALE`.

The selected source must be CANDIDATE_OVERRIDE or CLIENT_DEFAULT. The opposite channel can legitimately have NONE/NULL; every unavailable bucket still needs its canonical nullable row. Native `o.id`/`d.id` remain the authorities, not guessed IDs, an inferred Contract default, a new witness or a new financial snapshot.

## Retained Daily scope

0018 requires the genuine current authorised SELF_REPORTED Daily HOURS financial row, physical version and worked/break tuple, real Candidate/financial Client/context identity, London worked date/week and actual policy/rate-source JSON. For a real Contract it verifies the Contract Candidate/Client; a contractless reader call retains the genuine financial Client and does not create a Contract. It refuses actual Additional/expense/mileage payloads/amounts; this isolated rate-array fix cannot grant payable non-hours components.

The current registered route 1450 still joins a Contract and returns no genuine DAILY_HOURS route; current publisher 1009 still requires its Contract and admits ordinary/expense routes. The core's contractless identity seam also remains a real later requirement. Preserve these existing guards until the named DY-03/DY-04 owners close them, preserving booked-shift authority and frozen Client/settings. Do not force an ordinary fallback, fake Contract, new producer, parked F25 machinery or early Working/Draft wiring to close WB-04 now. The later DY-08 hours-only integrity guard, DY-06 Manual-after-authorisation rule, WB-16 Daily label and maintained projection requirements remain required.

## Source checkpoint

- Current 0018 whole source: `fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2`.
- Native resolver 05012026: `ecc46d69e6ae9a2553b9553ee75ac13440a8ecca93d26d75e80219340a8c1ce5`.
- Native normalization helper's 2128 source: `f6c04bf346f2e31a54b59fe3637fe9d633a4c50d1b6c39e7afec45f889d8a523`.
- Current publisher 1009 checkpoint: `34227084b44b53abfb441b2b2826d9ce437fdb20e636e7d3d1d3f54bf324c7d7`.

No implementation, compilation or app acceptance verdict follows from this design check. All contracted/contractless Daily and missing-bucket app checks remain NOT_RUN.
