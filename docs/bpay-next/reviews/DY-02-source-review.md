# DY-02 independent source review

11 October 2026. Read-only source review of sequence 88. No SQL, database, tests, fixtures, browser, hosted calls, product edits or checklist writes were performed. This is a source verdict, not installed or joined in-app acceptance.

## Verdict and smallest remaining change

**SOURCE PASS for the narrow D01 successor; D02/ITEM retains the DY-04 dependency below.** Most DY-02 rate capture was already present and is reused. The identified helper defect was the all-three-NULL bucket filter in `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql:351–352` (also supplied `daily-evidence.sql:124–125`). New `11102026_0925_banking_pay_next_daily_five_standard_rate_rows_v1.sql` removes only that WHERE predicate so its five native bucket identities always carry all fifteen genuine PAYE/UMBRELLA/charge cells, including NULL. No zero, conversion, substitute Contract Standard rates, schema or publisher rewrite is added.

This is not a missing-row claim about the current stored rate schedule: final `10102026_1009_banking_pay_next_canonical_rate_rows_v1.sql:29–42` unconditionally emits all five Standard rows. Final publisher `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:535–543` supplies that helper an empty Standard object and merges only absent Standard identities; therefore a wholly NULL Daily bucket omitted by the old evidence helper was restored as an all-NULL schedule row. The old evidence helper itself nevertheless failed the card's literal five-row evidence contract. The supplied pack has the same predicate, so literal pack equality alone did not close this clause.

## Authority and source census

The complete controlling card is `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:901–927`; all three local checklist rows are `IMPLEMENTATION_CHECKLIST.md:2626–2628`, with future joined clauses at 2634–2638. DY-02 owns evidence-helper spans; DY-03 owns Contractless schema and DY-04 owns the full composed Contractless publisher. Worker and Screen are NONE. F-05 review, its exact publisher delta at `design/F-05-successor-port-analysis.md:186–268`, and F-06's source inventory/review establish that the carried Daily helper and existing caller were imported together; dated old plan statuses are not a fresh catalogue claim.

The pre-correction final-definer registry named:

- `private.bpay_next_daily_approval_evidence_v1(uuid)`: `supabase/repeatable/07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql`, definition at 229.
- `private.bpay_next_publish_ordinary_from_financial_v1(uuid)`: `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql`, definition at 333.
- `private.bpay_next_contract_rate_rows_v1(jsonb,jsonb,jsonb,numeric,numeric)`: `supabase/repeatable/10102026_1009_banking_pay_next_canonical_rate_rows_v1.sql`, definition at 5.
- `private.bpay_next_ordinary_authorisation_trigger_v1()`: `supabase/repeatable/10102026_1332_banking_pay_next_withdrawal_projection_v1.sql`, definition at 287.

The mentioned 08102026_0718 root contains frozen run_work/run_line protections, not a Daily-helper replacement. The 0018 helper's complete definition is LF-byte-identical to supplied `DAILY_AVAILABLE_RATE_CAPTURE_V1_20261006/daily-evidence.sql`; this was a static text comparison, with neither composer nor tests executed.

## Rate and capture findings

0018:247–277 requires the exact current authorised DAILY HOURS TF and physical Timesheet version, matching clocks/breaks, factual Candidate/Client, selected channel, processed policy/source snapshots and strict canonical provenance. 281–286 preserves a present Contract only if its Candidate and Client match TF. 290–300 binds canonical context to that exact TF, derives the worked date in Europe/London and checks its week.

0018:304–310 calls the existing dated resolver twice with factual Candidate, Client, role, band and worked date, once per channel. 311–324 compares selected source kind/override/default/channel and all ten selected pay/charge values through existing `_candidate_daily_rate_contract_v1` against the authorised TF. The contract normaliser `07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql:4–26` merely types those values; it performs no money calculation.

Native `05012026_tsfin_resolve_rates_batch.sql:87–128,132–195,206–216` chooses the dated exact Candidate pay override, otherwise the dated enabled Client default, with strict role/band/date/channel qualifiers. Missing authority remains NULL; charge comes from the same Client default for both channels. The Daily helper's context-equality check at 326–330 preserves that shared charge/context and permits distinct channel pay/source overrides. 331–350 validates genuine available precision and freezes PAYE pay5, UMBRELLA pay5 and charge5 directly; no channel conversion occurs. Its only identified capture defect is the filtered wholly NULL row stated above.

Final 1236:531–550 uses this helper for contracted DAILY and keeps Weekly's Contract branch. It uses dated Standard rows plus existing Contract ancillary availability and labels; count at 567 and INSERT at 611–632 consume the same retained array. TF selected approved rates are preserved separately. No post-Draft reader call to this resolver is introduced. The existing frozen-capture protections remain in 0718; no helper/schema promotion or later owner can be inferred merely from the old supplied proposal.

## D02 and acceptance boundary

Current 1332:311–319 calls the final ordinary publisher on genuine new authorisation while NEXT is active. The current publisher has the named `BPAY_NEXT_ORDINARY_USED_RATE_MISSING` and bucket details for MILE (1236:527–529), aggregate used Standard buckets (804–806), Additional (953–957), and the other used-rate branch at 695–700. These guards prevent silently publishing absent used rates as zero.

**Do not claim full authoriser-visible named-bucket closure.** Current final generic authoriser `10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:2145–2147` rejects non-PENDING_AUTH/non-READY_FOR_HR status with `AUTHORISE_NOT_ALLOWED` before the TF authorisation update at 2200–2207. The genuine RATE_MISSING path therefore can refuse before the publisher's named-bucket guard. This remains an explicit DY-04 admission/transport dependency, alongside DY-03's NULL-Contract schema/current publisher's strict Contract point read (1236:413–424). Keep DY-02-D02/ITEM honest about that open dependency; no broad authoriser rewrite belongs in the helper-only correction.

DY-02-D03's two dates/roles/bands, override/default, both channels/unavailable opposite, empty Contract Standard JSON, unused NULL buckets and frozen later-edit history remain NOT_RUN. Source reuse plus a helper successor is not a claim of joined application acceptance.

## Reviewed bytes

Raw SHA-256, before the pending narrow successor:

| Source | SHA-256 |
| --- | --- |
| Daily 0018 | fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2 |
| supplied daily-evidence.sql | 48885f8637a0c1ca14ad675c6de9f3846f8fe64464004180007caf732888ea1b |
| current publisher 1236 | f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d |
| canonical rate rows 1009 | 07843dcef992a74c2dd2c8c40185d17ce70aba9c33c1c53db5bbcbf6d6952fe2 |
| authorisation trigger 1332 | 9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391 |
| generic authoriser 0931 | 7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b |
| frozen input safety 0718 | 0a884c8cb2568ca568ac51069fcf8fac945159a7f3f4d688d57b1731c295feae |
| final-definer registry | 56c8f726194c1f4ab353430c3e632a23275967ff221d44cca7976947a7398bab |

## Final successor closure

Fresh static comparison of the complete 0925 definition against complete 0018 confirms exact equality after removing one matched all-three-NULL WHERE predicate and putting its terminating semicolon on the VALUES table expression. There is exactly one new definition. Its 119–126 aggregation has five fixed bucket identities and fifteen direct numeric-or-NULL cells; all pre-existing provenance, selected ten-rate equality, charge/context, precision and Contract identity guards are unchanged.

0925:7–8 retains STABLE SECURITY DEFINER and fixed `pg_catalog, public, private` search path; 135–137 retains owner postgres and revoke from PUBLIC, anon, authenticated and service_role. The root is one ordinary transaction, with no schema, invocation, writes or grant expansion. The registry's exact helper chain now advances only this signature from 0018 to 0925 and retains DY-08 as next editor. No later replacement of this helper was found in the saved source census.

The current canonical Daily producer `broker/src/daily-canonical-authority.js:17–25,161,207–210` checks actual used hours against missing pay OR charge, returns RATE_MISSING/has_rate_issue when processed, and preserves UNPROCESSED where instructed. The generic authoriser refusal therefore genuinely occurs before the publisher. Current `broker/src/index.js:93941–93961` recognises the publisher's named bucket refusal only with a UUID Contract and tells the user to set Contract rates; it does not establish the required contractless override/default refusal. This is retained evidence for the scheduled DY-04 closure, not a DY-02 Worker change.

Final raw source hashes:

| Source | SHA-256 |
| --- | --- |
| 0925 successor | 39c1a1f69fc5c9a5152aa08ca037e295e6e08b64bbd0651177556fa6fe52c99a |
| final-definer registry after narrow advance | 61455513e90f34d4bab3e0d3ceb0482158ff729f30630ce284a6baa011c94c0a |
| canonical Daily producer | 64cd2109da90f9ee277c2829a734683e7e06649ebebe14b1e2d296ac1c04d00b |
| Worker index read-only context | 5bc63c0e68c95913535e0a8947c0bbe25bfb899005ef15b811b4d14b798bf40d |

No further essential D01 source defect was found. Local compile, catalogue/contract verification and source-gate results are root-owned evidence; this reviewer executed none. D02's future DY-03/DY-04 dependency and all D03 runtime clauses remain open.

