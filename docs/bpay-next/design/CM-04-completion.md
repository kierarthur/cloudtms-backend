# CM-04 — shared calculator source gate

11 October 2026, sequence 78. Exact objects read before build: D01 the existing `pay_finance_selected_quote_v1` and `bpay_next_value_source_for_target_v1`; D02 a final-source check under `tests/`; D03 the future joined checklist. Dependencies F-04/F-05/F-06 are complete. OR-CM-01 in `../B5_OWNER_APPROVALS.md` approves the lifecycle note (SHA-256 `a2d0febe10fef5698a4beda9825cc9827ba272b090ded56960a30115e23bf08e`); CM-04 remains an independent shared primitive.

**BUILDER DECISION BD087:** use the current F-04 registered physical files containing final NEXT/comparison signatures, including the shared quote's registered physical home. Filename-only `*bpay_next*` matching would omit the real `banking_pay_next` files and `1157`. This is the narrowest complete card scope: 114 unique current physical files, with no predecessor/include-history expansion or second winner registry. The auditable formula patterns live in the test file. Only the five exact helper bodies are exempt. Source checks allow contextual classification, chosen manual units/rate, saved-value ratios and rounding, while rejecting copied ERNI/VAT/margin formulas and wrong first valuation arguments. No producer, money-owner replacement, table, trigger, command, Worker route or screen is added.

D01 code-reading confirmation; installed source retained unchanged:

- `supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql:43` — selected quote PAYE→Umbrella calls `public._pay_convert_paye_to_umbrella`; reverse calls `_pay_convert_umbrella_to_paye_ex` at 46; same-channel Umbrella uses `_pay_umbrella_vat_calc` at 48. Manual replacement rate uses explicit units at 54 and VAT helper at 56; manual amount at 59 and VAT helper at 61. Suggested derived unit rate and identity rounding remain caller evidence, not a replacement conversion formula.
- The same `1157:982–985` — NX-028 / SCX-066 automatic valuer calls the shared quote explicitly in `SUGGESTED_EQUIVALENT_BASIS`, with `p_source_ex_vat` and null units/manual inputs. Immutable policy-window selection remains at 965–979; outputs are read directly at 986–988. No inline ERNI/VAT formula or live ledger fallback is present.
- Existing five helper declarations are in `26052026_2100HRS_NEW_FUNCTIONS.sql:518,531,569,601,620`. Percentage normalisation preserves 15=0.15 and 1=100%; helpers own conversion/VAT and round to two decimals. No helper is rebuilt. See `CM-04-source-investigation.md` for exact current source references and all eight valid caller bases.

D02: `tests/bpay-next-shared-calculator.test.js` scans only those registered physical sources and is imported by the existing push/protected-release source gate test. It reuses the existing executable SQL body scanner, exported without a behavior change. Checks use source text only, including copied inline percentage, fractional/multiplier aliases, hard-coded conversion, VAT, margin, JSON input, complete nested/named first-argument refusal and exact helper-body exemption. A mutation of the real `1157` source is rejected in memory; no file or database is changed for that control.

Normal checks: 336 migrations / 882 repeatables pass `db:check`; sole local PG17 CHANGED load reports zero migrations and zero closures because product SQL is already correct. JS syntax and source gates pass. Contract export/seal remains `129cb378690580aed31d2c95758492b18bbccb8bc38fb9947b4b6cae284e3265`; coupling passes. No public SECURITY DEFINER membership change; existing browser-isolation constants remain applicable. Independent review is in `../reviews/CM-04-shared-calculator-review.md`. Local source/compile evidence does not claim financial first use or hosted acceptance.

CM-04-D03 future joined in-app checklist — **AUTHORED / NOT_RUN**:

- [ ] After the owner's single release go-ahead, compare PAYE→Umbrella, Umbrella→PAYE and both same-channel cases using one normal joined app receipt.
- [ ] Umbrella £20 at ERNI 15% yields the existing reverse helper's £17.39 suggestion on review and history; accepted manual £17 still governs when selected.
- [ ] Verify 15 and 0.15 give the same two-decimal result, and 1 retains its established 100% interpretation.
- [ ] Verify all automatic/manual selections agree with the shared helper once, with no Worker/browser/CSV money calculation. Reuse that receipt for the explicit clauses; create no local fixtures or proof programme.
- [ ] Worker, queue, app, release and installation evidence remains NOT_RUN until the authorised normal journey.
