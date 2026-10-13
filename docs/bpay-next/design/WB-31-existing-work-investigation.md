# WB-31 existing work and implementation boundary

13 October 2026. Read-only preparation for approved sequence **142**, after WB-30 and before WB-32/WB-33/WB-34. Root alone writes product. This note uses current saved source and the registry; it makes no installation or joined application acceptance claim. No SQL/RPC, business data, hosted action, tests, fixtures or screen generation were run. Future application checks are **NOT_RUN**.

## Binding scope and checklist

Read the complete approved `02_WORKBENCH.md:1168–1191`, all eight WB-31 checklist rows including the ITEM row, `review/R2.md:264–266` / R2-F03 and W12–W14, and `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:224–228,525–527`. Dependencies are WB-29, F-16 and WB-45. The card requires a single-component suggestion/manual-rate/manual-amount flow, a non-bucket gross-total flow, server effect/charge/margin, actual figure/work confirmation, explicit zero, manual reason, stale review protection and explicit linked opt-in. Typing only validates format, blanks and reason; it neither prices nor calls the server. Client-charge changes never change approved worker pay (`02_WORKBENCH.md:1171–1189`).

| Checklist object | Current reuse / remaining responsibility |
|---|---|
| D01 `private.pay_finance_selected_quote_v1` | CONTEXT, duplicate of WB-27-D04. Reuse actual pure1157 quote and mode validation; no second calculator. |
| D02 `public.bpay_next_workbench_command_v1(jsonb)` | CONTEXT, WB-19-D11. Current2320 typed SAVE/CLEAR, original request receipt and ordered affected controls remain the write owner. |
| D03 `private.bpay_next_component_resolution` | CONTEXT, WB-45-D05. Current2044 schema and subsequent native constraints retain independent Work/line/bucket/economic target/version and based-on-to-published continuity. No new decision store. |
| D04 shared1157 residual contract | CONTEXT, WB-27-D08. Current input quote consumes actual residual native units/source; full component quote and pro-rata substitutes remain prohibited. |
| **D05 amount-led/manual rate/suggestion decision window** | **Only assigned DELIVERABLE.** Build the native single-component and gross-total UI, reusing existing W1/W2 DTO/controller and F-16 owner address. Card-required missing W1 preview facts or linked discovery are narrow supporting changes to that existing owner, if needed. |
| D06 Additional/expense/mileage classification | CONTEXT, WB-03-D04 / DR-38. Reuse current1419 Additional TAXABLE and expense/mileage NON_TAXABLE assignments; no publisher change for WB-31. |
| D07 output-only card | CONTEXT/specification, not another implementation or proof gate. |

The checklist's historical MISSING/LOCAL_ONLY labels do not override current registered owners and do not prove application acceptance. No new design approval is required by the card (`:1187,1191`).

## Exact current owners

`docs/bpay-next/final-definer-closures.json` resolves these current signatures. Dates below identify actual source files, not installation inference.

| Exact signature | Current `supabase/repeatable/` file |
|---|---|
| `public.bpay_next_workbench_page_v1(jsonb)` | `13102026_0117_banking_pay_next_chosen_display_read_barrier_v1.sql` |
| `private.bpay_next_resolution_context_v1(uuid,jsonb)` | `13102026_0201_banking_pay_next_bucket_resolution_review_v1.sql` |
| `private.bpay_next_resolution_scope_validate_v1(jsonb,text)` | `12102026_2241_banking_pay_next_resolution_context_v1.sql` |
| `public.bpay_next_workbench_command_v1(jsonb)` and `private.bpay_next_resolution_command_v1(uuid,text,jsonb,boolean)` | `12102026_2320_banking_pay_next_component_resolution_v1.sql` |
| `private.bpay_next_component_resolution_basis_v1(uuid,uuid,text,text)` and `private.bpay_next_component_resolution_active_v1(uuid,uuid,uuid,text,text,text,uuid,boolean,uuid,date)` | `12102026_2100_banking_pay_next_resolution_remaining_quote_v1.sql` |
| `private.bpay_next_component_resolution_need_v1(uuid,uuid,uuid,uuid,text,uuid,boolean,date)` | `12102026_2146_banking_pay_next_treatment_resolution_need_continuity_v1.sql` |
| `private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric)` and `private.bpay_next_component_resolution_quote_v1(uuid,bigint,uuid,uuid)` | `12102026_2357_banking_pay_next_captured_resolution_composition_v1.sql` |
| Same input quote with trailing `uuid,text`, and trusted current-working overload with trailing `uuid,text,jsonb` | `13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql` |
| `private.pay_finance_selected_quote_v1(public.pay_finance_component_resolution_mode_enum,numeric,text,text,numeric,numeric,numeric,numeric,numeric,boolean)` | `06102026_1157_banking_pay_shared_selected_quote_v1.sql` |

Current0201 retains the complete2302 explicit-context body apart from its two finite WB-30 READ delegates. The independent inverse/catalogue receipt is `reviews/WB-30-reader-final-review.md`; its reader-slice verdict does not approve the future WB-31 UI.

## Reusable native semantics and transport

`2100:8–61` derives actual `original_basis` from the sealed approved revision/line/rates. Known positive units plus approved worker/charge basis distinguish RATE_BEARING from AMOUNT_LED. Standard, Additional and Adjustment are supported typed families. A gross-total reason is not permission to treat a genuinely rate-bearing Additional row as amount-led. Use native semantics to offer the permitted controls; do not decide modes from the window title or component name.

`0045:218–232` already refuses manual amount on RATE_BEARING, replacement rate on AMOUNT_LED, blank/nonfinite/negative/overprecision values, missing manual reason and ambiguous manual amount residual. Rates have native six-place precision; amounts two-place precision; reasons have a 2,000-byte ceiling. `0045:388–413` computes the effect once through shared1157 using the actual selected units and qualified policy window. Known native zero is separate from unavailable source/residual. Current2320:158–199 independently validates requested owners/quotes before mutation and requires `zero_confirmed` for a zero SAVE.

BE `broker/src/bpay-next-resolution.js:95–174,229–271` and FE `js/bpay-next-resolution.js` already carry closed decimal-string operands, original basis, available rates, suggestion/unavailable code, expected owner/version, saved decision/quote, explicit preview/unavailable code and zero-confirmation fact. An explicit W1 read with an intent performs Preview; `RESOLUTION_SAVE` is Apply. No new command kind or route is needed. The FE controller retains original namespace/body/UUID before sending, performs readback after unknown outcomes, and only retries the original bytes on explicit retry. Reuse that controller rather than create another journal. Stale context/owner evidence must retain inputs and require explicit refresh; unavailable suggestion never becomes zero.

Original worker basis remains independent of today's Contract availability. Existing W1 distinguishes captured approved basis from available current rates; do not infer a new approved worker rate from changed charge or availability. Existing E2/Rule B still holds back or system releases changed Work after accepted Create; WB-31 cannot revive or reprice a frozen Draft.

## Concrete remaining seams

1. Add the single-component native window/row launchers using actual `work_id`, current revision, `approved_line_id`, native family/code and expected decision pair. Existing F-16 pay-line reads and Action/Candidate addresses provide those identities. FE `bpay-next-workbench.js:1230,1311,1538` currently opens only `CloudTMSBpayNextRates` for Standard `RATE_RESOLUTION_REQUIRED`; it does not implement the WB-31 gross/amount-led flow. `index.html:3043` loads that bucket module. Preserve existing native frame/actor/epoch/current guards.
2. Reuse the WB-30 frame, input retention and result handling where appropriate, but not its Standard-only `selectedWorks()` (`bpay-next-rates.js:32–35`) or bucket table. The current bucket module's discovery and evidence describe Standard rate needs, not complete Additional/Adjustment linked scope. If linked scope is offered for those families, extend only existing RESOLUTION_CONTEXT with bounded, positively qualified native discovery and explicit exact owner/version membership; an unknown or oversized set must be unavailable, never silently omitted. The explicit W1 request/W2 write shape already accepts typed non-bucket rows.
3. Current explicit preview supplies `preview.quote.ex` and `remaining_units`; `original_basis.source_charge_rate` is a per-unit approved fact, while available current charge is separate provenance. It does **not** currently return an explicit charge-total/margin pair (`2241:176–284`; current0201 inherited branch; `0045:409–413`; closed DTO `bpay-next-resolution.js:254`). Full card `:1171,1173,1184` requires those server facts. Supply only the necessary authoritative preview metadata through existing W1/native qualified basis, with honest unavailable values when its original charge amount is unbound. Reuse native shared quote for pay; never multiply/subtract money in the browser or substitute current Contract charge. WB-30 grouped margins are not automatically the single-component effect, especially with linked members or partial residual units.
4. Suggested/manual modes and linked consent must be explicit. Invalidate any shown effect after changing value, reason, mode or linked scope; Show effect/Apply are explicit operations. Confirm the server figure and exact affected Work, with a separate deliberate-zero question. Expense/mileage do not enter channel conversion or gross-total suggestion.

No scope justification exists here for a new table, money owner, producer rewrite, generic pricing/selection framework, parked F25 owner, WB-32 channel Case flow, WB-33 Clear UI, or WB-34 adoption/global stale-save rewrite. Existing SAVE receipt/recovery is reused for this flow. SCR-23b's Clear button is WB-33, not an excuse to build it early.

## Supplied screen mapping

Read-only pack `SCREEN_DESIGN_POLICY_20261010/SCREEN_INVENTORY.md:63–65` and `DESIGN_INDEX.md:37–39` assign WB-31 to **B2 SCR-23 / SCR-24 / SCR-25**:

- SCR-23 manual rate; SCR-23b saved amount; SCR-23c blank rate/reason errors. Their review notes explicitly disable manual amount for hourly pay and manual rate for fixed amount, keep linked opt-in, and obtain effect only on Show effect.
- SCR-24 Resolve gross total; SCR-24b unavailable suggestion/manual entry. Native owner restrictions still govern genuinely rate-bearing non-bucket rows; no unavailable suggestion may submit zero. Expenses/mileage never use this flow.
- SCR-25 exact rate/work/pay confirmation; SCR-25b deliberate zero with safe Go back.

Associated SCR-28 belongs WB-33 and SCR-29 belongs WB-34 (`SCREEN_INVENTORY.md:68–69`); their future controls are not WB-31 deliverables. Pack composite background totals, timestamps and later controls are not permission to add reader fields. `SCREEN_DESIGN_POLICY.md:172` exempts dialogs/expanded children/fixed business order from server sorting, so this card needs no new sorting system. No images were generated or pixel acceptance claimed in this investigation.

Source checkpoint:0201 SHA256 `A482A5AEED03A9C2E0206DA41A26370DBD59E5E1A4D8601E621DA65779546981`;2320 `1DD72479BA41C1997439F80B8A2546D285F29CD1F791BBDF28DC2FC615385CE2`;0045 `6488C85F18850119D0A64812E3420C7AAC765A4801924AF737E08D9BEA39960B`. These are current read bytes, not a freeze of future Root edits.
