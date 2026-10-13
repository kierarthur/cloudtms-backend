# WB-31 transport source review

13 October 2026 / sequence142 / BD172. Independent read-only review of BE `broker/src/bpay-next-resolution.js`, FE `js/bpay-next-resolution.js`, native0314 component/effect payload and the approved boundary/card. Root alone edits product. No product, database/RPC, business, test, fixture, browser or hosted execution was performed. UI/A4 and the full SQL owner's review are separate.

## Current checkpoint

**OPEN for T31-01/T31-02 only.** Root acknowledged both and is implementing finite corrections. All other reviewed closed contract additions fit WB31. Static text comparison confirms the entire shared BE/FE validation block is identical after export/newline normalization. The complete original FE command-controller body is identical to HEAD; original namespace, persistent bytes, readback/retry, receipt and unknown-outcome semantics are unchanged.

**T31-01 — bound the complete new envelope.** `componentResponse` validates the stripped context/works envelope through the original 120,000-byte decoder, but does not cap the full payload including `component_review` header/exclusions. The actual outer read transport allows 131,072 bytes. Add the same complete `JSON.stringify(payload)` UTF-8 <=120000 check used by the bucket decoder to both mirrors. Native0314's output bound does not replace consumer validation.

**T31-02 — unavailable suggestion must not erase actual amount-led residual matching.** The initial component signature reads residual/included/charge only through suggestion/effect.suggestion. Those paths become NULL when suggestion is unavailable, allowing different actual native Position residuals to share the same otherwise equal amount-led signature. The approved design requires actual monetary basis matching regardless of suggestion availability. Root's proposed finite correction is sufficient: add the already-qualified native context Position A/R/H/source-basis-channel tuple plus original approved units/charge basis to AMOUNT_LED matching, in native linked discovery and both transport mirrors. Keep actual suggested amount comparison when present. This is validation of existing native operands; no browser residual/pricing calculation, storage or producer widening is needed.

## Completed source census

New input has exactly one nested `component_review` variant with six closed Candidate/Work/revision/line/family/code fields. Original 32KiB request limit, canonical UUIDs, native Standard/Additional/Adjustment code ranges and aggregate 100 flattened rows remain. Output echoes exact root identity, root single-row membership, Candidate/actor/NEXT context, nullable Contract equality, native physical/applied/policy identity and expected decision versions through the existing explicit decoder. Linked count is exactly Work count minus the original Work; duplicate Work/row scope remains refused. Exclusions use closed native reasons and qualified partial membership. Native header labels are bounded and never supplied as financial inputs.

Component-review rows require the new effect object. Original explicit/bucket response paths retain compatibility when the optional effect is absent; when present, it must have exactly suggestion/saved/preview. Each effect exists only with its corresponding quote; exact penny pay equals that quote's ex-VAT value. Charge/margin state is jointly known or NULL: approved charge requires signed margin equal to charge minus pay, unavailable charge requires NULL margin. These BigInt checks validate server metadata rather than derive prices. Existing source/residual quote, selected mode/manual echo, native zero, available-rate provenance and historical NULL constraints remain unchanged. No extra effect data is copied into the existing W2 request.

Native linked signature is closed to family/code/component/tax/source/channel/Umbrella/policy/unit/semantics/rate/charge/availability/window/suggestion plus amount basis. Exact target/date remain shared context and saved decision qualifiers. Linked ACTIVE decisions are refused by the branch. Amount-led original/source/residual matching needs only T31-02's independent tuple correction; genuine rate-bearing quantity variation remains permitted under exact rates. Required manual reason, source quantity and modes remain owned by the original native quote. No new fault pair, route, command, controller or journal is introduced.

## Initial reviewed hashes

| Saved source | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-resolution.js | `d9f2585ae6bd2ba4791db674771b0f009c391bd98b7cafdc11965926fd408196` |
| FE js/bpay-next-resolution.js | `3ac881a1ca6ae814bbbdc0ccc6986d75b25042745aa7a5313859434d79b02bc7` |
| native0314 single-component review | `aa8e7a1c5d1ac2d6ce5f4a97a270b7b35923d70bff6b5bd5b9ac28738b059192` |
| WB-31-build-boundary.md | `2ae3e7cbf4dc348859f7afe026cdb05520074afa2ec1c5f376994e822d7b26cd` |

Full source, compile, A4 and joined completion are not claimed by this checkpoint. Root remains the sole product author.

## Final transport override

**SOURCE PASS for the reviewed transport slice; T31-01/T31-02 CLOSED, zero open transport findings.** The new component envelope is checked in full against 120,000 UTF-8 bytes before delegation, including header/exclusions. Both mirrored validators now pass the exact already-qualified context Position tuple into AMOUNT_LED signatures; `amount_position` and original `amount_quantity` remain present even when suggestion is unavailable. The sealed approved charge rate is already an independent signature field. Native0314's root and linked comparisons use the corresponding actual context Position and approved units. Differing residual/charge operands therefore cannot match merely because suggestion/rate paths are NULL. No browser pricing or new DTO financial input was introduced.

Fresh static comparison again confirms complete shared BE/FE block parity and the complete original FE controller body unchanged against HEAD. Original explicit response compatibility, typed quote/effect NULL qualification, aggregate scope, actor/owner/context fences and exact original-byte journal/recovery remain as reviewed above. Full SQL, UI/A4 and joined runtime acceptance are separate; no execution was performed by this reviewer.

| Final reviewed saved source | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-resolution.js | `bb6826163c83090eb66642facb33f33cdbb16e886c58e608f767bf295dc78e7d` |
| FE js/bpay-next-resolution.js | `df4a0bec13a3d1dab602ea34c7009b20bca0511096caf4b796bff618c01bff57` |
| native0314 payload / amount-signature joins | `a78fe98f30e2c71df0cee118130faee4c21ee0e8b011d4fbc85a08d98b7ecac7` |
