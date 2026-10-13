# WB-29 frozen rate transport source review

13 October 2026. Independent read-only source review against the saved transport design. **Final scoped SOURCE PASS — all six findings closed; no runtime, SQL, fixtures, rendered screens or business calls performed.** Root alone authors product fixes. WB-29 as a whole remains open. Earlier OPEN checkpoints below are retained as review history and superseded by the final closure.

## Initial concrete findings

- **TFR-01 — Office rejects every selected treatment.** `js/bpay-next-office.js:2016` compares `selected_treatment.run_work_id` with `line.run_work_id`, but the actual closed preparation `LINES` row (`:317`, mirrored in Worker `bpay-next-preparation.js:364`) contains no `run_work_id`. Retain the native run-work ownership proof and compare only fields available in the selected frozen line; do not extend the existing LINES contract for this comparison. Source/target channel, source-consumed amount, captured revision, component and frozen quote are available operands.
- **TFR-02 — first advice detail page omits its required null cursor.** `broker/src/bpay-next-frozen-advice.js:196–200` turns the raw detail body into native args before the shared `pageCall` defaulting. The unchanged fixed advice RPC requires explicit null `p_after_detail_no`, `p_after_break_no`, `p_after_bucket`, or both rate-schedule cursor fields. Every first nested detail request is rejected before network. Normalize the closed request through the same original `pageCall` semantics before dispatch; keep original instruction-only admission.
- **TFR-03 — touched advice transport still admits PAYE only.** HEADER uses `target_pay_channel:'paye'`, its input enum omits `UMBRELLA`, entered-net qualification equates null solely with CASE_PAYOUT, and final membership expects one net adjustment for every OWN destination. Native original advice admits Umbrella instructions, whose entered PAYE net is null and which have no payroll NET_ADJUSTMENT. Make the exact native channel/input qualification conditional; preserve PAYE and one-off checks.

## Source checkpoint

| File | SHA-256 |
| --- | --- |
| BE `broker/src/bpay-next-frozen-read.js` | `27cf7e430574bef3465b195a8e9e784054c6281e6044412b8e64f699506251bb` |
| BE `broker/src/bpay-next-frozen-advice.js` | `30e5bb1380a4f30ac73fe6b6a900d47ced523361944e9d6cb66958c783e8dae9` |
| BE unchanged `broker/src/bpay-next-frozen-advice-rpc.js` | `acf94f66927911c3252f2f59d5e4bc4ab2ec4ae3d7e550e4d4d0100b8bf80549` |
| FE `js/bpay-next-office.js` | `8eeffcb435c5ee4ce807701126ab8a1d2b2791882cd9777de7762efdffe4ca7a` |

The two selected-treatment validator bodies are copied identically. RATE_DETAILS alone has the extra native/HTTP envelope key; original source rows/cursors remain unchanged. BigInt target penny conservation performs validation, with no browser pricing. Detailed native ranges/null qualification and final repaired integration remain under review.

## Fresh repair check and remaining ranges

TFR-01 and TFR-02 are **closed on fresh source**. Office now compares the available captured revision/component, worker/Candidate, source channel/source-consumed amount, target and frozen quote; absent run-work identity remains native SQL proof. Advice now calls exported `originalAdviceDetailCall(request)` before its original instruction RPC and passes its normalized `call.args`, including every explicit null cursor. The shared response decoder retains original row/cursor validation and charges the assembled response budget.

- **TFR-04 — family-specific native code length.** The selected validator uses 245 characters for all rate families. Native ADJUSTMENT permits 256 (`2100` basis identity and actual component key; `2044` table CHECK), while the existing W1 transport restricts only ADDITIONAL to245. A legal long ADJUSTMENT must not become an invalid frozen response.
- **TFR-05 — selected quote rate is not a raw rate operand.** The validator uses one16-integer decimal limit for quote rate and native unit/rate operands. Native `1157` calculates suggested rate using `round(ex/units,6)` in unconstrained numeric; composer2357 bounds quote money but does not impose16 integer digits on that rate. Existing W1 quote validation accepts canonical six-decimal text within the native120000/whole-page budget. Keep that actual selected-rate contract separate from original units/source/manual rate fields, which native2100 qualifies below1e12 with at most6 decimals. Target money remains16 integer digits/two decimals.

Exact native null/mode qualifications reviewed: manual rate versus amount operands are mutually exclusive; NATIVE_DEFAULT is the uncovered STANDARD-only suggested path; consumed-source zero is the native zero-included-units/source/target tuple with selected rate null. Nullable units/source evidence for honest nonzero unqualified amount-led or mixed manual-rate paths must remain nullable. Source bucket amounts are not summed to replace the single authoritative component source reserve.

Fresh TFR-01/02 closure hashes: BE frozen-read `755c2b4639a477c0563f4439e896c00ac4945c788e79a91eb88125c69327277d`; BE advice `90942d76c51ce048033749a113393af5e2e2eca8984fdeae4b049cd29dc700d9`; FE Office `3c6621aeb67ca35d7f8f204436d8b57004f9f2a1ff02d0d159384319d8d12b40`. These supersede the initial checkpoint for those fixes only; TFR-03/04/05 remain pending repaired source.

Native SQL0028 confirms both original reader variants expose selected metadata only on RATE_DETAILS, with original line/worker/quote/policy/date ownership qualification and metadata charged before source rows. Its original advice authority explicitly admits `UMBRELLA`, requires null entered PAYE net/request command and cash equal frozen gross-inc VAT. This review requests no new Office renderer under WB-29 Screen:none.

## Repaired source check

TFR-01 through TFR-05 are **closed** on the latest three-file source. The BE/FE selected validator bodies compare exactly after newline normalization (5015 characters each). ADJUSTMENT now admits256, ADDITIONAL245, STANDARD only the five native bucket codes. Unit/source/manual rates use12 integer digits/six decimals; selected quote rate permits its actual canonical six-decimal native text under120000 characters and the whole-response byte limit. Both validators qualify STANDARD-only NATIVE_DEFAULT, exact positive immutable decision versions and outer/bucket policy-window equality. Consumed-source zero requires actual zero included units/source/target and null selected rate, while honest historical/unknown nullable evidence remains nullable for nonzero source.

Advice now admits the native closed PAYE/UMBRELLA channel and input enums. PAYE retains its original CASE_PAYOUT/null-entered-net qualification. Umbrella requires input UMBRELLA, null entered net, OWN destination and frozen projection cash equal gross-inc VAT. Original positive instruction/leg cash, metadata, one-off Case restrictions, counts, immutable worker/line qualification and actor admission remain. Final net-adjustment cardinality is one only for PAYE OWN, zero otherwise.

**TFR-06 (last narrow null check):** the actual1157 MANUAL_AMOUNT result always has `quote.rate:null`, independently of source reserve amount. The selected validator currently allows a numeric quote rate for a nonzero-source MANUAL_AMOUNT. Require null for that mode; this is a native result validation condition, not pricing. Root has been notified. Final SOURCE PASS awaits that one condition.

Latest checked hashes: BE frozen-read `9db284d43b464307219270aa55c7059d036bec8751ba0c310c3ce577d86e1fe2`; BE advice `00bda626dc69226264e814cfc3fb5ab41cc159bc35ca58ac681ae93d1e25f021`; FE Office `606f27752a1df1b15303ec986fec477a5fb0444b0b20292cdfd385f68598e725`; native SQL0028 `bb2bf3e049eb042036cb414e4801dbd163ac382b85123f50b2561df07d0746c7`.

## Final exact-source closure

**SOURCE PASS; zero remaining concrete findings in this transport slice.** TFR-06 now requires null quote rate in MANUAL_AMOUNT, as the unchanged1157 arithmetic emits. Fresh BE/FE validator bodies are exactly equal after newline normalization, 5038 characters each. This final verdict supersedes every pending checkpoint above.

The closed17-field envelope and15-field bucket records match native SQL0028. Required UUIDs, component identity, policy/window/date, exact source/target enums, target Umbrella nullable pair, immutable decision pair/version, family-specific native codes and all decimal/null/mode fields were checked. Bucket identities are unique; BigInt checks only target penny conservation and VAT applicability. No multiplication, division, conversion, inferred Source amount, replacement source reserve or live pricing occurs in these transport additions.

Standalone frozen-detail keeps exact native4/HTTP7 fields for non-RATE kinds and native5/HTTP8 for RATE_DETAILS. Advice uses its own exact native2/3 envelope, current fixed instruction-only RPC, admin admission, abort propagation,8-second per-page deadline,128KiB page bound and≤100 native rows. Shared normalization preserves every original null cursor; shared decoding preserves original source rows/order/endpoint checks and exact continuation. A non-RATE extra metadata key is refused. Nullable historical/no-reference metadata remains explicitly null.

Advice consumes RATE_DETAILS page-wise, requires identical selected metadata across continuations, binds the first selected envelope to the original actual header/line, prints escaped native selected decimal facts once, and independently retains the original expected source-rate-row count. A valid zero source-row page still processes selected metadata. One current nested page and≤5 selected buckets are retained; streamed HTML still uses the original16KiB chunks and incomplete/error marker. Umbrella header qualification and zero payroll-adjustment membership now follow the existing native authority; PAYE and one-off guards remain.

Office preserves selected metadata through retained pages, compares it on continuation and binds only actual available frozen-line/worker/Candidate fields after the existing current-context check. The missing run-work field is no longer referenced. The original five-tab renderer, captured used-line facts, detail recovery/currentness, source bucket cursor, request/journal behavior and actor/NEXT admission are retained. No Office selected-treatment screen is added or claimed; WB-29 specifies Screen:none.

| Final file | SHA-256 |
| --- | --- |
| BE `broker/src/bpay-next-frozen-read.js` | `ab352c72df54319c2c2d46dc9f3dfcd36959a1b45f5dd9ca382e110bb5de7b5c` |
| BE `broker/src/bpay-next-frozen-advice.js` | `00bda626dc69226264e814cfc3fb5ab41cc159bc35ca58ac681ae93d1e25f021` |
| BE unchanged `broker/src/bpay-next-frozen-advice-rpc.js` | `acf94f66927911c3252f2f59d5e4bc4ab2ec4ae3d7e550e4d4d0100b8bf80549` |
| FE `js/bpay-next-office.js` | `1dbe0073a2e226d11fd7778f43a347b5264da57bfe9eeae0525b966ba0401b29` |
| Native installed SQL0028 reference | `bb2bf3e049eb042036cb414e4801dbd163ac382b85123f50b2561df07d0746c7` |
| Reviewed transport design | `911f5637af74df97958e444b90bfe8eff151d93ad82eb5e4007da10971642650` |

This is a source-reading receipt, not joined runtime/visual acceptance or whole WB-29 completion. Root-reported checks/installation are not independent business execution evidence from this reviewer.
