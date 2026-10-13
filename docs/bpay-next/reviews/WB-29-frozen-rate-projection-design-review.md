# WB-29 frozen rate/advice projection design review

Verdict: **DESIGN PASS — 0 open findings.**

Reviewed 13 October 2026. Independent review of `design/WB-29-frozen-rate-projection-join.md` against the actual saved 0620 frozen reader, 2018 original CSV advice reader, unchanged 0025 four-column rate-detail SRF, and current authored 2357/0005 captured composer and line joins. Approval covers this proposed reader/closed-transport slice only. No product edits, SQL/RPC, data, fixtures, tests, browser or hosted actions were performed. Implementation and runtime acceptance remain NOT RUN; whole WB-29 remains open.

## Exact evidence

| Saved source | SHA-256 |
| --- | --- |
| Reviewed design | `69D3DF477F00AA6A0E69020650E53320998392068998FD0CB3E95A4CE5FF05FF` |
| 0620 frozen public reader | `96A26C83328DBB7253E8526C789DD11544D04D1B8A9DCF1B421E946FCD7DBC65` |
| 2018 original advice public reader | `7433AC2A1CCFE64613666EED4FF672E81C67A36BB8C9ABBC6585833B9DCBE09B` |
| 0025 private frozen-detail SRF/guard | `D7433FD91345A02EA75A596577C66514465C007D622DD0D225CA7EDF96708CE9` |
| Current authored 2357 captured composer | `E45C85D73006720CAAA39622DD7CC6560EDEDBCCDEC830B709FEC8E91F773052` |
| Current authored 0005 hold/line refs join | `5DD92083243DDE0BD41418DA6436D28D1566C32C1B0A0A540D9A8D4AE742F66A` |

The last two hashes refresh the earlier checkpoints printed in the design's evidence table. The current saved bodies were inspected for this review; the design's proposed joins remain compatible with those bodies.

## Required boundaries verified

| Check | Result | Assessment |
| --- | --- | --- |
| Scope and original rate rows | PASS | The only new public property is `selected_treatment` on RATE_DETAILS. Existing rate-detail row fields, native bucket seek/order/limit, all other reader kinds and the private four-column RETURNS TABLE remain unchanged. No SRF recreation or new reader kind is needed. |
| Exact captured subject | PASS | RunLine → exact `(run_work_id,component_key)` RunPosition → RunWork/Worker/Run is qualified before quoting. The design requires exact Work, Candidate, revision, approved line, source/target tuple, policy/window/date, line refs, Source consumption and frozen target equality. The current composer and 0005 line insertion supply these existing native facts. |
| Original arithmetic and full quote | PASS | One call to the captured full-component facade with NULL prefix supplies the quote and bucket facts. The projection does not divide amounts, infer units, prorate money or introduce another calculator. Original 1157 arithmetic stays with the composer. |
| Captured references | PASS | Nonempty immutable line refs must equal the composer's original component subset. Exact retained ID/version is used even after a later CLEAR. No ACTIVE lookup, current Candidate/Contract schedule or current Position is used to choose a replacement treatment. Failed qualification refuses instead of falling back to source-only display. |
| Historical and empty refs | PASS | NULL historical refs and original `[]` refs retain `selected_treatment=NULL` and their existing rows/packing. No captured-composer call is made on those paths. Source-channel presence alone does not admit taxable composition. |
| Bucket shape and unknown facts | PASS | At most five unique native `(rate_family,rate_code)` facts are projected explicitly. PINNED has a jointly non-NULL exact decision pair; NATIVE_DEFAULT has a jointly NULL pair and no invented CM receipt. Approved units/source rate remain separate from Included units/selected target rate. Nullable attributed Source and raw manual operands remain NULL when unproved; component Source is charged once, not summed from repeated diagnostics. |
| Zero and nonsegment components | PASS | Genuine zero retains zero component amounts and NULL selected rate. Additional/Adjustment may have treatment metadata with empty original STANDARD rate rows; no DAY row, placeholder quantity or inferred unit is introduced. Reimbursement/Case/no-ref paths retain their original owners. |
| Advice admission | PASS | Original instruction → BANK transfer → sealed projection/Worker/Run → actual WORK member is the authority. The 2018 issuance gate deliberately survives later status/account/projection changes. The READY-only 0025 guard must not be called by this advice join; the design explicitly preserves that distinction. |
| Standalone admission | PASS | The 0620 standalone detail continues using its original READY Worker and REVIEW/DRAFT Run gate. The advice gate does not broaden standalone availability. |
| Paging and byte budget | PASS | Full captured qualification occurs before original source-row paging. The metadata is identical for one frozen line across pages, included even on an empty rate page, and encoded once against the existing 120,000-byte response budget. Existing first-row overflow refusal, byte-short continuation and exact-full-page semantics remain. The private 8 MiB working set and prefix targets are never exposed. |
| Closed transport | PASS | Worker/Office admission must be branch-specific: selected_treatment is accepted only for RATE_DETAILS, with strict keys, bounded facts, decimal text/qualified NULL and exact identity/context checks. Other branches retain their original closed envelopes. No generic opaque private JSON passthrough is permitted by this design. |

## Implementation review boundary

The implementation should retain the exact checks above before emitting the small metadata object, keep complete original reader bodies outside these named RATE_DETAILS seams, and retain existing public ACL/actor/NEXT checks. The selected envelope is factual captured metadata; its internal valuation date does not authorise displaying a pre-execution payment date. A later renderer may present Original/Selected facts but cannot manufacture target rates or paid units.

No further design correction is required. Actual successor SQL, closed decoder mirrors, full-body parent comparisons, final byte accounting and runtime joins require their own source/owner verification before completion.
