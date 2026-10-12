# WB-29 captured working-set design review

Verdict: DESIGN PASS — CS01 closed; 0 open design findings. No product/source/runtime or whole-item approval.

Reviewed the saved working-set implementation note originally SHA-256 `21B6747E0B9933149F4EF640F7A5FE8AD6F6D0A6B9B5F910BF50D60C9B294E05`, then the exact CS01 amendment at line64, now SHA-256 `E35928E25151625C6B1384A296B2B59B6F3893116D8A3692F3E545057A9F387B`; also the approved captured composer, exact current edit map, WB-29 card/checklist and current2218/2302/1419/1236/1157 source. Root alone writes product. This reviewer read source and wrote this note only; no SQL, RPC, tests, business data, screenshots, hosted calls or mutations.

## Finding and verified closure

**CS01 — preserve the genuine authorised-zero frozen line.** The new note obtains `component_quote` from the final positive Included prefix, but a genuine authorised-zero capture has no such prefix. Current1419:1306–1311 explicitly sets `v_freeze_zero`; its original flow creates the frozen zero RunLine and advice facts, and1419:1435–1448 creates a financial hold only when Included Source is positive. Add the explicit zero result after complete native identity/capture/refs/mode/window qualification: zero ex/VAT/inc, no artificial rate or Source attribution, qualified zero Included/equivalent operands while retaining actual approved-unit facts, empty `prefix_targets`, and the existing frozen advice line without a financial hold. Unknown positive attribution must still refuse; it cannot enter this branch by being coalesced to zero. Original2218:89–93 is the existing zero admission/output precedent.

Closed: root added and this reviewer re-read the explicit genuine authorised-zero paragraph at implementation-note line64. It requires full native identity/mode/window/capture qualification first, zero component/qualified bucket outputs and empty prefixes, retains native v_freeze_zero advice without a financial hold, rejects non-NULL prefix and preserves scalar MANUAL_AMOUNT admission. The originally missing branch is now specified; no other design gap remains.

## Otherwise sound design boundary

| Subject | Result |
| --- | --- |
| One arithmetic owner | PASS: original mode/window/quote admission lives in one six-argument body; original four-argument entry is a thin NULL/NULL delegate. Unchanged1157 remains the calculator. Gather/facade supply operands and compose independent outputs only. |
| Accepted identity and currentness | PASS: exact immutable WorkChoice refs and frozen RunLine subset, native expected/captured revision, real RunPosition/RunWork/Worker/Run/Candidate and target/policy/date. No late ACTIVE or current Candidate/Contract substitution;2302 live snooze and1419 current Position/Source/lease guards remain. |
| Full captured qualification | PASS: complete physical parents before Included filtering; exact maintained counts, attribution completion, same parent/approved-segment identities and full/per-parent A/R/H plus four attribution conservation. Signed A/R and Included above algebraic residual apply only after this positive proof; scalar bounds remain unchanged. |
| Finite working scope | PASS: explicit builder choice 10,000 parent rows / 8,388,608 encoded working bytes, including refs, member cells, bucket plan and compact outputs. This is not an existing page100/reference100 theorem. Overflow refuses before financial use. |
| Bucket units and Source | PASS: same Included segment native hours/pay cells; intact Included manual vectors survive outside paid/held/excluded siblings. Suggested/default requires actual per-member Source roundtrip/conservation. Manual bucket Source may remain NULL. Exact finite quotient, multiplication and unchanged1157 roundtrip qualify one-bucket residual; unproved positive multibucket debit units refuse. |
| Scalar manual amount | PASS by retained2218 admission: genuinely amount-led original saved Position/full residual scope only. This extension does not allocate or scale MANUAL_AMOUNT across STANDARD shifts/prefixes. |
| Native defaults | PASS for this proposal: tagged in-memory equivalent-basis default, NULL decision pair, own genuinely qualified operands; no fabricated CM receipt or resolver state, no resolution of an unresolved positive sibling. Remaining full owner joins cannot be passed merely by this default. |
| Prefix and hold/origin conservation | PASS: one component invocation gathers once and returns compact cumulative targets; unchanged1157 quotes cumulative bucket operands, origin shares difference ex/VAT and verify final tuple against original line/ACTIVE hold. Actual Included Source is reserved once. Negative shares refuse without remainder spreading. |
| No new proof/protocol | PASS: actual native RunPosition/prefix inputs only; working JSON stays private ephemeral data. No browser working-set certificate, new table/cache/producer/queue/public protocol or frozen SRF return-type replacement. |

Source review must still verify bounded materialization (reject maintained count overflow before gathering and use a physical sentinel/completeness check), one ordered materialization rather than repeated full gathers, and compact result construction that does not repeatedly concatenate the entire growing working set. These are implementation checks, not a claimed timing proof.

## Current evidence

| Source | SHA-256 |
| --- | --- |
| captured-native-composer-design.md | F3B8EC7F2800C79C3E89A1F7DE801C56E896DA0FFFC6536A15B1107F7E820E9F |
| captured-composer-edit-map.md | F2761091803B770A6EA1394BEE344F7F11E219E7D4B7A48C51C97E5C0E762ED9 |
| current2218 input quote | 1F161C8A632DED5985BAC8CD37DB7CCA9DE189C7275B9E2309FE423E98F67A89 |
| current2302 wrapper | DD3A2BB6D0E0A262A65CDCB96C0620E6B5A0A84E0A518191DA1D6D15B6075B7B |
| current1419 hold | 16E437BB839CC8A91CD499303CD0D6D574797E3FB0DA6570AB24858D42B6CE87 |
| current1236 origin recorder | F74EA457BF77F0D85CE7F3F21089BDECF9D9D9121D4005850DE430D50AA7379D |
| unchanged1157 | AD1C7A92754F2937EDB642DD5EE387F62761EF241CEF77D3B792185B2EB22F3C |

Actual composer/hold/origin/frozen/estimate implementations and closed transport schemas still require independent source review. Joined acceptance and whole WB-29 remain open.
