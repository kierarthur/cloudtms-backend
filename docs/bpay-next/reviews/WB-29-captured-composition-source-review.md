# WB-29 captured composition SOURCE review

13 October 2026. **Current five-owner composition SOURCE PASS: CC01/CC02 remain closed; eligible-leaf assertion and CC03 zero-Header correction reviewed, no remaining essential finding in this slice.** Current root-authored, uninstalled `supabase/repeatable/12102026_2357_banking_pay_next_captured_resolution_composition_v1.sql` SHA-256 `E45C85D73006720CAAA39622DD7CC6560EDEDBCCDEC830B709FEC8E91F773052`. Earlier CC01/CC02 closure bytes were `1C4C166C42101DD9DA9721BE323B1468F9C8723B5608C2B8197FC6453AA176B4`; initial reviewed bytes were `B471393436B9D5DF13D3DFAB9A9AFFD61BFBE14D9638A82C335A69B28E104AC9`. Findings below preserve those historical checkpoints. This receipt covers the five private composition owners and their exact eligibility-helper dependency only. Whole WB-29, universal Create/materialisation/hold integration, remaining origin/frozen/estimate joins and business acceptance are not passed.

Reviewer read saved sources/design/schema and wrote this document only. No product change, SQL/RPC, compile, tests, fixtures, hosted/business call or database data inspection. Supabase skill security principles were applied to the static review; task authority forbids its execution workflows. The reviewer previously authored the captured implementation/edit-map advice; this relationship is disclosed, and the separately saved working-set DESIGN PASS remains distinct from source approval.

## Historical findings and verified source closure

### CC01 — closed: prefix metadata extends beyond the selected prefix

Current2357:468–476 stores the requested prefix's top-level bucket quote and component quote, but returns the final loop's `v_bucket_facts` and all `v_prefixes`. With two positive Included members and a request through the first, `component_quote` is the first prefix while `bucket_details` contains both members' cumulative units/Source/quote and `prefix_targets` includes the later member. This contradicts the working-set design's “only the relevant compact prefixes” requirement and can make frozen inspection/receipt consumers display metadata inconsistent with the selected quote.

Small correction: retain the selected prefix's bucket facts at the same branch that stores selected component/requested quotes, and return compact prefixes no later than the exact requested native key. NULL-prefix calls retain the complete component facts/all prefixes. This changes only metadata composition, not1157 or native Source reservation.

Verified final closure:119 declares selected bucket facts;468–469 captures them at the same selected native member as component/requested quotes;475–480 restricts emitted compact prefixes to the exact through-key and returns those selected bucket facts. NULL/full calls retain final component facts and all prefixes.481's final8MiB check now includes the encoded raw plan too. No quote formula or native financial mutation changed.

### CC02 — closed: prefix facade performs an uncapped parent aggregate before the gather's bound

Current2357:510–513 aggregates every parent up to `p_through_segment_key` directly. The capped materialized census at43–83 and the maintained10,000-member refusal at34–35 occur only after that aggregate, when515 delegates to the input helper. A caller with a large native scope therefore executes the uncapped prefix sum before the advertised physical sentinel/refusal; the one scoped index does not impose a cardinality ceiling.

Small correction: bound the facade's native membership read with the same10,001 physical sentinel and maintained count check before computing its prefix basis, or derive those operands within the already-qualified complete gather without exposing a caller working-set argument. Preserve the exact supplied-basis comparison and real positive Included prefix guard. Do not truncate a prefix and treat it as complete. The current code is linear with a duplicate scoped read, not a repeated full gather per prefix; the issue is the order of the physical bound, not an asserted timing regression.

Verified final closure:514–515 refuses maintained expected count>10,000 before reading parents.516–525 materializes the full indexed native component scope in segment-key order with LIMIT10001, obtains full count/positive requested-member presence and computes prefix sums only by FILTER on that bounded set.526–528 refuses overflow, mismatch against both maintained counts, or a nonpositive/missing requested key before530's quote delegation. The full gather still checks native identities/conservation before any target quote. NULL/full facade behavior remains unchanged. There are two bounded linear parent reads for an optional prefix facade invocation; no timing or universal physical-cost claim is made.

## Confirmed source behavior outside those findings

| Subject | Evidence and limit |
|---|---|
| Native capture identity | Gather15–33 binds real RunPosition→RunWork→Worker→Run, exact WorkChoice expected revision/SEALED state, sealed approved line/revision, Candidate/Work/component and source channel. Captured overload132–140 binds an exact stored decision/version present in original refs. No ACTIVE/current Candidate/Contract substitution. |
| Complete physical membership | Gather43–83 reads the indexed native component prefix with LIMIT10001 before Included filtering, joins exact native segment/approval identities, checks positive Included disposition and full counts, A/R/H and four attribution totals. Original1100 composite FKs also bind RunWork/Work/revision/segment/component. No removed absent-approval row supplies units. |
| Accepted refs | Component subset is1–5 entries/8192 bytes with duplicate family/code refusal36–41; every prepared pinned entry's ID/version/family/code/line, Candidate/target/Umbrella/VAT/policy/date are requalified218–227. Native defaults have NULL decision pair, distinct PINNED/NATIVE_DEFAULT labels445–450. Uncovered positive admission remains a separate full-owner requirement. |
| Scalar retention | Four-argument entry483–488 delegates NULL/NULL. Original2218 identity/basis/mode/manual/window, scalar nonnegative A/R/H/residual, exact partial quotient/roundtrip and target range code remain in the shared six-argument body. Original scalar output keys remain. Genuine zero is retained after mode/manual/window qualification288–290/431. |
| Captured signed conservation | Only completed native census allows signed A/R and positive Included above net residual250–258; external scalar bounds remain228–249. Full/prefix supplied basis/Included must equal derived native values141–156. |
| Exact units and nullable Source | Real Included native five-cell vectors qualify once344–394. Intact manual units survive other component R/H; source split requires same-member native quote/conservation; ambiguous positive partial multibucket refuses. Single-bucket residual requires exact finite six-decimal quotient, multiplication and original-channel1157 roundtrip374–387. Manual Source can be NULL; Suggested/default Source cannot425–426. |
| One arithmetic owner | Source roundtrip and target calls use unchanged `private.pay_finance_selected_quote_v1`; no second rate/conversion calculator. Target composition is at most five prepared entries per positive prefix, with native ex/VAT/inc/range checks438–461. Source Included is not multiplied by bucket count. |
| Explicit zero | A complete zero-Included segmented scope has qualified zero bucket tuples, zero component target and empty positive prefixes399–431/463–466. Non-NULL prefix requires a genuine positive native member146–148. No unknown positive attribution is coalesced into this branch. |
| Working scope | Gather before quoting refuses >10,000 physical parents/>8MiB34–35/77/88–89; prepared/qualified/returned working representations have additional8MiB checks395–396/477–478. Internal budget is not a public response allowance. CC02 must close for the facade's optional prefix read. |
| One gather per component input invocation |128 gathers once; immutable parent/member arrays are then traversed for qualification and cumulative quote. No SQL full-capture call inside the positive-prefix quote loop; bounded plan lookup is at most five. This is a source structure statement, not a runtime/timing/allocator proof. |
| Current wrapper retention | Final533+ preserves2302's exact stored version/boundary/live current Work+Position and live snooze qualification; scalar residual guard is restricted to live mode, captured mode delegates to positive native capture. |
| Security/scope | All five owners are STABLE SECURITY INVOKER with fixed pg_catalog,private search path; final ALTER/REVOKE block preserves postgres ownership and revoke-all including service_role. No public function/table/command/protocol/temporary staging or locking/mutating helper introduced. |

## Source evidence

| File | SHA-256 |
|---|---|
|2357 initial reviewed checkpoint|`B471393436B9D5DF13D3DFAB9A9AFFD61BFBE14D9638A82C335A69B28E104AC9`|
|2357 final reviewed CC01/CC02 closure|`1C4C166C42101DD9DA9721BE323B1468F9C8723B5608C2B8197FC6453AA176B4`|
|2218 original input quote|`1F161C8A632DED5985BAC8CD37DB7CCA9DE189C7275B9E2309FE423E98F67A89`|
|2302 current saved/live wrapper|`DD3A2BB6D0E0A262A65CDCB96C0620E6B5A0A84E0A518191DA1D6D15B6075B7B`|
|1100 native segment schema|`0A2E8A72F43BD6DE767E0B277C8C895E02ED29482588A40C23CD51EBD8AC5EF9`|
|Working-set implementation design|`E35928E25151625C6B1384A296B2B59B6F3893116D8A3692F3E545057A9F387B`|
|Composer edit map|`F2761091803B770A6EA1394BEE344F7F11E219E7D4B7A48C51C97E5C0E762ED9`|
|Independent working-set DESIGN PASS|`6CEFB050C4DCD6D9D274E09B8DE7F56190DE0B3F4904E17CB5AA53D3B5D5A506`|

The original RunPosition residual equation is also retained by native core1324:554–555;1100's typed parent/composite FKs supply exact native membership. Unchanged1157 remains the sole calculator. No compile/catalogue or first-use claim is made. This paragraph was the pre-eligible-leaf closure: at that checkpoint, the remaining WB29 joins and genuine uncovered EX-08 admission were pending and were not certified by a NATIVE_DEFAULT bucket. The current assertion review follows.

## Current eligible-leaf assertion — SOURCE PASS

Reviewed current2357 `E45C85D7…` with final `WB-29-eligible-leaf-join.md` (`D183403891DAD551341E1F160785820C5B84523191230381704A28F267D43798`), its independent design review (`0F21C81053BB51DF5099AB948E4267741B0CB2F9A840A1FD5D51F36611B18623`) and current `13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql` (`35648FDE73F982466ED72C065EB9B6916D6EF998364BD64AF0F491625A73CFD3`). The earlier full-census, prefix metadata, physical sentinel, accepted-reference and shared1157 conclusions remain valid; this amendment introduces no arithmetic or money mutation.

Current2357:25 now requires an actual accepted descriptor in addition to the original exact captured Run/Worker/WorkChoice/revision/line guards.35–36 unconditionally calls the STABLE private `bpay_next_choice_leaf_eligible_v1` before the parent read.0016:13–25 raises `23514/BPAY_NEXT_CHOICE_ELIGIBLE_CAPTURE_MISSING` if that descriptor-backed Run lacks its HEADER or the HEADER actor/pay date/accepted context/raw intent differs. Its legacy no-descriptor TRUE branch cannot rescue this gather because2357:25 already refuses that scope. The universal non-memory HEADER need not be acknowledged to prove native admission; this does not grant remembered-baseline semantics.

For every positive Included parent,2357:65–66 requires INCLUDED disposition, a genuine native approved segment and TRUE from that helper for its exact segment.0016:26–50 binds the sealed Work choice/expected revision, actual Candidate/Work/revision/approved line/component, native segment identity, SUBSET selected component, exact immutable LEAF key and basis revision/line/component/segment with PAYMENT direction. Both PINNED and uncovered NATIVE_DEFAULT composition therefore require this original eligible native segment; no latest ACTIVE decision, EX-08 classifier, current Workbench or current Candidate/Contract pricing supplies the proof. Each lookup uses existing native point identities and the selection-memory primary key `(run_id,row_kind,leaf_key)` from2306 schema:17; the complete capped parent census/conservation remains unchanged.

### CC03 — closed: segmented zero skipped unknown-Header validation

Intermediate2357 `DD453D9030B65A25D144FBCE9E6A0C986914BE8A2B0A1F8336043EE220B89342` called the helper only for positive Included segments and count=0 nonsegment scope. A segmented count>0/Included=0 could reach the explicit zero quote without any HEADER check. This bypassed the design's typed refusal for missing/mismatched historical capture. Reported to root before closure.

Verified current correction:2357:35–36 always validates the original HEADER. The returned component-leaf Boolean is deliberately ignored at this preliminary call: a segmented component has WORK_SEGMENT leaves, and a known excluded zero scope must not acquire a fake WORK_COMPONENT leaf. Unknown HEADER still raises. Count=0 nonsegment scope separately requires TRUE at82–83, covering both positive and authorised-zero nonsegment quotes. Segmented zero remains its original complete-census/native-conservation/mode/basis/reference/window-qualified zero branch; it supplies no positive units/target and does not claim every excluded segment was eligible. Positive Included still requires the exact per-segment TRUE assertion. No unknown positive basis is converted to zero.

Current selected-prefix facts/filter/budget are475–488, and optional facade sentinel/full-count/positive-through-key checks are520–536. The first physical parent read remains LIMIT10001 at46–48 after the maintained>10,000 guard37–38. Helper and all five composition owners remain STABLE SECURITY INVOKER with fixed `pg_catalog,private`, original postgres ownership and revoke-all including service_role. The exact private dependency introduces no locks, mutable classifier, public command/route/table or pricing owner.

**Final boundary:** current2357 SOURCE PASS, CC01/CC02/CC03 closed. This proves the captured composer's assertion against an existing immutable original cut, not that every new0016 Create/capture/materialisation successor or0005 hold integration has passed its separate source review. Runtime/compile/catalogue and all joined/business/app journeys are NOT_RUN by this reviewer.
