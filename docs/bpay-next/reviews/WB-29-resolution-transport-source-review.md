# WB-29 W1 resolution transport source review

12 October 2026. Independent read-only source review against2241, the unchanged W1 DTO in2302, and `WB-29-resolution-transport-design.md`. Root alone writes product. **Final W1 transport SOURCE PASS — R29-T01–04 closed; the final receipt below supersedes the historical OPEN checkpoints. Whole WB29 remains open.** No product edit, tests, fixture, SQL/RPC/database action, capture, hosted call or commit was performed.

## R29-T01 — quote must bind native residual and explicit Preview operands

`broker/src/bpay-next-resolution.js:59–67` qualifies Quote's scalar types, policy, amount equation and saved pair, but never binds `source_component_residual_ex_vat`/`included_source_component_ex_vat` to this row's Position. At line107 it accepts any Preview with matching policy. Thus a response with this row's Position residual40 but a Quote residual/included400 passes, as does a MANUAL_AMOUNT Preview ex50 for the original request40 or a MANUAL_REPLACEMENT_RATE quote rate18 for request17. This is validation of native echoes, not authority to price in JS. Smallest fix in both mirrors: derive/check the exact penny residual from this row's native A/R/H for every non-null Quote and the native current W1 included scope; compare manual amount Preview ex and manual-rate Preview rate to the original exact decimal operand. Keep the honest nullable bucket Source amount and typed residual refusal; do not demand source_ex==whole included or infer worked units.

## R29-T02 — Basis family/kind/key/semantics must retain native qualification

`broker/src/bpay-next-resolution.js:46–54` allows each native enum independently. It accepts STANDARD Basis with component_kind ADDITIONAL, ADDITIONAL with a mismatched component_key, or RATE_BEARING with NULL/nonpositive units/missing used pay/charge. Current2100 basis owner requires STANDARD→WORK/PROTECTED_WORK and rate-bearing; ADDITIONAL→ADDITIONAL with exact `ADDITIONAL:`+code; ADJUSTMENT→ADJUSTMENT with key==code and amount-led. Its rate-bearing predicate is exact positive units plus non-null actual used pay/approved charge for STANDARD/ADDITIONAL. Add these finite interfield checks identically to both helpers; preserve genuine zero rate, nullable historical operands and non-rate-bearing Amount-led data.

## R29-T03 — keep existing environment/Office refusal handling reachable

FE `js/bpay-next-workbench.js:672` sends every failed RESOLUTION_CONTEXT response to its full Workbench fault validator before the existing three-key ENVIRONMENT_MISMATCH/OFFICE_FORBIDDEN handling at line674. Those original authoritative refusals now become RESPONSE_INVALID for this new kind. Move or share the existing exact three-key branch ahead of the new-kind fault branch. No extra error shape or route is needed; retain its original retry=false behavior.

## R29-T04 — finite refusal flags must agree with the native fault map

Both helpers' `validateResolutionFault` (BE:131 onward) require retryable/reload_required booleans but accept any combination for each admitted code. A MODULE_NOT_ACTIVE reply with retry=true/reload=true, or a stale native response with reload=false, currently passes even though the exact native map emits the opposite. Validate flags against the finite existing native code rules (and exact generic Workbench faults); preserve the native distinct SQLSTATE/message qualification at the Worker RPC boundary. No prefix-wide allowance or automatic retry is needed.

## Checked / retained

The closed GET keys, duplicate JSON key reader, lowercase UUIDs, original scope/row order, <=100 rows and owners, 32KiB encoded query/full RPC request,4s deadline and streamed128KiB/native120000 result bounds are present. Decimal strings preserve native precision; no Number/epsilon/pricing was introduced. Actor/Candidate/Work/revision/physical/policy binding, available/basis equality, sorted exact owner union, incompatible versions and zero flag/refusal relationships are checked. Native READY/UNBOUND Source-book states remain observations. New branches bypass ordinary cursor/rows assumptions, index loads the new helper before Workbench and leaves other kinds' dispatch intact. Final parity/diff/source closure is pending the four fixes.

## Reviewed saved bytes

| File | SHA-256 |
|---|---|
| BE broker/src/bpay-next-resolution.js | `ae2e11d4f07f43a2a13ea5db1330a9adeca1f2fa9ceeb8b09dd33cfd69e8d0d8` |
| BE broker/src/bpay-next-workbench.js | `44b22bcb1aa4a9787944f2943ab5c821ce54c752bbbcec8c9ae2ad14e3533341` |
| FE js/bpay-next-resolution.js | `7fa9d45f97eaff38af78744dcb77ed4b9a317247fba516e3cbb6c253989a6366` |
| FE js/bpay-next-workbench.js | `9b61a8d574c6ecd8e6295bdf8d84358ce59a3f08548be8d0a6164b608fa21cad` |
| FE index.html | `74820c731a7a5c6b5a5125fdfe0ca63ac21c9d2a65b7d955d85880f66913b4f4` |
| SQL2241 resolution_context_v1 | `be4033bd9973db55f3fc41b96ef29a11b396c508d37bfc6f06dcb7945a1b586f` |

Review files reference the two build worktrees identified by root. This checkpoint covers source only; runtime and whole-item acceptance remain NOT_RUN/open.

## Static parity and native zero qualification

The complete BE/FE validator cores are identical after removing only the ESM/UMD wrappers (16,988 normalized characters each). Existing caller diffs are finite to this new read kind, original actor checks, bounded query/RPC serialization, response validation and helper loading.

R29-T01 must preserve the native input-quote2213:90–94 zero-included fast path: exact ex/vat/inc/remaining_units=0 and rate=NULL can be valid even for a positive submitted manual operand. Compare manual Preview amount/rate echoes only for nonzero included scope; preserve the existing zero confirmation flag. Use sign-safe exact BigInt penny parsing for A−R−H: the present cents() implementation parses −1.20 as −80 pennies. Signed observational realised values and A<R+H facts remain admissible when native quotes are unavailable; a non-null Quote must satisfy the native residual/included relation. No Source bucket amount or units are inferred.

## Fresh correction checkpoint

Current BE helper49fb45ec96cf45d2b53fa8ffa03b0fcdd2293bd55681937b0212ff2e5a8c0e97 and FE mirror255de7b9fde17889c24b16dce8a15a8f444e9847cfba440ca015d86dabf14d3c implement sign-safe residual/included binding and manual Preview/saved operand echoes. R29-T02, R29-T03 and R29-T04 are closed in these saved bytes; FE Workbench329b23b912c69d06dfaa1063725e6e650667d3a3f53cbf2755203e035073ef24 handles the original three-key refusals before new-kind validation.

One R29-T01 subcase remains OPEN: Suggestion uses quote() alone. With native residual/included=0 and intent=NULL it can still accept nonzero Suggestion money/rate. Qualify the exact native zero fast path in quote() for every non-null Suggestion/Preview/saved Quote: source_ex_vat=0, ex/vat/inc=0, remaining_units=0 and rate=NULL. The original native quote owner2213:90–94 provides this relation. No additional request/response field or financial calculation is required. Final SOURCE PASS remains pending this small correction and final parity/hash capture.

## Final saved-source receipt

**SOURCE PASS.** Fresh complete helper reread and static comparison confirm identical BE/FE validator cores (19,484 normalized characters after only ESM/UMD wrapper removal). No outstanding source finding remains in this W1 batch.

R29-T01 is closed: exact sign-safe BigInt A−R−H qualification binds every non-null Quote's residual/included scope, and quote():70 now applies the native zero fast path to Suggestion, Preview and saved Quote. The shared manualEcho() validates nonzero manual amount/rate echoes against the original Preview request or saved decision, while preserving native zero scope. Signed observational realised money and unquotable negative residual facts remain visible through their native nullable Quote/refusal relationship. Honest nullable bucket Source amounts remain nullable; no Source attribution, pricing or units are derived in JS.

R29-T02 is closed by the exact family/kind/key and rate-bearing predicates. R29-T03 is closed by the original exact three-key environment/Office refusal branch before the new full fault branch. R29-T04 is closed by exact native/generic retry/reload qualification; unknown native errors retain the existing read-unavailable response. Request/context/result identity, compatible saved owner and the complete distinct incompatible-owner union remain closed and bounded.

The finite Workbench additions retain the original route, trusted admin actor, current-actor checks before/after await, lowercase UUIDs, duplicate-key parser, exact GET keys, original scope order, 100-row/owner caps, 32KiB query/full RPC request, 4s deadline and streamed128KiB/native120000 response budgets. Decimal text preserves native amounts, versions and six-decimal rates without Number/epsilon conversion or a lower financial ceiling. Index loads the new helper before the existing Workbench client. Ordinary read kinds and mutation/journal paths are unchanged by this batch. The2302 live-snooze successor changes native qualification, not these DTO keys; its wider SQL review belongs to the separate SQL reviewer.

| Final file | SHA-256 |
|---|---|
| BE broker/src/bpay-next-resolution.js | `e0a063d9865ac23f8336825d0b69f0b18658ea738ae3218b3716004389686be6` |
| BE broker/src/bpay-next-workbench.js | `44b22bcb1aa4a9787944f2943ab5c821ce54c752bbbcec8c9ae2ad14e3533341` |
| FE js/bpay-next-resolution.js | `d3ae4d6206f4286d5f3e32157bc7e96f46555765a8618acf2689a2f27b25c720` |
| FE js/bpay-next-workbench.js | `329b23b912c69d06dfaa1063725e6e650667d3a3f53cbf2755203e035073ef24` |
| FE index.html | `74820c731a7a5c6b5a5125fdfe0ca63ac21c9d2a65b7d955d85880f66913b4f4` |
| SQL2241 resolution_context_v1 | `098ef7ee8e20b889b85d9702a374cb381a172d07cb47692e56fe20f7cd9d650e` |
| SQL2302 resolution_live_snooze_v1 | `dd3a2bb6d0e0a262a65cdcb96c0620e6b5a0a84e0a518191da1d6d15b6075b7b` |
| WB-29-resolution-transport-design.md | `e0e0a28f2202d0cbed3133cc5b420dcd896cc73099f6d27df241d93b069900b3` |

This receipt is source-only. No tests, syntax execution, business/database calls or joined screen acceptance were performed by this reviewer. W2 and the remaining WB29 owners/whole-item acceptance are not covered or marked complete.
