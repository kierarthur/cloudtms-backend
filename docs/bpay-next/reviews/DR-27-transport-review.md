# DR-27 independent transport source review

12 October 2026. **SOURCE PASS** for the two closed directory readers at the hashes below. R27-T01 was found and corrected during review. This receipt covers saved Worker/Office transport source and its agreement with the native reader DTO; it does not certify SQL financial implementation, runtime behavior, performance, visual approval or the later DR28 host. No product edits, tests, syntax checks, SQL/RPC/business calls, fixtures, captures or hosted actions were performed by this reviewer.

## Current reviewed bytes

| File | SHA256 |
| --- | --- |
| `broker/src/bpay-next-preparation.js` | `5566476528f95af3a0f28634d9ab1c7a4c19d73e6fcc4f464ddbd363d79fb22b` |
| FE `js/bpay-next-office.js` | `0c9934bcd334a39e3209d2e260e2eca7c8fa13908dc01bc55fae17dbbf31c790` |
| Native DTO reference `supabase/repeatable/12102026_1125_banking_pay_next_channel_draft_list_v1.sql` | `8e92c091539f1516af4ca0cc8ec1d37c81d9b60b7caa48a3ea92b3548adde60a` |

Authority: full DR27 card/checklist, BD142 `design/DR-27.md`, `design/DR-27-contract.md`, and the saved independent design review. D08, real DR28 list integration and later execution-writer integration remain open.

## Finding closure

**R27-T01 — closed.** The first Office source at line 1078 converted non-null directory `context` and `after` objects with `String`, producing `[object Object]`. Valid continuation and shared-generation count reads would therefore fail the Worker's JSON parser. The final saved serializer JSON-encodes only `CHANNEL_DRAFT_LIST`/`CHANNEL_DRAFT_COUNT` `after`/`context` in that existing seam; null omission is retained and unrelated kind serialization is unchanged. Source closure is at Office 1077–1084 and Worker 758–769. No runtime execution was used to establish this closure.

## Contract checks

The complete `CHANNEL_DRAFT_CONTEXT` through `channelDraftResult` block is text-identical in Worker 17–54 and Office 14–51 after newline normalization. Each adapter uses its actual native field signature and exact shape validator: Worker 441 and Office 569. The only new page schemas are the two read kinds (Worker 294–295, Office 465–466); no command/journal/reauth schema changes are introduced by these additions.

Required nullable context, exact agency/actor/owner epoch/status/generation, seven finite statuses and labels, exact channel/type pairing, full run/channel identity and strictly closed row/cursor shapes are checked. COUNT binds returned context to the request. LIST additionally checks requested limit, duplicate addresses, actual status-filter membership, strict continuation order from the submitted anchor, complete/next-cursor agreement, nonempty pending pages and an issued cursor naming precisely the last returned tuple/context. A full page followed by an empty complete terminal read remains valid. Unavailable future reference/route/date/schedule remain nullable; route/timing interfield qualification rejects contradictory facts.

Timestamp order preserves all six native fractional digits: it converts whole seconds to integer microseconds and adds the padded fraction, rather than sorting by millisecond `Date.parse` alone. UUID/channel ties remain exact C-compatible canonical native values. Monetary numeric38.2 text is checked and compared as integer pennies with BigInt; the gross ex/VAT/inc equality is validation of supplied server facts, not pricing or derivation. Current cash cannot be negative and its separate completeness flag is retained. No browser totals, conversion, rate lookup or execution-status inference is added.

Native page shape at 1125:183–285 agrees with these fields, count/context and tuple cursor. Its native packing keeps rows below 100000 bytes and cursor/context small within the existing 128 KiB whole-response transport. Worker has the existing 32768-byte request cap, 4096-byte closed cursor cap, streaming 128 KiB response cap and finite 8-second read deadline. Office retains bounded JSON and finite read timeout and rechecks current actor before and after awaited response/body reads (975–1005). The fixed scope and cursor shape keep directory GET encoding within the original request envelope; no long display labels are serialized into a cursor.

Worker maps only the exact native `55000/BPAY_NEXT_CHANNEL_DRAFT_CONTEXT_CHANGED` to a named definite 409 (1079), with no automatic retry or outcome-unknown claim. Office retains that known server code on its existing refusal object. Existing administrator/session service injection and SQL rechecks remain the original authority; adding directory readers does not bypass them.

The final SQL reference also contains the private deferred directory lock as `FOR UPDATE NOWAIT` at line 163. That is an implementation refinement of the reviewed COMMIT-only metadata publication, not a new transport protocol. Existing rollback/replay behavior remains the relevant recovery boundary; this transport review does not separately certify every helper or deferred SQL trigger.

## Limits and packaging

This is source-only approval. Actual list/count reads, changing generation, same-microsecond pagination, historical access, performance and full in-app opening remain NOT_RUN by this reviewer. Root owns required compilation/checks and later A4. The current Worker Git diff includes newline churn; this receipt certifies the semantic closed block and adapters, not preservation of every unrelated raw line ending. A later packaging repair needs a fresh hash amendment, without reopening unchanged semantics.

## Final packaging amendment

**SOURCE PASS remains at the current table hashes.** The earlier newline-churn observation is superseded: current Worker diff is 46 insertions/1 deletion after root's parent-EOL preservation. Independently reread the saved channel block, two page schemas, JSON parser/result adapters and named error mapping. The complete closed block still equals Office after newline normalization; Office remains at `0c9934bc…` with the fixed JSON cursor/context serializer. No semantic transport change or new finding arose from packaging. The current native DTO reference hash reflects qualified native SELECT aliases, with no changed directory fields. Its later 1136 guard successor is outside this transport amendment. Final design/contract hashes and the COMMIT-only `NOWAIT` refinement are recorded in the updated design review. Compilation/installation were reported by root and were not executed or independently claimed here.
