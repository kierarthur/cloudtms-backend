# PS-07 Worker guard-scope investigation

10 October 2026. Independent read-only source investigation of full card06:1457 onward and exact saved local checklist PS-07-D01–D04 (sequence31; history successor1 of3). Read actual index dispatch, frozen handler/transport, separate history handler/transport,0250/0025/1134 guards and1206 ancestry/state predicates. No product edits, SQL, fixtures, runtime, tests, network, business or data operations.

**D02/D03 should reuse the current Worker implementation. No missing readiness/activation separation requiring a Worker product edit was found. Historical detail uses the separate52-section history route; the four frozen-detail kinds remain guarded Draft review reads.** D01's SQL successor belongs to its independent exact scope; this finding does not waive that delivery or its first-owner chain.

## Exact dispatch and admission

Index:198704–198719 normalises route slashes, recognises exact supported NEXT paths, authenticates with requireUser, requires admin and applies the existing PF-13 configuration/environment fence before constructing the service transport. Environment readiness means matching explicit TEST caller/Worker and configured TEST gateway/local root; it performs no active_owner, READY or financial-state read. It remains required for both routes and is not a workflow-readiness guard to remove.

The retained legacy classifier explicitly exempts the supported POST history-page and frozen-detail routes (legacy-fence:172–173). The index's earlier legacy guard therefore does not query active_owner for these reads or block history when financial owner changes. Unknown/wrong-method routes retain their existing unsupported/legacy-fence semantics; no prefix-wide exemption is justified.

| Route | Exact owner call | Intended state boundary |
| --- | --- | --- |
| POST `/api/banking/pay/next/frozen-detail` | index:198741–198748 → handleBpayNextFrozenRead → fixed frozen RPC → public0250 | Four Draft review kinds only; module NEXT plus REVIEW/DRAFT and worker READY remain. |
| POST `/api/banking/pay/next/history-page` | index:198732–198738 → handleBpayNextHistoryRead → fixed public1206 RPC | Independent historical/current-labelled sections; no active_owner, run status or worker READY predicate. |
| GET existing NEXT advice/instruction ID | index:198759–198767 → existing advice handler/RPC | Separate existing PAYE advice contract; no PS-07 enlargement. |

Both handlers also validate injected Office identity/admin independently. Neither body can provide its own actor or arbitrary RPC. Fixed transports inject the authenticated actor, use the existing service headers/route class PREVIEW_PAGE, prohibit redirect, and preserve bounded raw-response parsing. The SQL owners recheck service_role claims and an active admin actor on every page. Browser roles retain no wrapper/private-page execution grant.

## D02: Draft-only guard is already correctly scoped

`handleBpayNextFrozenRead` admits exactly SHIFTS, BREAKS, RATE_DETAILS and RATE_SCHEDULE, with their current exact argument/response allowlists. These are all Draft review kinds. There is no historical kind dispatched through this handler, so adding a historical bypass or accepting paid/cancelled records here would broaden the wrong contract.

0250 checks active_owner NEXT and delegates fixed branches only. SHIFTS uses1134 shift_v2; BREAKS and RATE_DETAILS use0025. All three call `simple_frozen_line_guard_v1`, which point-resolves the exact run_line approved_line_id/captured_revision_id and requires run REVIEW/DRAFT plus worker READY. RATE_SCHEDULE has its own equivalent state check in0025:162–171 and reads only run_work.captured_revision_id's frozen schedule. Its detail is not today's Contract/rates. BREAKS preserves its exact shift-in-frozen-line ancestry check.

The JavaScript handler does not locally invent a readiness decision; it forwards only these bounded Draft read requests to the installed SQL authority and preserves explicit unavailable/forbidden/dependency errors. This satisfies the card's Worker guard-scope confirmation through actual delegation. Preserve the module/state guards and existing mutation guards.

## D03: independent historical caller already retains original authority

The history handler performs no owner-status probe, active-owner check, READY requirement, preparation RPC, job wake or financial write. It validates one current52-section request, obtains one page from1206, validates the closed manifest/page/cursor and exact scalar string/null fields, and returns. It does not rewrite a request to frozen-detail or follow a latest revision automatically.

1206 likewise has no activation or workflow-readiness predicate. Its only current_revision_id occurrence is a labelled CURRENT WORKS data field, not a predicate selecting historical revision authority. Historical traversal retains RUN_WORKS.captured_revision_id and RUN_LINES.captured_revision_id/approved_line_id. Subsequent REVISION/LINE/SHIFT ancestry point-checks the actual sealed original revision; SHIFTS/RATES are keyed by that original approved-line UUID, and rate schedules by the selected revision. A newer approved WORK revision therefore does not change the requested old capture or impose current READY.

Every parent is bound back to the requested Candidate or exact Timesheet WORK before paging (1206:234–313); a bare foreign UUID is insufficient. CASE-only cash has genuine INSTRUCTION/result/hold/transfer provenance in Candidate history and is not routed through a fabricated run-line/Timesheet. Timesheet-context allowances require actual retained collection/Case/instruction ancestry. Current WORK/position/collection/worker/request-status caches remain explicitly labelled current; their display does not make them historical financial authority.

The response remains `LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT`; cursor/upper bounds are scoped to section/parent/context and are not a cross-request snapshot. Existing100-row/byte/time bounds, exact decimal text, request/cursor foreign-scope refusal and no partial-page money allocation remain. PS-03's typed member projection is a pure read helper on the same wire; it does not add readiness or activation.

## Minimal action and remaining acceptance

- Record D02/D03 verified reuse with this exact route/guard evidence. Do not edit index/frozen handler/0250/0025 to relax Draft review just because historical paid/cancelled detail is needed.
- If D01 adds an evidenced current-contract field, mirror only that SQL final's exact projection in the existing history allowlist/consumer as required. No such delta has been authored/reviewed in this investigation. Preserve first-owner history order PS-07 → EX-41 → CM-18; do not build later move/comparison or outcome sections early.
- Reuse ordinary frozen-read/frozen-rpc/payment-history-caller cases for auth, bounded raw transport, exact keys/errors, separate route and current manifest coupling. Source inspection here did not run them; no new harness, SQL fixture or runtime programme is proposed.
- D04 remains future NOT_RUN: paid and cancelled original detail after a newer approval/current worker non-READY; frozen original hours/instructions; Draft mutation/review refusal retained; foreign scope denied; history permitted independently of module financial activation with the required authenticated TEST environment. PAYE advice remains its separate unchanged contract.

The examined index, frozen handler/RPC and advice modules have no current Git diff. Worker reuse does not establish installed/connected/app acceptance or whole PS-07 completion.

## Raw source SHA-256

| Source | SHA-256 |
| --- | --- |
| index dispatch | `9610a75e0cd0d83f023a050104f7fad3de3995a5592b1b26ea9b4e3bfadd9fa3` |
| frozen-read | `3c7d75b485db6824a38000d580eb96c37864c01a61fdaf114d5551637e10c3b2` |
| frozen-rpc | `6ddbc1ecb64873714e9a9c9a05bc4fcb2dae5d7fca95236a4f75976594a4e83c` |
| history caller incl PS-03 typed helper | `b5bb109e546787fc65a83df1916dfd29c1b21d32ae6372fd3fd04ae97baa0d50` |
| environment fence | `82d0339fd2a3881d94cc73a7237fd27c5e5755a60ca9133c1a0a460dabfa3424` |
| legacy fence | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| public0250 | `ca6e50fcb6324e9e3a39e840a664ddfa53a7a7d96d9d0fb9859d65743614e091` |
| private0025 | `d7433fd91345a02ea75a596577c66514465c007d622dd0d225ca7edf96708ce9` |
| private1134 shift_v2 | `7a65a389a1fc34cd06f7a1ba7d9fb4b448bb50b03ba77c807ffa11a6b56eff48` |
| current SQL1206 | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| unchanged advice handler | `010d06386af89983e48c63a3eafcc2796eba929f0aa74def33202157b55c00c4` |
| unchanged advice RPC | `acf94f66927911c3252f2f59d5e4bc4ab2ec4ae3d7e550e4d4d0100b8bf80549` |

Investigation complete, source-only. No open concrete D02/D03 guard gap found; D01 final SQL successor and actual D04 acceptance remain separately required.
