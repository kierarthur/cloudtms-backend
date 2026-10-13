# WB-33 recovery design and initial source review

13 October 2026. Read-only review of BD174, unchanged original `createResolutionCommandController` in FE resolution428–491 and the current integrating `js/bpay-next-clear-choice.js` (SHA-256 `ed94ce6f7ffd8c71b2d5e828aae483e24241133e0f88226c9d0c640f20ac189c`). Root alone authors product. No product writes, business/SQL/RPC, tests, browser/captures or installation. **Core recovery design is sound; SOURCE OPEN for R33-R01–04 below.** This receipt does not approve unfinished caller wiring or A4/app acceptance.

## Minimal native recovery architecture

Keep the original financial controller, storage record schema, action/args JSON bytes, request UUID, readback and retry behavior unchanged. Its original namespace is agency/actor/Candidate; its internal stored epoch must match the original body. It validates exact native receipt against that body and settles/removes the financial journal before execute/recover returns428–491. A saved original receipt is final even when subsequent read/metadata/callback fails. Original readback precedes identical retry; absence of a reply or presence of an old cache never authorises a fresh financial request or a newer owner/version.

Use one bounded screen-local clear-window cache per original agency/actor/Candidate slot. It is recovery/presentation metadata, not server financial authority. Persist the validated original native clear reply, original root scope and exact original CLEAR body before execute; fail visibly before dispatch if that required metadata cannot be saved. Bind original Candidate/Work/revision/line/family/code/owner ID/version to the disclosed native owners and original common context. Pending shared journal must byte-match the cache body; another original request cannot be overwritten. Cache a validated accepted receipt immediately, then resume only existing exact COMPONENTS/publication mapping. Remove this local metadata only after successful own-parent restoration or a proven original definite nonacceptance; stale/unknown/replay conflict retains its original identity.

The settlement/cache-save crash interval is real: no main journal alone does not prove never dispatched. A pending sidecar with absent main journal must retain its exact body and resolve by original native readback. The integrating module's narrow rehydration of the identical original journal record followed only by `recover()`92 is acceptable: it does not generate a UUID, change body/schema or auto-dispatch. Financial retry stays a later explicit action only after that readback proves absence. An accepted cache can resume native reads after strict original receipt validation and genuine current UI authority; cached amounts never drive new pricing or a mutation.

Wanted mapping is derived from original selected `args.works/rows` and validated `receipt.result.works`, not all server-discovered optional linked rows. Match original native component keys from the validated original row context, read only each exact accepted published_revision_id through existing COMPONENTS and retain <=100 requested keys/native cursors. Each turn is<=10 pages/5-second start budget; continue only while visible/current, never resend accepted CLEAR. A missing key at completion, changed publication or actual retired parent refuses/stays stale; it is not a fabricated completed mapping. After exact fresh W1 restores the same parent, notify outer lists without widening current()/actor/owner fences. Old captured refs/amounts are unchanged.

## Current integrating source findings

| ID | Actual evidence / required finite correction |
| --- | --- |
| R33-R01 — original epoch reinterpreted as current | retained11 requires original reply.owner_epoch equal today's addressed context.owner_epoch. The unchanged controller and native W2 intentionally keep original epoch internally and replay before later epoch checks. Qualify current agency/actor/Candidate and actual NEXT/caller lifetime, plus exact old body/record epoch; do not block original receipt readback merely because today's offer epoch changed. No permission to dispatch an old offer is granted by this correction. |
| R33-R02 — duplicate restored READ identity | CLEAR may legally select two different actual owners for the same line/family/code, because W2 uniqueness is by owner ID. restore83 maps both to identical new W1 READ rows, whose native READ validator rejects duplicate row identity. Deduplicate only the restored read scope by actual new line/family/code; retain every original cleared owner ID/version in the immutable request/receipt and confirmation. |
| R33-R03 — cache bound below native payload envelope | Cache10/30 is196608 bytes while a legal record contains reply<=120000, args<=32768, escaped exact body up to65536 and receipt<=65536 plus scope/version. A pending/accepted cache can therefore fail solely from legal combined native inputs. A fixed320KiB ceiling accommodates that representation, or compact duplicate args and compute its exact bounded ceiling. Keep original financial/read bounds; no unlimited local history or reduced legal request range. |
| R33-R04 — unrelated original journal accepted as Clear | With no clear sidecar, load53 constructs the original shared controller from current clear read. That controller can retain a pre-existing SAVE or another request in the same Candidate slot. recover66 then calls accept without binding actual action/body/cache; the result can be displayed as Choice removed and restore73 dereferences absent record.args. Inspect/qualify the existing main journal before any fresh clear read; preserve or route its true original recovery, never reinterpret another action as this Clear. Only this exact cached original CLEAR body may become the Clear confirmation's accepted receipt. |

All four sent immediately. The integration already correctly persists metadata before execute, validates matching native owner IDs/version, caches accepted receipts, preserves orphan original bytes and selects only original requested mapping keys. The remaining fixes are local finite guards/bounds/dedup; no server route, command, table, protocol, status helper, quote calculator or new money owner is warranted.

Caller/source review must also verify that local accepted-cache save failure remains a truthful accepted outcome, that completed restore clears only its own cache, and that real actor/owner/parent retirement never produces late focus or dispatch. Current-frame confirmation text/visible differences and all42policy rows belong to A4; joined business/application acceptance remains AUTHORED_NOT_RUN.

## Fresh finite repair checkpoint — R01–R04 CLOSED

Re-read current module through SHA-256 `e092f9369523f3fbe28ef0a008ca28a14254c8ffebb9123634335a4be11e10e2`: original epoch is internally qualified rather than compared to today's offer; cache limit is327680; restored W1 identities are deduplicated without changing original expected-owner/request/receipt scope; before a fresh read an existing main journal is refused, staged-controller race protection remains, and accept() requires a validated received original CLEAR receipt bound to record.args. Original command controller text remains unchanged from HEAD. Pending exact-body/orphan rehydration still reaches readback before retry/live discovery.

Two current local seams remain OPEN and were sent promptly:

| ID | Actual evidence / finite correction |
| --- | --- |
| R33-R05 — final read start budget | restore85 may start fresh W1 after the last COMPONENTS page completes even when the five-second turn budget has expired. Apply the same stop-before-next-read budget check before that W1 request; preserve exact mapped keys/cursors/receipt and let the existing visible read-only continuation start the next turn. No new status/RPC/protocol. |
| R33-R06 — child dismissal callback | Rates132, ComponentChoice82 and PayChannel91 pass dedicated onClearDismiss to restore pendingClear state; ClearChoice105 instead calls the unrelated options.onDismiss. Keep/Close can strand parent pendingClear=true and invoke a parent dismissal callback. Invoke the dedicated child callback after its own timer/listener cleanup; preserve the owning parent and original current/actor fences. |

Native status enrichment uses only existing GROUP_LINES for exact affected Work/receipt publication under fresh same agency/actor/epoch/WB/adoption context, with bounded page turns and compact per-component status facts. It must not infer Ready/Need from a missing native status, broaden financial scope or automatically retry accepted CLEAR. Full caller/A4 review is separate. SOURCE remains OPEN solely for the reported finite seams at this checkpoint.

## Budget/dismissal closure and own-parent scope checkpoint

At clear-choice SHA-256 `83385b7125ee414e4f2938ed59d819745216f558bfd9663634486e27d62340df`, R33-R05/06 are CLOSED: the same page/time turn check precedes final W1, and child cleanup calls dedicated onClearDismiss. All R33-R01–06 are now CLOSED. Parent callers expose an explicit Check result while cached/original cancellation remains pending, refuse replacement writes, recover before live context, and suppress redirect when restoring the same Rates/Component editor. Component channel scope is remapped through the genuine returned Work revision rather than discarded.

Two finite caller completeness findings were sent immediately after inspecting the integrating parent callbacks:

| ID | Actual evidence / correction |
| --- | --- |
| R33-R07 — missing native status becomes no pay | ClearChoice calls onRestored when status pages are complete even if a requested mapped component has no matching native GROUP_LINES status. All three parents treat empty statuses as Nothing to pay remains. A missing row is not authoritative zero/no-pay, particularly when fresh W1 still has a positive residual. Require a genuine current native status for every requested mapped key or retain unavailable/stale; no invented Ready, NEED or no-pay. This is a bounded existing-map completeness check, not another reader. |
| R33-R08 — whole Rates parent narrowed to first line | Rates onRestored133–134 replaces an originally nullable whole-Work approved_line_id with the first new STANDARD row. Its next0201 reload then filters to that line, hiding other requested Standard components from the same parent. Preserve original null scope for a whole Work; only remap an originally specific line by its exact old immutable component key. No new membership or broad adoption facility is needed. |

Fresh inspected caller hashes: Rates `baec077c5891b2c996b837d3b9376883909323fcc6332addbe65fe1dfc450a18`, ComponentChoice `e95dd7be1b7c6d05e904e1f591a7ef36ff08c2842b56300c9cab16bfeb7512d0`, PayChannel `aae25353c49d4328d24ceaa033d7c9f0dc1957f562d3507907a9ae2b374d841e`. The original controller comparison remains exact; new closed DTO hash `1c01bc2a743d1a6d4489b80802ab499334294f96cd240ff57c40a5f8542e2f98` includes only the new reader contract around it. Source remains OPEN for R07/08, not for any financial owner/protocol expansion.

## Current recovery and parent delta — 13 October 2026

SOURCE OPEN only for R33-R09 below; R33-R01–08 are CLOSED at this checkpoint. Static saved-source review only; no product edits or runtime/business/SQL/RPC/tests/captures. Original createResolutionCommandController normalized source is identical to FE HEAD.

R07 now requires a genuine GROUP_LINES status for every wanted mapped component before completed restoration. R08 preserves nullable whole-Work Rates approved_line_id; only an original specific line is mapped through its original native component key. Each caller binds the screen cache to its actual RATES/COMPONENT/CHANNEL parent kind. Rates and Component anchors come from the validated original parent row (or its validated retained anchor), carry only native Work/revision/line/family/code/component identity, and are appended only to read-only accepted COMPONENTS/fresh W1 restoration. They never enter the original CLEAR financial args. The accepted receipt remains final, exact original command/body/owner IDs/versions remain unchanged, and unrelated pending original requests are refused before live discovery.

Four native sections times three finite resolution reasons plus NULL permits at most16 distinct status facts per wanted key; dedup keeps this genuine bound without rejecting legitimate mixed status membership. Component restoration filters to its own original component key; after exact component reader STALE, its fallback requires the same native Work/new revision/line/family/code and resolver_ready, under matching original native context. Unknown/missing status remains unavailable, never invented zero/Ready/NEED. Status-only reset on failed/stale reads preserves accepted receipt and exact publication mapping. Parent onChanged is deferred until own restoration.

**R33-R09 — completed status facts can outlive their context.** ClearChoice restore keeps completed GROUP_LINES maps without storing the context which qualified them. A later continuation gets fresh W1, skips already-complete maps, and can adopt those old facts with newly read facts if another state has not yet started its cursor (or no subsequent old cursor is consulted). Existing page-to-fresh-context checks cover only pages read in that turn. Store the existing native status context and compare it with fresh W1 before retaining any old status maps; on mismatch reset only those status maps/cursors and reread within the same native bounded continuation. No new reader, counter, money calculation, command or protocol is needed. Sent to root immediately.

Inspected file SHA-256 values:

| File | SHA-256 |
| --- | --- |
| FE js/bpay-next-clear-choice.js | A6D3CFC0F4A53AEF321F269174D236B2C4E3671F7B8D00B786260DE8E1DE90D2 |
| FE js/bpay-next-rates.js | C6A1274879E6819DC4C3E35FAC6AED9DF8EBBE43049876E29A55FA527A9EF6A4 |
| FE js/bpay-next-component-choice.js | D72F16DD7D50F31F9D7892F3426A1280EAC05699B9BDAED13E7A2F226A4A0C7D |
| FE js/bpay-next-pay-channel.js | 8BB75B81AB11E015F2EC08662C3D16D1AFF56B8D4CD7C3BC35EC1BE224A0F682 |
| FE js/bpay-next-resolution.js | 1C01BC2A743D1A6D4489B80802AB499334294F96CD240FF57C40A5F8542E2F98 |
| FE index.html | 14420390F07DF1CBB7B28F183F7176DE9B9731FC033EBDFA643327D5259845EC |

The Clear file changed while review was active; its fresh saved hash above is used. Historical findings and hashes above remain historical evidence. Whole-card/A4/application acceptance is not claimed by this recovery receipt.

### R09 closed; read-preflight duplicate regression

Fresh clear-choice SHA-256 `D85185AD976F27DF804311D70D02DD7B4C5F2042E01DCEBD960D73843FD96B1E`: R33-R09 is CLOSED. Each status map now stores the existing agency/actor/epoch/workbench/adoption context, compares it to each fresh W1 before reuse, and resets only status maps/cursors on mismatch. Accepted receipt and exact COMPONENTS mapping remain unchanged.

SOURCE OPEN solely for **R33-R10**: the new restoration READ preflight73 maps every original CLEAR owner row, then normalizes75 without the identity dedup already present in actual restored READ101. Two actual owners of one line/family/code are legal in original CLEAR; copied duplicate READ identities refuse that valid cancellation before dispatch. Apply identical per-Work Map(identity) dedup before normalize/count/byte preflight, retaining every original CLEAR owner ID/version. The <=100-row and30000-byte read preflight otherwise use original native identities, add only the typed read-only parent anchor, and leave the original financial body unchanged. Sent immediately; no product execution.

## Final scoped recovery SOURCE PASS

Fresh read of final clear-choice SHA-256 `96BA50263ECEDEAF4974C472DAAE29096F3144B768B49186BECCF7E70F738341` confirms R33-R10 CLOSED: preflight73 now deduplicates each read-only Work row set with Map(identity), as actual restored READ101 does, before typed validation and finite row/byte checks. Original CLEAR args68–69 still contain every actual saved owner ID/version and are unchanged by that read-only dedup. The native five-field status context comparison104 and capture111 remain present; accepted receipt/publication mapping remain retained across status-only resets.

**R33-R01–10 are CLOSED; zero open recovery/source findings in this reviewed scope.** All current parent/source hashes in the preceding table remain unchanged except clear-choice, superseded by the final hash above. The original shared financial controller remains text-identical to HEAD, and its raw current module hash remains `1C01BC2A743D1A6D4489B80802AB499334294F96CD240FF57C40A5F8542E2F98`. This PASS covers the actual cache/replay/continuation/own-parent recovery deltas, not A4, whole-card closure or joined application/business acceptance; no runtime was executed.

