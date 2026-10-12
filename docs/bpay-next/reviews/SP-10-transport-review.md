# SP-10 transport source review

12 October2026. Independent read-only review of root's saved Worker/Office transport against `design/SP-10-transport-design.md`, the complete SP-10 card and exact original native eight-key owner. Root alone edits product. No execution, tests, SQL/RPC, fixtures, screenshots or business/hosted actions. SOURCE review only; joined acceptance is NOT_RUN. D03 S3 factual enqueue and unbound Source removal findings remain BLOCKED outside this transport slice.

## Initial concrete findings

**T10-01 — P1, owner-status dependency calls the factory instead of its RPC.** `broker/src/index.js:198863` passes the existing `bpayNextOwnerStatusRpc` factory itself. New `weekly-source/routes.js:600` awaits `dependencies.bpayNextOwnerStatusRpc(env)`, but index.js:40995–40997 returns `createBpayNextOwnerStatusRpc(...)`, which is a function. `normalizeBpayNextOwnerStatus` rejects that function and every otherwise valid decision returns owner unavailable before dispatch. Smallest fix: provide the exact async dependency invocation `(runtimeEnv) => bpayNextOwnerStatusRpc(runtimeEnv)()` or invoke its returned fixed zero-argument RPC in the named route. No SQL/API schema change is needed. Reported to root immediately.

The initial read is continuing for normalized root identity, retained remount controls and accepted refresh safety; root already identified and is repairing accepted map clearing/missing retry control. Do not interpret this initial receipt as a final verdict or stale known-fixed finding. Final fresh source hashes and closures will be appended after re-reading those saved bytes.

## Fresh source findings

**T10-01 CLOSED.** Current index.js:198863 supplies `(runtimeEnv) => bpayNextOwnerStatusRpc(runtimeEnv)()`, so the named route receives the actual validated owner-status result.

**T10-02 — P1, unknown KEEP replay disappears after a native NONE remount.** main.js:179352–179353 derives its root only from the proposal descriptor or normalized requested member; :179374 requires that proposal-derived root again and :179378 requires the proposal section as its host. The requested flag itself is correctly normalized as `is_requested_root` by presentation.js:751–770. However the genuine native KEEP path marks the original bundle ABANDONED (15092026_1534_weekly_source_protected_action_orchestration_v1.sql:1889–1893). The native proposal reader counts only PROPOSED / PENDING / RELEASING and returns NONE when those are absent (15092026_1534_weekly_source_read_projections_v1.sql:3227–3235/3274–3275). A lost accepted reply followed by Refresh therefore has no descriptor or members, and renderLaterChangeDecision omits the host at presentation.js:1759–1760. The existing retained record survives but cannot render or dispatch its exact retry. Creating the retry button inside an existing host does not cover this reachable normal outcome. Smallest correction: retain the original request and qualify its retry against the actual unchanged Timesheet/bulk-row parent identity independently of the absent proposal; mount only its retained plain notice/Try again control in that existing parent surface. New decisions still require the qualified native descriptor. No SQL, persisted store, protocol or financial-owner change is needed. Reported to root immediately.

Current accepted-map retirement now uses the current WeakMap array, before any caller refresh; this also avoids reintroducing a settled record through a stale captured array. Accepted truth remains separate from refresh errors. Source closure remains OPEN only for the concrete remount recovery finding and final fresh-byte review; joined acceptance is NOT_RUN.

Fresh T10-02 partial repair is present: main.js now captures the actual unchanged Simple Timesheet / Bulk active-row subject, can recover the old root only from that same retained namespace, and inserts the retry section in the native parent when proposal NONE removes its section. One genuine NEXT subcase still needs the existing informational wrapper fallback: presentation.js:2141/:2174 return renderNextInformation when lifecycle is not complete, and :1649 emits `[data-weekly-source-informational="NEXT"]` without either simple-lines or right-pane. The independently qualified ROOT_ACTIVITY_NOT_INDEXED proposal can start in this branch, then KEEP removes the proposal. The current fallback selector therefore still has no parent there. Extend only the owned existing informational surface fallback, retaining the same subject/current guard; this is the same T10-02 recovery finding, not new financial scope. Reported promptly.

## Final fresh-byte closure — SOURCE PASS

This section supersedes the intermediate OPEN verdicts above. **T10-01 and T10-02 are CLOSED in the current saved product; no remaining essential transport defect was found.** This is a source-only verdict, not joined, visual or business acceptance. D03's S3 enqueue and unbound paid Source-removal authority remain BLOCKED as separately documented; this transport review does not close those rows or the whole item.

T10-02 is closed by the actual subject qualifier at main.js:179354–179385, existing-record recovery at :179368–179372, and bounded owned-modal fallback at :179389–179401. The final fallback includes the existing NEXT informational wrapper at :179395, so both ordinary lifecycle and independently qualified NEXT informational KEEP→NONE remounts can display the original retained Try again. The retry uses the original frozen command without calling the fresh descriptor builder; a new decision still calls that existing builder. The normalized member flag is genuinely `is_requested_root` (presentation.js:751–770), and no UUID is inferred from row labels or history.

The reviewed Worker boundary closes this action alone to action/payload and seven browser operands, supplies the authenticated Office actor as the eighth native operand, accepts PostgreSQL's full digest-UUID domain, refuses unsafe revisions, and calls only the existing named native owner. The fixed owner-status dependency returns actual fresh NEXT authority. Existing body bounds, admin authorization, Source admission/ordering inside the native owner, and managed-root rollback recording remain in place. The five native SQLSTATE/message refusals are mapped only after the native replay/idempotency seam; other uncertain outcomes remain uncertain. Other route branches and their error behavior are retained.

The Office record captures signed actor, original modal context and complete frame stack, actual Simple Timesheet or Bulk active-row identity, original root, endpoint and original serialized command. Busy is set synchronously before dispatch. Opposite decisions cannot replace an unknown request; native remount restores only its retained control. A validated receipt must match the original decision/bundle and exact original-or-successor revision qualifiers. Accepted records are removed using the current WeakMap array before independent refresh; acceptance remains true if refresh fails. After-await actor/context/subject guards prevent late effects on another parent. Definite original refusals retire the record; later owner/auth/network failures cannot erase an unknown original attempt. Retention is in the existing page closure only; cross-page/reload recovery is not claimed.

The final diff adds one index dependency, the named route branch, the wrapper/status seam, caller retention/currentness and small existing refresh fences, plus cache versions. It changes no SQL, money calculation, Rule B cut, producer, queue, command kind, persisted journal/table or screen layout. Current API remains the page's existing fixed broker resolver; this review adds no endpoint proof framework.

### Reviewed SHA-256

| Saved file | SHA-256 |
|---|---|
| BUILD-BE `broker/src/index.js` | `0b50987a43f2b76c069eb1e08c68c44cb87f4a073b8a6667de9b74ccfca21729` |
| BUILD-BE `broker/src/weekly-source/routes.js` | `ec683c653f32bbe1c70f8e7a872138119273bcacbea51b0049799264ec77a5f1` |
| BUILD-FE `js/main.js` | `4f17687963fd3d8ea0bf1c65b2a8fdb6a213f7ac06dee0b65aaa3db2857fe294` |
| BUILD-FE `js/weekly-source-presentation-v1.js` | `00609dd3044b74c29b0c4c630c1a323cdc5348f2eeb8eda5f0c97210a678122d` |
| BUILD-FE `index.html` | `65aecb311b2d8885e19a03375c27f559373f37f5d73b88295f1bc452c8993599` |
| `design/SP-10-transport-design.md` | `5578ed414891260adcac117ebdb5f02c13cb029597174f2b33e5ad7a6f82ce1c` |

No product edits, runtime execution, tests, SQL/RPC, data, fixtures, render/capture or hosted calls were performed by this reviewer. Root owns any separate normal checks and acceptance evidence.

## Final current-source supplement

Independent fresh reread retains **SOURCE PASS**. Current actor/frame/context/actual subject and NEXT informational NONE-remount retry guards remain as reviewed. Accepted retirement still occurs before the independently guarded existing refresh; unknown retry still uses the original frozen bytes/key and never follows the new close/reopen remedy. That remedy is displayed only for stale fresh-decision preparation, definite original refusal or already-confirmed acceptance with unavailable refresh. No new dispatch or authority predicate is introduced by the wording.

Presentation-only changes add phone row labels, suppress technical bare Source reason/Contract UUID copy and format the genuine native recorded timestamp in London UK notation. Existing captured hour values and all descriptor qualification remain unchanged. Scoped existing-modal CSS supplies44px decision targets and a bounded right-pane layout. Cache versions include the changed CSS/main/presentation assets. These are source/presentation checks only; pixels and joined interactions are NOT_RUN by this reviewer.

The current hashes below supersede the earlier product hashes in this receipt; T10-01/T10-02 remain CLOSED.

| Saved product | Current SHA-256 |
|---|---|
| BUILD-BE `broker/src/index.js` | `0b50987a43f2b76c069eb1e08c68c44cb87f4a073b8a6667de9b74ccfca21729` |
| BUILD-BE `broker/src/weekly-source/routes.js` | `ec1a819336daeb552791af5170433d3925c9ff48e7d8c422dd3b0d92d14e6805` |
| BUILD-FE `js/main.js` | `87c66ac085c0a7c73413923c5fb853b841ad7fd4e777bcca3bddcf37ca30dd25` |
| BUILD-FE `js/weekly-source-presentation-v1.js` | `76caed36345211c316b1f3cdb420e49f5f0c3fc829ffb62cb5d2ee34998de74d` |
| BUILD-FE `css/weekly-source-presentation-v1.css` | `0cda5dde5e515e679ee8ae4f8d87a3d0b76f4ee1547732bbe172e5430b31bc99` |
| BUILD-FE `index.html` | `4410ed2d0dd2f903b0a06bea2cf65010378179fcd2c6f9676ab7e0f3c398e602` |

This transport supplement does not assert the removed-component helper's joined first use or resolve S3; its actual source verdict is separately saved in `SP-10-removed-component-sql-review.md`.
