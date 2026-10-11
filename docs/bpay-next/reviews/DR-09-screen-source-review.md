# DR-09 — independent Office screen-source review

Initial checkpoint, 11 October 2026. **CHANGES REQUIRED; source-only historical checkpoint. A4 pixels NOT_RUN at this checkpoint; final current receipt below.** Root alone edits product. Read current uncommitted Office source, full DR09 card249–272, current build scope/design investigation, current native frame/button rules, WB21 retained store and supplied B4 SCR42/SCR42b/SCR43 specs/PNGs (read only). No product/pack/tool/checklist writes, renders, tests, DB/hosted requests, fixtures, business actions or commits by this reviewer.

Inspected source: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\\js\\bpay-next-office.js`, SHA256 `be6ad370f596edae22532972c7469bb80a6d8e2d02210003f706477cf272a424`, before root corrections. Current controller carries exact PREPARATION_DISCARD input/result and existing pending bytes; PROGRESS24 fields / WORKERS two discard fields are additive own-card facts. SQL correctness/installation is a separate review, not approval here.

## Immediate findings

| Finding | Current source / trigger | Narrow required correction |
| --- | --- | --- |
| PDR09-O01 — PARTIAL issue control rejects real blocked scope | Office1167 checks facts.review >0; Office1204 always offers Review payment issue for PARTIAL. A protected READY survivor can produce discard_blocked_candidates >0 with review0, so the visible button throws REQUEST_INVALID. | Permit the current native PARTIAL/blocked result through this existing review handoff; retain actor/owner/pending guards. Do not invent a new issue reader/action. |
| PDR09-O02 — unknown discard replay does not refresh its outcome | Office1092 review-host recovery still assumes worker receipt.action and calls updateAction/readRows only. PREPARATION_DISCARD receipt has no action. A successful exact replay leaves pre-discard progressFacts, no discardRequested and no CANCELLING timer; old controls/header can persist until manual Refresh. | Branch on the retained pending action before retry, retain identical journal bytes/UUID, then follow accepted discard through existing PROGRESS/refresh and original return callback. Preserve separate worker replay logic. |
| PDR09-O03 — premature completed actor label | Office1070/1180 says Discarded by for CANCELLING (discarded_at null) and PARTIAL (blocked survivors). CANCELLING is still running; PARTIAL does not mean the whole Draft was discarded. | Render a plain factual requested/in-progress/partial actor label for those phases; reserve completed Discarded by for DISCARDED. Native timestamp only, never browser completion. |
| PDR09-O04 — destructive footer placement/extra buttons | Office1096/1203 adds ordinary button Discard this Draft without data-next-destructive. Native verb matcher colours it red, but has no left-placement rule; scoped Office382 margin-right:auto requires the marker. PREPARING/REVIEW with review>0 can also expose Discard + Review payment issue + Refresh + Continue (plus More), exceeding policy10's one-primary/two-other limit. | Classify whole Discard explicitly as secondary native destructive and place left using existing scope. Keep necessary card controls while putting the extra rare action in existing More; do not add a new menu/framework. |
| PDR09-O05 — duplicate progress feedback | Successful whole-discard receipt sets result=Discarding Draft; render1175 adds that notice and render1180 adds the same factual discardResultCopy notice for CANCELLING. Both survive until completion. | Once native discard facts exist, show one truthful state/count notice; suppress the redundant transient result copy. |
| PDR09-O06 — disabled Confirm after accepted discard | Review1097–1098 shows Back only for terminal DISCARDED/PARTIAL, otherwise renders Confirm Draft. CANCELLING therefore retains permanently unavailable Confirm without a reason or purpose. | Omit Confirm once discard_phase is non-null; retain real Refresh/recovery/Close and terminal return controls only. |
| PDR09-O07 — blocked reason lacks a useful remedy | Office1084 uses This payment is already protected (technical/opaque) or This Candidate could not be removed (repeats badge). Neither tells payroll what to do. Existing reader provides only two protected codes, no finer confirmed/provider state. | Use one plain truthful cannot-remove explanation plus a real Refresh/contact-support next step; do not fabricate paid/submitted/bank/unknown details or introduce a new server field to fill the design. |

Small wording boundary: confirmation984 and partial996 hard-code Candidates even for1; use native integer count formatting/singular form. Counts must not become browser page counts. This is a manual policy16 finding, not a pack tool run.

## Source checks retained / limits

- Whole confirmation is small/native, title names original active scope and actor/start, consequence is supplied ticks-kept wording. Safe Keep Draft is appended first; destructive submit carries native btn-warn/data marker. Current owner11 destructive-left visual rule is distinct from initial safe focus; final actual comparison must record that difference from the supplied confirmation PNG.
- Whole discard is reachable only for native unconfirmed PREPARING/REVIEW with no existing discard, pending request or worker action. Current original review_revision (including0) is sent; confirmed/expiry handling stays separately typed. Empty native receipt gives a genuine no-op. No browser-cancellation shortcut.
- Current receipt validation distinguishes CANCELLING, DISCARDED, PARTIAL and separate removed/already-removed/blocked counts; completed whole-return text is not used for PARTIAL. Protected frozen amounts remain reader values, not recomputed.
- Per-Candidate Remove remains DR05 WORKER_RELEASE with original worker/revision and safe confirmation. Added PREPARING availability must be proved against the current native release successor by its source owner; no UI assertion broadens admission.
- On known DISCARDED completion, the original frame closes and calls onPreparationDiscarded: Banking Pay receives an inline result and existing shell/list reload. WB21 original choice memory/intent is retained; no discard/reset/array reconstruction. Its maintained reference supports original saved selections. Actual result visibility/focus/selection remains unproved until screenshots/joined acceptance.
- Current WORKERS still has no factual human person/reference. Generic Candidate/removal subject cannot pass full who/wrong-person review. DR15 identity/detail completion remains explicitly DEFERRED; no fake names, extra financial columns, useless caret or future Draft detail control.
- Current original72-hour expiry/confirmed no-expiry state remains separate from actual discard actor/time; no expiration receipt is relabelled discard. Existing progress owner/scope/read-ticket and live-header guard are retained. Native race/ordered recovery are source only.

## Required actual screenshot boundary

Root should capture real native product output with the smallest fixed replies, all dark/device-light/390px phone: whole Discard confirmation from progress/review; cancelling in progress/review; genuine EMPTY no-op; full DISCARDED return in Banking Pay with original ticks/result; other-user/reopened discarded actor/time; PARTIAL exact removed/already/blocked counts and review handoff (including review0/blocked>0), blocked rows/reasons/unchanged amount; unknown original-discard response and successful exact replay/readback; running per-Candidate Remove confirmation/result; confirmed no Discard. Reuse existing narrow local capture route, with no DB rows/hosted call/test programme. Actual full42 policy/user-eyes review and every visible design difference/control-purpose list remain pending; this source checkpoint is not A4 or whole-item PASS.

Supplied read-only design root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\cloudtms-backend\codex_outputs\claude-stage2-continuation-20260925\SCREEN_DESIGN_POLICY_20261010`; exact specs/PNGs `batches/B4/{specs,png}/SCR-42_discard_draft_confirm`, `SCR-42b_draft_discarded_result`, `SCR-43_remove_candidate_confirm`. A2 requires existing factual values only; all missing full DR15 context must be named/deferred rather than invented.

## Corrected-source checkpoint — 11 October 2026

**CHANGES REQUIRED: PDR09-O01–O07 closed in source; PDR09-O08 remains. A4 pixels NOT_RUN.** This additive checkpoint preserves the original findings above. Re-read saved Office SHA256 `d7ee44f304ea12c1a271ba0f73848d1be366fb9e89257bd60e4b6dd58d890cbd` and scoped Office CSS SHA256 `37e3e29bc92aa3abfff3da897137694436d682cc3aaf7b4e019619371cb73c4a`; uncommitted DR09 boundary. No execution or rendered approval.

| Finding | Source result and evidence |
| --- | --- |
| PDR09-O01 | CLOSED: issue1168 accepts native PARTIAL with positive blocked count even when review is zero; current/read-error/pending guards remain. |
| PDR09-O02 | CLOSED: recovery1093 retains the pending action before exact retry; PREPARATION_DISCARD sets known discardRequested and calls original refresh/PROGRESS instead of the worker-only update path. Worker recovery remains separate. |
| PDR09-O03 | CLOSED: discardActorCopy999 uses Discard requested by for CANCELLING/PARTIAL and reserves Discarded by for DISCARDED, adding only native non-null completion time. |
| PDR09-O04 | CLOSED for placement/button count: review1097/progress1204 carry the existing destructive marker; rare Review payment issue now uses existing More, leaving the allowed primary/two other global actions. The new menu layout defect is separately PDR09-O08 below. |
| PDR09-O05 | CLOSED: progress1177 suppresses transient result once discard_phase is non-null; one factual native discard state/count notice remains. |
| PDR09-O06 | CLOSED: review1099 omits Confirm once discard_phase is non-null; CANCELLING retains actual refresh/recovery/native Close, terminal phases have Back. |
| PDR09-O07 | CLOSED: blocked row1085 gives one plain cannot-remove explanation with Refresh/contact-support remedy; no finer payment/provider fact is fabricated. |
| Singular count wording | CLOSED: confirmation984 and partial997 branch on the native exact integer count for Candidate/Candidates. |
| PDR09-O08 — More options overlap | OPEN: PREPARING with review >0 appends both Continue processing and Review payment issue as direct .btn children of one details1202–1203. Scoped CSS471 positions every direct .btn at the same bottom/right coordinates; phone CSS482 likewise shares the same left/bottom. Both actions therefore occupy the same popover position. Required narrow correction: stack the two existing options in one popover layout; retain their existing action guards, labels and native controls. No new menu/action/reader is needed. |

The source checkpoint covers control/recovery integration only. Human Candidate context remains DR15 DEFERRED, and actual final dark/device-light/phone policy/user-eyes comparison remains pending root-supplied screenshots. Native ordered mutations, race acceptance, retained-selection return and whole-app behavior remain NOT_RUN by this reviewer.

## O08 corrected source — 11 October 2026

**PDR09-O08 CLOSED in source and first dark pixels.** Re-read Office SHA256 `45b3ec36234349d2f950d5d6d0187f4ff5d16d2c3b4532b45a438e0d3ae7046b` and CSS SHA256 `0efedaab60da9575075cf07544af468fd9e854ab82ab2f1a7c53ded3cfa3195d`. Office1202 moves the existing options into one bpay-next-progress-more-options container; scoped CSS472/484 positions the flex-column container once. First DR09-more-dark actual PNG shows both controls separately. All O01–O08 source findings are closed at this boundary, with no mutation/recovery runtime assertion. New actual pixel findings P01–P04 and return/counter coverage limits are recorded immediately in DR-09-screen-review.md; final A4 approval remains pending corrected captures. This does not retroactively replace the historical hashes or approve later source edits.

## Final DR09 current-source receipt — 11 October 2026

**PASS for inspected Office/control/recovery source integration; O01–O08 and singular wording closed.** Current actual A4 scope separately approved in DR-09-screen-review.md and four folder DR09-review.md receipts: all39 final PNGs viewed, P01–P06 closed. Full DR15 human identity/wrong-person/composite review remains DEFERRED; joined/race/ordered request execution remains NOT_RUN. This final current receipt supersedes pending historical checkpoint statuses, preserving their exact original hashes/findings.

Re-read exact saved Office confirmation/review/progress rendering and scope guards, original exact request recovery, native parent callback and scoped CSS473–489. PREPARATION_DISCARD retains current run/review_revision; unknown recovery branches on original pending action and refreshes native PROGRESS, retaining same journal bytes/UUID. Native CANCELLING/PARTIAL uses request actor; only completed DISCARDED gets completed actor/time. One state notice, no accepted-discard Confirm/extra Discard, no empty Actions column/footer. Progress Discard is prepended secondary red left; two More actions share one stacked container. Phone review Back and main Filters/Refresh/tabs have narrow44px rules; no shared framework or new financial reader/action/job kind.

Full native WORKERS is still an ordered outcome list with factual frozen_ex_vat/missing em dash, not DR15's person/detail reader. Current PARTIAL reader intentionally projects ready0/review=blocked; initial requested alternate sample is withdrawn. Whole known-success return retains original WB21 choice store and uses actual existing callback; actual local parent pixels show green result and checked native Candidate. These observations do not prove durable joined retention or money/race acceptance. Genuine EMPTY is source-only in this batch.

Frontend saved DR09 boundary: base HEAD `6ce81d34941e09e05b4045ce550c16cc0f96123c`, Office/CSS uncommitted at review; no later DR11 SQL source reviewed or attributed here.

| Source | SHA256 |
| --- | --- |
| js/bpay-next-office.js | `8b9e437170c1cb015d13391447d3fb20aa39b37718de9040dc0e5051097abdf3` |
| js/bpay-next-workbench.js | `f01b5540b281e9ee30b45098811f6ec7c8067ddad93f9247faa4bea760990e3e` |
| css/bpay-next-office.css | `2ead8368e6f134ee4ef5c63d65092a1add29b2a96890494d4528409dc7d682ac` |
| index.html | `603549abac46dff55ab8c90e86ac6da335dddab5221e1961fdb987f2df086609` |

No product/pack/checklist edits, renders, tests, DB/hosted calls, fixtures, business actions, deployments or commits by this reviewer. Current exact PNG/fixed-reply and read-only supplied-design hashes are in the four DR09-review.md receipts.
