# F-16 Office source port design

Completed 9 October 2026. Read-only investigation/design; no frontend or backend product edits, tests, browser, runtime, SQL, fixture or hosted execution. Root alone writes product. Companion `F-16-office-port.inventory.json` records exact raw/LF hashes, bytes, complete source patch hunks, matched current contexts, overlapping upstream hunk coordinates, preservation-file hashes and critical fresh commit contexts.

## Controlling scope

Read BUILD-FE AGENTS.md/AGENTS.override.md, BANKING_PAY_BIBLE.md in full (778 lines), F-16 card and D01–D07 checklist, start sheet, binding answers and B11. Desktop overrides cloud restrictions; start sheet overrides the older mandatory fixture/testing/approval programme. The Bible's Policy X, immutable financial authority, same-modal adoption, current payable-group and history rules remain. This is a foundation frame port and owner-read gate, not proof of installed/API/browser joining or completion of later feature screens.

F-16 is the screen prerequisite. Retain genuine shared Source, invoice, Timesheet, Candidate and MyTMS contracts. OR-45 removes any old Banking operational-preservation/compatibility requirement; harmless dormant code may remain. OR-44 freezes the financial state at Create Draft; complete only actual capture gaps through their owning cards, do not move the freeze to opening this sheet. OR-46 permits later Candidate pay-method changes for the next Draft while the existing Draft keeps its frozen channel/creditor. OR-47 supports one/several exact shifts OR whole Timesheet snoozes with mutual exclusion, preserving unsnoozed same-day shifts. No F-16 financial recalculation, rate conversion, eligibility determination, new snooze policy or mutable post-Draft enrichment.

New NEXT labels must say Payment Advance; keep LOAN as stored/wire kind only. Keep SOURCE/PROTECTED_WORK/WITHDRAWN in the saved contracts and required display kinds. UK ordinary event times use DD/MM/YYYY hh:mm:ss, 24-hour London display; explicitly captured local/UTC clocks are distinct labelled evidence and may not be replaced or invented. No switch screen, switch permission, Golden Key or reauthentication flow is added here. Payment Summary is separately approved/owned, not part of this foundation frame.

## Exact authoritative sources

OFFICE is `.codex-worktrees/banking-pay-reset-office-20261004`, saved dirty tree on base `7a75111ec2923693e75feef4a7ccbcb6ecba942e`. BUILD-FE is `.codex-worktrees/bpay-next-build-frontend-20261009`, fresh base and observed HEAD `aada67455afe5a3b6828a713c7062832c0617782`. Saved files are authoritative. Do not copy OFFICE main/index wholesale. Do not restore/copy its older Weekly Source or Candidate assets, old cache markers, or fixtures/runners/browser artifacts.

| F-16 row / file | OFFICE raw bytes / SHA-256 | Current BUILD-FE raw bytes / SHA-256 |
|---|---|---|
| D01 `js/bpay-next-office.js` | 79775 / `c19f87424e7022528c41d58a444af112b1ae4e0b76b22e95b759728ce0913c51` | ABSENT |
| D02 `js/bpay-next-payment-history.js` | 43700 / `d8af52c1402b1c700dc0170ed9e1c61edecb75f32f966a6ec2980a820d87addf` | ABSENT |
| D03 `css/bpay-next-office.css` | 1997 / `5fa3e52ab734f0fee7071d364f57c44e7130478648fe797d98bf3b6804b50b1f` | ABSENT |
| D04 `js/main.js` | 17620016 / `abdfd5208f0849af97dca165af6a00ef0ad4c46f6078f12f57b8cb147e1dad55` | 17748010 / `8f36a37810b61552359aa2762181ab045865566389c1f2888265682a8c2b28d0` |
| D05 `index.html` | 98666 / `371222f5750583419185aeec9dbf7089fe2e1317262f45533696af4effdc52f5` | 99657 / `588ca10ba04a55d1d455932f719f1ab12e81809862ea8370beccda203cca3722` |
| D06 `BANKING_PAY_BIBLE.md` | 147494 / `b0fdfedf11b20dbcadd2f187aa7519a7ef32d2e7d93e0c37100604b4c62e91f4` | 138718 / `84435631b0211f058128d0cf9a1801aa194f2061f8bc1f2d0f906654b17bc2f2` |

D01/D02/D03 are the three absent standalone product files. Copy those source bytes as the starting point, then make the bounded functional/owner/read-copy completions below. Do not copy any OFFICE tests/fixtures/.local-next-office-artifacts or Playwright configuration during F-16. The raw copies are thin caller references, not final acceptance claims.

`js/bpay-next-office.js` is a UMD/IIFE browser global `CloudTMSBpayNextOffice`: strict request/response schemas, explicit factual pages and frozen detail, actor-scoped sessionStorage key `cloudtms.bpay-next.v1.<actor UUID>`, journal version 3 with narrowly validated complete v1/v2 migration, exact saved mutation bytes/UUIDs, no automatic unknown-outcome mutation retry. Unknown receipt disables conflicting controls and offers Retry exact request. Accepted work/case page counts and sealed PAYE/Umbrella Draft addresses survive close; unsubmitted page selections do not. The separate host has role dialog/aria-modal, Escape, keyboard focus trap, focus restoration and a bounded factual progress poll; close clears poller/host while retaining journal. Candidate bank editing delegates only exact Candidate ID to the current `openCandidate` owner, never a bank tuple or proposed frozen replacement.

`js/bpay-next-payment-history.js` global `CloudTMSBpayNextHistory` uses the existing showModal framework, a 52-section allowlist/manifest, explicit bounded read paging and captured parent links. It retains one page, exact scopes/cursors/upper bounds, distinct source/target/VAT/NET/cash fields, exact decimal text and NULL. Opening normal records must not fetch history itself; history opens only from an explicit qualified record action. No aggregate, all-history scan, fake move/comparison attribution or mutable-price fallback. Actor changes abort/clear reads. Both modules use escaped/textContent labels rather than interpolating untrusted HTML; advice remains the approved streamed original-instruction path.

The 10-line stylesheet is scoped under `.bpay-next-office`; a fixed 100dvh/safe-area flex host keeps Close outside the bounded scrolling body, tables own overflow, focus-visible styling remains, and phone header controls keep their minimum height. Retain current global modal/dark-theme and Candidate stylesheet authority.

## Minimal three-way hunks onto fresh saved files

Static context analysis of `git -C OFFICE diff 7a75111 -- js/main.js index.html BANKING_PAY_BIBLE.md` found eight main.js hunks, three index hunks and one Bible append. Every old main hunk sequence matches current BUILD-FE exactly once after LF normalization. Some upstream changed ranges intersect the larger hunk context, but the exact old sequence still exists; that is not permission to replace nearby fresh content. The index CSS hunk is the only context that no longer matches. This analysis is not an executed git apply/merge test.

| File / hunk | OFFICE base lines | Fresh saved context line | Minimal treatment |
|---|---:|---:|---|
| `js/main.js` / 1 | 5042 | 5056 | Add exact-path/method APILOG streaming suppression; preserve current authFetch body. |
| `js/main.js` / 2 | 5055 | 5069 | Move existing url declaration once; keep current auth modes/header/session/401 retry owner. |
| `js/main.js` / 3 | 6309 | 6329 | At clearSession close NEXT host, fully dismiss exact history frame, clear owner cache; preserve exact journal. |
| `js/main.js` / 4 | 6962 | 6982 | Add owner-gated pay_next nav entry without replacing other current nav structure. |
| `js/main.js` / 5 | 6985 | 7005 | Route pay_next through qualified owner helper and existing callable API/actor/Candidate owners; deliberate fenced legacy destination display. |
| `js/main.js` / 6 | 325359 | 327036 | Add btnPaymentHistory to retained read-only header action allowlist. |
| `js/main.js` / 7 | 325384 | 327061 | Permit only read-only history paging buttons under banking-pay-history frame; keep current Candidate finance-report rule. |
| `js/main.js` / 8 | 331667 | 333350 | Add qualified explicit Candidate/genuine Timesheet history button before current Contract button rules; preserve current primary-record ownership. |
| `index.html` / 1 | 10 | NO exact context | Insert NEXT CSS beside current Banking CSS; retain fresh Candidate marker and all newer Source CSS. |
| `index.html` / 2 | 2869 | 2871 | Insert hidden type=button btnPaymentHistory beside Related/Close; retain current shared modal frame. |
| `index.html` / 3 | 3015 | 3017 | Statically include NEXT owner helper, Office and history before main.js, after required definitions; retain every existing include. |
| `BANKING_PAY_BIBLE.md` / 1 | 776 | 776 | Append all four dated OFFICE local-source entries as history, then current F16 build entry; retain original full Bible. |

Current Candidate CSS marker is `20261005-candidate-header-r2`, not OFFICE's `20260907-timesheet-layout-r2`. Current Source CSS is `20261009-status-colours-r2`. Keep both. The existing Source include block is authoritative: combined-finalise `20261009-status-colours-r1`, combined-review/import-workspace `20261007-recheck-recovery-r1`, protected-shift-editor `20261007-history-r1`, workspace-actions `20261009-contract-checkbox-r1`; MyTMS Office is `20261009-contract-invitation-save-r1`. Keep current main.js's entire accumulated query/cache key; append a current F16 marker if needed rather than replacing it with OFFICE's older script element.

Add a separate clearly labelled NEXT static include block. Suggested dependency order: `js/bpay-next-owner.js`, `js/bpay-next-office.js`, `js/bpay-next-payment-history.js`, all before main.js invokes them. Root should use one current F16 cache marker for changed NEXT JS/CSS and record exact include lines. Preserve every pre-existing Candidate/Source/Banking definition script in its current order. No dynamic guessed loader.

## Concrete caller gaps requiring minimal completion

1. **History URL bug.** Current and OFFICE `API` is `(path)=>BROKER_BASE_URL+path` (main.js:42). OFFICE history main hunk passes `api: API`, but history module:212 uses `String(options.api).replace(...)+/api/banking/pay/next/history-page`. This stringifies a function into the URL. Smallest hunk completion: pass `api: BROKER_BASE_URL` for the history caller only; keep callable API for thin Office/new owner helper. Alternatively explicitly support a callable API in history transport, preserving exact route. Existing isolated history tests supply a string and do not prove this real main integration. No tests were executed here.

2. **Exact streaming exemption.** OFFICE main authFetch change uses `.pathname.startsWith('/api/banking/pay/next/')`. F-16 requires exact NEXT paths. Use method/path allowlist for POST command/preparation/history-page/frozen-detail, GET preparation-page/owner-status, and GET advice with the fixed UUID shape. Preserve existing auth401Mode/Authorization/session/refresher/CORS behavior and never clone.text/log these financial replies, even under opt-in debug. No generic /next exemption.

3. **Typed owner-stale completion.** The thin Office transport:200–203 validates error_code but replaces it with generic SERVER_REFUSED; there is no owner helper or stale callback. History's allowlist likewise excludes owner-stale. Preserve an allowlisted typed owner-stale code separately, then invalidate the one-open owner/epoch helper and re-read while disabling money controls. Do not expose the raw backend code, infer LEGACY, delete exact pending bytes, change command IDs, submit a replacement intent or automatically replay a mutation. Requalify the same actor/open identity before publishing a response. The actual root Worker stale code must be matched, not guessed from a substring.

4. **Visible history logout.** Thin Office.close removes its host. History.close calls onDismiss/client abort but does not call the shared modal close owner, so its visible frame survives. Current clearSession does not dismiss modals, and performLogout hard-closes all frames only for inactivity. Complete manual/session-clear cleanup for the exact current `banking-pay-history` frame through current `closeModal()` (fresh main.js:350840), with its onDismiss cleanup. Do not broadly rewrite Source/Candidate modal lifecycle or clear actor-namespaced mutation journals. Root may pass an exact dismiss callback or perform an exact-kind closure in clearSession; avoid onDismiss recursion.

5. **Payment Advance display.** Stored/wire case_kind LOAN remains valid. Office current Case table:460 renders `c.case_kind` raw; history displayScalar returns nondate values unchanged. Add only presentation mapping for LOAN→Payment Advance and human state labels as necessary. Preserve complete schema/wire values and exact journal bytes. Technical scope identifiers currently appear in the thin diagnostic frame; later feature/history cards own full staff-friendly screens, and F16 must not claim that a raw-source thin caller is final programme UX.

## D07 owner helper and gate contract

New `js/bpay-next-owner.js` must perform only authenticated admin GET `/api/banking/pay/next/owner-status` through the current authFetch/callable API, with bounded timeout/body and exact two-field validation. active_owner in LEGACY/NEXT/DISABLED; owner_epoch positive decimal text <=PG bigint. No mutation, Source/Draft eligibility probe, work/financial reload or old-engine fallback during this lookup.

Use actor+one Banking-open identity and coalesce concurrent consumers into one in-flight read. Retain the accepted owner/epoch only for that open; a new Banking open reads once again. Distinguish loading/unknown/failure from LEGACY. NEXT enables the NEXT nav/history consumers; DISABLED shows Banking Pay is paused; LEGACY shows deliberate NEXT inactive; unavailable owner disables money controls. Check actor/open generation again before accepting late replies. On explicit close/next open/logout clear the appropriate open cache; on typed owner-stale clear and re-read without changing pending mutation identities.

Main nav, thin Office.open and explicit history action must qualify ownership, not merely hide a menu item while an old direct entry can still launch. Under NEXT, known fenced legacy Banking destinations show unavailable under Banking Pay NEXT rather than raw 410; preserve Source/invoice/Candidate bank edit and independent actual bank review owners. No repairs to keep the old Banking engine running.

**Record-entry design detail:** Candidate/Timesheet history needs positive NEXT qualification even when Banking has not previously opened. A peek-only empty cache would hide the button indefinitely. Root should use the same coalesced helper to qualify the eligible active record consumer (once for that record/open scope, or reuse an already-current Banking-open answer), then rerun only the current frame's button update. No history read occurs until the explicit click. Keep frame/actor tokens so a late owner lookup cannot enable a different record or old session. This is display qualification, not a money/action owner, and must not rebuild the Candidate modal.

The historical-reader backend can retain its explicit workflow-independent read contract; F16's screen visibility is still gated as the card specifies. No owner setting is changed by these screens or the port. F15 owns exact GET authentication/403 and service-only backend read; F16 reuses it.

## Preserve newest upstream behavior explicitly

Fresh base contains 23 changed JS/CSS product paths since OFFICE base. Their complete saved hashes are in the inventory. Main/index are patched narrowly; every other listed source asset remains byte-identical. In particular preserve:

- Candidate active-state staging/header/read-only coloring from 9100bc5/83103d2/2a9201e, current candidate-office-v1.css and genuine primary-record frame checks.
- Contract duplicate-assignment filtering by pay method and vacant copy workflow (87b385c), contract authoriser authority and post-save dirty state (99243cd), ad-hoc pattern clear/lock ownership (5ca04b4/e8eee40/06a35c2), backdated source-created Contracts (3b5c88c).
- Complete clean Contract save and post-save MyTMS invitation (aada674) in upsertContract, restoreModalDirtyAfterRender, resumeParentAfterChildReturn and saveForFrame; keep latest mytms-office-v1.js. History-button insertion must not replace `_updateButtons` or the save pipeline.
- Current protected-shift picker/accepted pending protected-pay Save (21e83df and later), exact recheck recovery and authoritative Source display/search/finalisation (d35c40c, 36d2b7e, 948a9a5, 6835c69 and later). Keep all `js/weekly-source/*`, current Weekly Source presentation modules and status styles. Do not import OFFICE versions of them.
- Shared modal layout/modernisation, Candidate/Timesheet summary, bulk-authorise lifecycle, manager authorisers and invoice modules. No rewritten upstream closure, selector or owner merely to simplify the port.

| Preserved current product path (except narrow main edits) | Raw SHA-256 |
|---|---|
| `css/candidate-office-v1.css` | `3c6496c94a2667552b76ccc4539702f72d0f62aa148fc7edbd5506cf6459fc4b` |
| `css/invoice-batch-modal.css` | `531289a33e680e3bba3799b51721e5cb8e2f853f587f1c9f2bc0df73e7e1989c` |
| `css/modal-modernisation.css` | `266e62f9d4ebf9b3e1d4bfb99b0ac63af0eda67f34afacc97230e0c7579ddd57` |
| `css/summary-modernisation.css` | `263ca2842777d3aa5abf7419c61c2a93cb0b5735628091f2dcb77a1c992db6dd` |
| `css/weekly-source-presentation-v1.css` | `db065bfdfe41514273b0b7da448621a697db559f31f45ee9cfe039b641f683b9` |
| `css/weekly-source.css` | `0fdaa03c67663c8ac442283ca64fc2acbaf4537d3d44abdcc24ce2399ca66b24` |
| `js/bulk-authorise-lifecycle-v2.js` | `cccef0842ed3234059bdb2cf63e64ceae0b28f1118a3aed969e282d149d2dce8` |
| `js/import-review-v1.js` | `684a1e38eacb3d7a745b1687ac6411ef06096a09611a0e67cc6a318192212593` |
| `js/invoice-batch-modal.js` | `dbcbe229ce2995c35b032db679b78547c9b447927c2e55987e05fc85669c5f7a` |
| `js/main.js` | `8f36a37810b61552359aa2762181ab045865566389c1f2888265682a8c2b28d0` |
| `js/manager-authorisers.js` | `ba4161223fd67163a7b3164499718b408eb3c7f59df7667fd33929643ba09b09` |
| `js/modal-modernisation.js` | `7c3ac05d122089765d9ba6bcadd1138ccf7f7deed32e6a22d1fccfe5d3e3be4e` |
| `js/mytms-office-v1.js` | `cff7c52e994510dd5c6aed7366c29b0172acb1dbb68420fae02af6b776f14867` |
| `js/record-modal-layout.js` | `c9901cdefdb18f6de6206373c252de3b60e5f1473d31ba73ba2850124fffe54c` |
| `js/weekly-source-presentation-v1.js` | `fbf280c1f01c5b1f1363226142256b11db51c4d9d30a41132e6a24d728911713` |
| `js/weekly-source/combined-finalise.js` | `da0fa4532a4a5c0282f8eee5ad8dae2b9fa2cb53739052304166e91a52ac11ba` |
| `js/weekly-source/combined-review.js` | `f950a841aa7b54adbdf0591ef378453bff3e8f01287e45afe8449196f5c6b99e` |
| `js/weekly-source/finalise-batch.js` | `f099aba88dec4007347069b6021e9db73048579194bd4ff7b910d59bf8320231` |
| `js/weekly-source/import-workspace.js` | `ad0952f73c3cb06ad1cb079f9858329948694ff72968b89243eee0128b491a27` |
| `js/weekly-source/office-notifications.js` | `9a2e0979e9604f9c1412e42c79af3451ab8395aae8b774e19f15ff7cebdd6bdf` |
| `js/weekly-source/protected-shift-editor.js` | `b2cf7f2775608588c5f9dc6137f271ecda89d7154ec26bbbe9f875792de68f46` |
| `js/weekly-source/settings.js` | `879059bdb212cfd4bb333470ee3bf029b62fccfab397a2237c2b573513a578b8` |
| `js/weekly-source/workspace-actions.js` | `35c53fd6f78e11f73bd68383e347b6ad464cb3960113b9592746a9d60af08b22` |

## Root handoff and limits

Root may apply the explicit OFFICE patch three-way onto BUILD-FE and resolve the CSS/version context by insertion, or transcribe the documented minimal hunks. Re-read saved destinations immediately first; preserve new concurrent edits. Copy only the three standalone product files, author D07, make the five bounded caller completions and exact gates, statically include every added file, append the Bible evidence and a current source-only F16 entry. Update checklist/completion log with actual files/include lines and no installed/browser claims. Ordinary repository source regressions can be ported if root's authorized source-gate workflow requires them; this subagent ran no tests and created no fixtures/harness.

A successful static port does not prove login, real modal API joining, production-shaped 52-section history, installed SQL, performance, provider/payment/CSV/remittance or all later screens. Root performs only checks authorized by start sheet; later connected app testing remains later. No deployment or activation is inferred.
