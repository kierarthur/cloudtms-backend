# EX-08 Office source review

Independent static review, 11 October 2026. Product was read only. No browser, rendering, tests, SQL, database, hosted or business actions were performed. This receipt covers the new controller, the exact Candidate caller/mount changes, native window lifecycle, asset wiring and scoped CSS; A4 visual approval is separate.

Authority: full EX-08 card `04_EXECUTION_AND_OUTCOMES.md:318–342`, all owning checklist rows D01/D02/D03/D04/D05/D07/D09/D10/D11 (`IMPLEMENTATION_CHECKLIST.md:2843–2851`), `design/EX-08-implementation-boundary.md` BD114, and `design/EX-08-office-seams.md`. Paths below are relative to BUILD-FE unless explicitly marked BUILD-BE.

## Current finding

**P2 — retained recovery can display a different target from the immutable request.** `js/bpay-next-pay-method-change.js:53–70,94,120–128` loads the exact actor/Candidate/endpoint-qualified retained request, but the chooser is still constructed from the newly supplied `targetMethod`/`umbrellaId`. `paint()` only disables it when `stored` exists. For example, an unknown UMBRELLA/U1 change reopened after choosing U2 shows disabled U2 while Try again sends the preserved U1 bytes. A retained PAYE operation reopened through an Umbrella choice has the same mismatch. The server dispatch and accepted native target are correct; the visible recovery intent is misleading. Smallest fix: bind recovery display to `stored.request.new_method` and its original target Umbrella identity/display facts, or suppress the fresh chooser and show the original retained target. Keep the original bytes and operation unchanged.

## Verified source boundaries

- `js/main.js:280892–281125,289200–289229` qualifies native ownership, handles PAYE→UMBRELLA, UMBRELLA→PAYE and UMBRELLA/U1→UMBRELLA/U2 through the existing entry, and keeps the original LEGACY route. NEXT destination patches contain only the native company ID or an empty PAYE patch. Accepted Candidate method/company come from the validated receipt, including an earlier retained operation rather than the latest form choice.
- `js/main.js:285399–285443` records deliberate PAYE bank edits only from trusted input events on the current owned mount. NEXT removes staged/prefilled bank fields and reapplies that separately owned intent at `281117–281120`; original programmatic Umbrella prefill/blanking does not manufacture a bank edit. Ordinary Candidate PATCH remains separate from pay-channel Apply.
- `js/bpay-next-pay-method-change.js:71–91,124–134` retains endpoint-qualified original serialized bytes, validates their parsed request equality, persists uncertainty before dispatch, and never deletes a prior unknown identity on a later refusal or owner-epoch mismatch. A first known refusal can clear only the exact matching operation with `unknown_outcome=false`. Actor, Candidate and current owner admission still apply before every dispatch and late effect.
- The bounded fetch uses the original endpoint, exact retained POST bytes, 6/10-second read/write deadlines, abort/reader cancellation and a 64 KiB closed JSON response. The `retainedBytes` parameter closes the former temporal-dead-zone defect. Refusals require the closed four-field shape and exact POST operation identity; success requires the closed native receipt.
- Closed Preview context, typed native rows, category counts, 20-row page limit, 1,000-member bound and exact continuation context are mirrored from the Worker contract. Continuation is keyset scrolling, not a new navigation protocol. The selected target invalidates fresh Preview; serial/busy/current checks and dismissal cancellation prevent stale rows or accepted effects from reaching another actor/parent. Native amounts are formatted without browser money calculation.
- The existing standard-window adapter owns header Close, focus, stack identity and dismissal. Result/header-detail live-copy markers retain later asynchronous text through the native moderniser. CSS is scoped to the native modern modal; card radius is now 13px. Index order places the controller after its existing UI dependency and before Office. Screenshot/pixel obligations remain pending independently.

## Earlier findings closed in these bytes

Endpoint qualification/exact-byte persistence, old-epoch retained readability, uncertainty before dispatch, prior-unknown preservation, matching refusal identity/unknown flag, the POST parameter TDZ, live-copy markers, card radius and one-sentence stale Preview copy have been corrected. They are not outstanding defects. No product changes were made by this reviewer.

## Reviewed source SHA-256

| BUILD-FE file | SHA-256 |
| --- | --- |
| `js/bpay-next-pay-method-change.js` | `c3cb2c8befe9cf27aa8f558227e3cec4579736b5ebf518487ef1fe2933d83d50` |
| `js/main.js` | `cafc1d0e48b4ee479ebdf476193417f55cb1059cc1920bf569993d01ab972bc1` |
| `css/bpay-next-office.css` | `d5dc0e83efdbb35c70912607f26386b3378d6ba7eb6e8da259a2f86f3331c470` |
| `index.html` | `4b919e4cc1e55eb77f56a1860239ce5cbd24c5f49507b9be8d9999b452b99e7c` |

Verdict: one concrete recovery-display defect remains; corrected source requires a fresh narrow reread. This is not a runtime or visual PASS and does not close the item checklist.

## Final source closure — retained target display

The above P2 finding is **closed and superseded** in the current saved source. `js/bpay-next-pay-method-change.js:94–96` hides the caller's fresh company chooser whenever any retained request exists and shows the original request's source/target methods. `125` saves only the already validated reader's native Candidate name/reference/target company as `preview_display`; recovery uses that closed optional display shape. An older journal without these optional display facts remains readable and shows original methods without guessing a company. The POST still sends `stored.bytes`, the matching original operation and original context; no accepted or uncertain identity changes with this display correction.

Fresh source SHA-256: controller `a51facf62fba66d01e893444c666c294ad09af50f2b9a91a1b467668ab0c4932`; main `cafc1d0e48b4ee479ebdf476193417f55cb1059cc1920bf569993d01ab972bc1`; CSS `d5dc0e83efdbb35c70912607f26386b3378d6ba7eb6e8da259a2f86f3331c470`; index `4b919e4cc1e55eb77f56a1860239ce5cbd24c5f49507b9be8d9999b452b99e7c`.

**Final SOURCE PASS:** no outstanding essential source defect found within this Office/caller review. The existing Candidate update route already requires admin (`BUILD-BE broker/src/index.js:149654`), matching the native owner client's current actor admission. This conclusion is static source review only; root's verification and A4 screen review remain separate, and this receipt does not mark visual/runtime delivery or checklist completion.

## Final caller ownership fence amendment

Fresh reread confirms `js/main.js:289208` compares any caller-supplied native `next_owner` with the fresh owner's exact `active_owner` and `owner_epoch`, after the existing parent/actor check and before either LEGACY fallback or NEXT window opening. A resumed Candidate save therefore cannot switch owner branches between its first qualification and the modal's fresh qualification. The original catch returns a handled refusal; it cannot fall through into LEGACY. **SOURCE PASS retained.**

Final reviewed hashes: main `35073a4ed6ca21d8dbc7bdf40d6970f175656f258567282a57eebbd106bfc243`; controller `a51facf62fba66d01e893444c666c294ad09af50f2b9a91a1b467668ab0c4932`; CSS `d5dc0e83efdbb35c70912607f26386b3378d6ba7eb6e8da259a2f86f3331c470`; index `4b919e4cc1e55eb77f56a1860239ce5cbd24c5f49507b9be8d9999b452b99e7c`. No execution or product edits by this reviewer.

## A4 presentation-driven source amendment

Fresh reread of the saved controller/CSS confirms protected Work rows omit their technical subline (`104–105`), error results use `role=alert`, and failed reads no longer add a misleading Choose company skeleton (`98,120`). Accepted state uses native success text and hides the Apply/Try again control (`94,97`), with no added footer Close. Original native header Close still reaches the same `onDismiss` acceptance path (`133–136`), which returns the validated original accepted method/company only while actor/owner/parent context remains current. Retained unknown requests still have their single visible Try again control and hidden fresh chooser. The exact-byte journal, request/receipt schemas, uncertainty, refusal and ownership fences are unchanged.

CSS `661` honors the component's hidden chooser/footer buttons rather than its flex styles overriding them; `637–641` allow the existing native header to wrap its owned subtitle; `666–667` provide 44px phone chooser/header/footer controls. These are source-level confirmations; actual geometry and A4 approval belong to the independent screenshot review. **SOURCE PASS retained, no new essential source finding.**

Current hashes supersede earlier snapshots: controller `7efc8117d675ceefbd3d48805678574b0ee985ceea39e3f9f22e99d83bbea4db`; CSS `f9455e83d6ef512f9ecca5ee9faec8657904ebfca4a51c55c23da4cec9ac9802`; main `35073a4ed6ca21d8dbc7bdf40d6970f175656f258567282a57eebbd106bfc243`; index `4b919e4cc1e55eb77f56a1860239ce5cbd24c5f49507b9be8d9999b452b99e7c`.

## Final P74 source supplement

Current saved bytes retain **SOURCE PASS**. Accepted reopen paints the validated retained success even with no live Preview, hides the fresh picker and Apply/Try again, and uses only the existing native header Close to return its accepted result. Pending reopen still offers the exact original request. Protected Work technical detail stays omitted; failed read/result branches use alert and do not show the chooser skeleton. Loading skeleton/continuation now have status roles (`67,98`), and the continuation selector is scoped to `#modal.ctms-modern-modal` (`CSS 663`).

The final geometry source uses the explicitly corrected outer card radius 16px, inner table/top sticky-header corners 13px, native modern-modal header selectors, and exact 44px phone controls (`CSS 637–652,666–667`). This supersedes earlier radius snapshots. Table headers are sticky with a painted native background and explicit rounded first/last corners; phone money keeps right alignment/tabular numerals. Negative amounts now display `−£…` by moving the existing string sign before the currency symbol (`controller 68`), with no monetary arithmetic or changed server value. Actor/owner/parent, exact bytes, scope/cursor, refusal and receipt logic remain unchanged.

Final reviewed SHA-256: controller `4bf56a3f87aa85d5c9ac762923dbb20639549fce49a202d4afd9a38ee9fbba99`; CSS `63144a3f4f13b617dca0d057126def00bc402573c5cbebb1941c99c8041b804c`; main `35073a4ed6ca21d8dbc7bdf40d6970f175656f258567282a57eebbd106bfc243`; index `4b919e4cc1e55eb77f56a1860239ce5cbd24c5f49507b9be8d9999b452b99e7c`. No new essential source defect found. This supplement is read-only source review; the independent screen reviewer owns the final 27-image A4 verdict.
