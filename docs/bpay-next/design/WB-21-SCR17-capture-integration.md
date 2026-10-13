# WB-21 actual SCR17 capture integration

Read-only helper inspection, 10 October2026. No helper, product, screen spec or generator edits; no capture, render, browser, imports, tests, SQL, business/data/hosted/network operations. This is the smallest proposed integration for the implementation owner, not a screenshot receipt.

The existing ignored **BE `.codex-local/WB12-screenshots.cjs`** is the closest starting point. No FE `.codex-local` directory currently exists. WB12 already loads the actual saved frontend `index.html`, opens `CloudTMSBpayNextOffice` through the real `createStandardWindowAdapter` (`:32`), supplies closed fixed capability/workbench reads, and waits for a coherent actual Ready list (`:35–36`). Its one origin route (`:16`) serves only saved FE files beneath the verified root and aborts every external origin/API path. Use that same boundary. WB14's helper provides the same actual Office/adapter setup but adds unrelated issue samples and imports the source row map; those additions are unnecessary for SCR17.

The current saved `docs/bpay-next/screen-designs/SCR-04/fixed-read-responses.json` supplies the real2 eligible Ready Candidates, separate deductions-only disabled row, current closed headers and capability receipt. Reuse that exact saved response structure, without regenerating the old screen samples or weakening current validators. A new capture destination can contain a copy of the fixed response sample actually used. Do not write the supplied screen pack's specs/generator or pretend a manually authored confirmation is actual Office output.

Smallest actual interaction after the existing coherent Ready wait:

```javascript
await page.getByRole('checkbox', {name:'Select Amelia Hughes', exact:true}).uncheck();
await page.locator('#btnCloseModal').click();
await page.getByRole('heading', {name:'Discard your unsaved selection?', exact:true}).waitFor();
// Capture the actual current #modal/content/footer using the existing screenshot call.
```

The first action changes only the real local choice store and labels. The second enters the actual main token-bound pre-close hook and shows Office's actual small Keep/Discard child. No Create, mutation response, run, financial command, SQL or POST is required. Retain the helper's dark1600×1000, device-light1600×1000 and phone-dark390×844 modes and save SCR17 captures under the normal BE product-evidence directory. Capture after the real heading/footer is mounted, not by injecting arbitrary dialog HTML or replacing `beforeClose`.

For the genuine optional-storage presentation, a separate local page can pass a small `choiceStorage` whose `getItem` returns null and `setItem` throws. After the real untick, the store reports false and the actual prompt must read `Keep leaves your ticks in this tab.` This uses the already-existing optional dependency seam and does not require a browser settings change or storage framework. Keep/Discard, paused-close and actor-change behavior should be assessed through their actual controls by the owner; this note does not claim those interactions executed.

After capturing, a Keep click can close through the actual child, then parent. A same-page reopen must reuse the original real Office options/actor/scope/date to observe retained intent. Expose/reuse only the one existing fixed-response `open` options inside that one-off capture if needed; do not reconstruct the dialog or fabricate a submitted journal. A Discard observation must remain confined to local unsubmitted intent. Existing accepted/uncertain controller journals are separate and cannot be cleared to set up a screen.

Inspected hashes: WB12 helper **`be9c2c0a850ae435081e400dd3209b48cfa1a114b337e67db6fc6754b3199bb6`**; WB14 helper **`400e04dc5645f182f78a677c224e80e6f0fa5b6e7c4b06f2843fc59f5580b497`**; saved SCR04 responses **`a0615c95e3cbfc268c1da77cc832b6f5ea312256e74822e128caa2f6cdb40d19`**. The actual source review hashes and CL01/CL02 closures are in `reviews/WB-21-close-store-review.md`. Actual images/policy results/user-eyes acceptance remain NOT_RUN by this reviewer.
