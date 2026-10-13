# FC-03 Office and typed-contract source review

11 October 2026. **SOURCE PASS — Office interaction and typed contracts, with fresh closure below.** Independent read-only review of the shared component, its Office adapter and matching Worker contract against BD113, the approved card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1060–1082`, all FC-03 checklist rows and the supplied SCR-116 context. No product edits, tests, browser work, fixtures, SQL/business calls or hosted operations were performed. A4 screenshots/policy approval are separate and remain pending.

## Initial findings — superseded by fresh closure

1. **P1 — recover the accepted Case before offering another Apply.** `js/bpay-next-case-preview.js:44` starts `accepted=null` on every mount. `js/bpay-next-office.js:1048` records the accepted `case_id` and Candidate in the existing journal, but `mountCasePreviewFooter` does not restore that state. Closing/reopening after acceptance but before successful CASE_COMPONENTS readback therefore presents a new Preview/Apply and generates another command instead of the exact accepted Case's Refresh. Recover a bounded accepted CASE_CREATE identity in the existing journal, keep it until the targeted read/callback succeeds, and deliberately acknowledge that local completion before allowing a future independent creation. The correction needs no new store, list or automatic mutation retry.
2. **P2 — qualify a retained retry to this component.** At `js/bpay-next-case-preview.js:59,101–104`, any `snapshot().pending` changes Apply into Try again and calls `retry()`. `mountCasePreviewFooter` accepts a supplied controller as well as its Candidate-qualified default controller. It currently does not require the retained pending action to be CASE_CREATE or its closed input Candidate to equal this footer's Candidate. Check those exact existing bytes and identities before enabling retry; never convert them to a fresh request. An unrelated retained action must be refused, not dispatched by this footer.
3. **P2 — validate native consequence echoes before enabling Apply.** Both `broker/src/bpay-next-case-contract.js:30–38` and the identical FE contract validate output types, token/input equality and first/last due/week distance, but accept arbitrary positive `total` and arbitrary nullable threshold/floor outputs. They also accept differing explicit supplied weekly/weeks. Compare returned total with the entered principal, threshold and override with their entered values, effective floor with the override or token's actual Candidate default, and explicit weekly/weeks with their supplied values. These are equality checks of native facts, not a browser schedule/money calculation. A malformed successful response must not display a different consequence while enabling Apply.

The modified Office/CSS cache references should also be advanced before final delivery; the initial reviewed index diff only adds the new component script. This is a publication check for the changed assets, not a new UI feature.

## Preserved boundaries observed

The new component is reusable and mounted through the existing native host/footer contract. It renders server amounts and dates, keeps Apply disabled before a matching successful Preview, clears the shown Preview on changed input, prevents double submit, and checks `current()` after awaits. Office's adapter calls the original actor/owner guard and the transport repeats that guard before dispatch and after network/body reads. Destruction removes handlers and suppresses late effects.

CASE_CREATE uses the original controller's actor-qualified journal and stores the exact request bytes before dispatch. An unknown outcome retains those bytes; explicit Try again invokes the original controller retry without a new command ID or fresh Preview. A confirmed first refusal clears the pending request, whereas a refusal during an uncertain retry does not erase its identity. The newly added action uses the financial-command route and existing ENROLL wake; Preview uses the read-only preparation-page route. The 32 KiB exception is action-specific, and the other actions retain their original 2048-byte bound.

Request schemas contain exactly the entered fields, native command ID and required token. The BE contract and FE component's contract text match in the reviewed scalar/type/token rules. Amounts and BIGINT operands remain strings; dates are real four-digit dates, nullable Candidate floor remains null, and the response envelope requires one complete Preview row with no cursor. The CASE_CREATE receipt is closed, uses ACCEPTED_PENDING_CASE, and binds Case ID to the original command ID; it is not falsely called complete.

Readback is only CASE_COMPONENTS for the returned exact Case/Candidate, limit 100. Office validates every native returned row's Candidate/Case identity and component ordering. Empty readback remains pending with explicit Refresh; read failure does not undo acceptance, generate a new command, reload Banking Pay or fetch unrelated Cases. The component's current-instance check also fences its `onApplied` callback.

The batch adds the shared panel/footer and its script loading only. It does not expose FC-04 fields, a Case list, future edit commands, a new calculator or a new reader. Visual geometry, targets and the explained omission of surrounding future forms/list will be judged on actual A4 captures rather than inferred from this source review.

## Initial reviewed bytes

| File | SHA-256 |
| --- | --- |
| FE `js/bpay-next-case-preview.js` | `dd6b9c3824d2361661879a5c354fbdcb741f579a44552f710bdb6c69dbf6c0c8` |
| FE `js/bpay-next-office.js` | `0a862df226df825f1a567b7cc0a502119adae94a0debb8613696d53c2f77ebd5` |
| FE `css/bpay-next-office.css` | `ed49406d20bca4809d7356671e2c62d5713c0e1bd7032fe21ee61fa4c1b12b0f` |
| FE `index.html` | `2abcaa01e28f51066fe1a25c1e2613a79dea031fc40c777641bdce08085088b0` |
| BE `broker/src/bpay-next-case-contract.js` | `f93e3ead14d7acab3ff1d30e3e6711ac542840f09aa2fcfb26d91835a8e29a92` |
| BE `broker/src/bpay-next-command.js` | `da695705237a5ef769d128f909cde3017fff942f860b70ecf47a59056e5c7692` |
| BE `broker/src/bpay-next-preparation.js` | `4ad1d28afae17e046d6e676edf0115731f677beab37b9da65d4f65445ac87271` |

The hashes identify the initial findings snapshot, not a final closure. Root remains the sole product writer; fixes require a fresh source read and an appended verdict.

## Fresh interaction and contract closure

The subsequent saved source was independently re-read on 11 October 2026. **SOURCE PASS for the Office interaction/typed-contract boundary.** Findings 1–3 above are superseded by the corrections below; they are retained as history rather than current failures. A4 remains pending and no runtime journey was executed by this reviewer.

- `mountCasePreviewFooter` now restores the exact accepted Case from the existing journal and refuses an unrelated run/worker or Candidate scope. The reusable component starts with that accepted identity and exposes Refresh instead of a new mutation. A failed or empty targeted read preserves it. `finishCaseCreation` clears only that exact Case after the owning targeted callback succeeds, allowing a later independent form to create another Case without a new store or automatic retry.
- A retained pending request must have both its journal action and its original request action equal CASE_CREATE, with its exact Candidate matching this footer. Original request validation already belongs to the existing closed journal parser. Retry continues to use those bytes; no input or token is regenerated.
- BE and FE validators now compare all required consequence echoes: principal total, threshold, nullable override/effective floor, supplied weekly amount and supplied week count. The penny conversion is equality-only and computes no schedule or affordability. The native first/last Monday relation remains checked. The shared contracts match.
- Existing Office and CSS cache URLs now use `20261011-fc03-r1`, and the new component script loads before Office. CASE_PREVIEW's Worker argument handling preserves its original input object rather than normalizing arbitrary reason/note text; the token therefore describes the same entered input.

One presentation condition was reported before screenshots: the new preview card currently declares radius 16, while the supplied policy fixes card radius 13. Root should correct that geometry and retain native modal scoping before final A4 capture. This does not reopen the interaction/contract findings or authorize a visual PASS.

| Fresh file | SHA-256 |
| --- | --- |
| FE `js/bpay-next-case-preview.js` | `860f40962c17a8f7c857a44df85bd9aba5efe84bfed73aa642f4c579a1663864` |
| FE `js/bpay-next-office.js` | `cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745` |
| FE `css/bpay-next-office.css` | `ed49406d20bca4809d7356671e2c62d5713c0e1bd7032fe21ee61fa4c1b12b0f` |
| FE `index.html` | `e2c4a8003c1f45d2b9b575568a452aea83e80f9a651645c968a6df1cfdfedc6c` |
| BE `broker/src/bpay-next-case-contract.js` | `91d2bc7499d1b786ef743e5ae1a9997446d90d4fe1fe84647e84d0892b3fc135` |
| BE `broker/src/bpay-next-command.js` | `da695705237a5ef769d128f909cde3017fff942f860b70ecf47a59056e5c7692` |
| BE `broker/src/bpay-next-preparation.js` | `10ae24bc8b65f95a7821c55d014768ad63ef79faf676c11a831bf935a3bb357e` |

## Final A4 presentation/source amendment

11 October 2026: **SOURCE PASS retained; A4 PASS recorded in `docs/bpay-next/screen-designs/SCR-116/review.md`.** Current presentation source and all 21 final native PNG variants were independently re-read/viewed after final capture loop 71817. Radius 13/footer gap 8 are present. The exact native-modal-prefixed phone rule produces visible 44px Close/footer targets. Error messages now use one sentence with a semicolon, and a recovered unresolved CASE_CREATE starts with “Could not confirm the result; try again.” rather than “Preview first.” The prior presentation radius/target/copy conditions are closed. Journal, exact retained retry, targeted accepted readback, typed consequence equality and current actor/context fences retain the earlier interaction PASS.

| Final FE file | SHA-256 |
| --- | --- |
| js/bpay-next-case-preview.js | d7551956a993482a33264f39789cf2ce4d83342475dbabb53d7fff54ef4788e8 |
| js/bpay-next-office.js | cbdb844f11509a334cab8e87fc0cdc247b6e73ebe3f741ac8cb5be789e700745 |
| css/bpay-next-office.css | 043e1f6594a31c80f872f4107c362c18c47f74265a36f0f6a79c083bfcc537bc |
| index.html | e2c4a8003c1f45d2b9b575568a452aea83e80f9a651645c968a6df1cfdfedc6c |

This is source/pixel review only; no reviewer business calls, runtime tests, fixtures, database or hosted actions. Supplied pack remains unchanged, and no future FC-04 form/list capability is claimed.
