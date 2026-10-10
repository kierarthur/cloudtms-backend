# SCR-15 actual export confirmation and result A4 review

Independent review, 10 October 2026. Read the full WB-18 card/checklist, supplied SCR-15/SCR-15b designs, SCREEN_DESIGN_POLICY and its complete 42-point checklist, and manually applied `tools/policy.json` wording rules. Individually viewed all twelve actual saved list-confirmation, Candidate-confirmation, failure and completion PNGs across desktop dark, device-light and 390px phone. Re-viewed all three completion PNGs after the final phone-target correction; the other nine final images are byte-identical to those individually viewed. Inspected current export/controller/frame source and the Candidate context correction.

The builder's captures use the actual local Office entry and standard modal adapter with minimal fixed responses and hosted network blocked. This reviewer read source and viewed saved images only: no browser, capture, pack tool, generator, wording tool, test, runtime, SQL, fixture, hosted request or export was executed.

**Final A4 verdict for the built export UI: 33 PASS / 0 FAIL / 9 N/A.** The applicable source/pixel and user-eyes checks pass. This is approval of the built SCR-15/SCR-15b UI, not evidence that the 1,000-row/three-unticked/Included-total/Candidate-only business journey ran. Server financial authority, exact CSV column/amount behavior and later joins retain their separate reviews and acceptance status.

## Findings and finite corrections

**SC15-01 — CLOSED: completed phone Close target.** The first completion phone image had a roughly 36px header Close. The final export-only phone selector `#modal.ctms-kind-bpay-next-read:has(.bpay-next-export) #btnCloseModal` supplies 44px minimum width/height without `!important` or native-screen rewiring. Fresh completion phone pixels show the 44px target. Confirmation/failure footer Cancel and Export/Try again already meet 44px. Completed state uses header Close only.

**Candidate namespace correction — verified.** The Candidate export first requires `sameWorkbenchRevision(address.context, context)` across actual agency, actor, owner epoch, official date and Workbench/adoption revisions. It then freezes using the real retained parent `address.context`, whose scope matches the unchanged choice-store namespace, and passes the exact child `candidate_id` separately. The store is not retagged and financial/current inputs are not refreshed. If the child is newer than that retained context, export is refused until the real parent is refreshed. The actual Candidate-confirmation capture demonstrates that the available real path opens.

**Earlier source findings EW01–EW04 remain CLOSED.** Each attempt has its own controller/timer; abort cancels its body reader and late unclaimed bodies; success hides footer controls; the main header has Filters and Export only, with real Refresh outside the strip. Candidate retains Refresh and Export. See `reviews/WB-18-export-ui-review.md` for scoped request/lifetime evidence.

## All 42 policy and user-eyes checks

| # | Result | Evidence and scope |
|---|---|---|
| 1 | PASS | Real whole-list and exact-Candidate Export controls reach this confirmation. Explicit Export dispatches the real bounded transport; Cancel, failure Try again and completed Close are present. |
| 2 | PASS | The existing list export capability and Included intent are preserved; no bank details or browser amount calculator are added. Exact CSV data behavior remains separately reviewed. |
| 3 | PASS | Export CSV, Included column, explicit no-file failure and manual Try again match the card/design purpose. Unsupported sample counts are omitted under BD074. |
| 4 | PASS | Read-only export freezes the existing complete choice/context descriptor; it does not change ticks, create a Draft, dispatch a financial command or refresh accepted inputs. |
| 5 | PASS | Real standard modal frame. Confirmation/failure follow SDP-24's headerless two-button exception; completion restores the normal title/header Close and hides footer buttons. |
| 6 | PASS | No extra tab row or navigation tier is introduced by this child. Parent controller and tabs are retained behind it. |
| 7 | PASS | One compact question/consequence/scope/result body and standard footer; no nested card, copied overlay or speculative summary. |
| 8 | N/A | This confirmation/result has no data table. Existing parent lists retain their separate approved table/card implementations. |
| 9 | N/A | No payment-stage badge is added to the export dialog. Failure uses the existing danger token. |
| 10 | PASS | One primary Export CSV or Try again, safe Cancel first. Completion has no repeated footer action and only header Close. |
| 11 | PASS | Existing standard adapter/footer and token styling are reused. The finite export-only phone target rule adds no `!important` or new component framework. |
| 12 | N/A | The dialog has no list or paging UI. Bounded server export enumeration is separately owned; no browser all-row list is substituted. |
| 13 | N/A | No data-column headings in this child. The request retains the actual parent's current server sort. |
| 14 | N/A | No new tick control in this confirmation. Existing complete local intent is frozen; no loaded-row selection counter is invented. |
| 15 | PASS | No fabricated export counts or totals are shown. The supplied design's sample count is deliberately omitted because no exact whole-export count reader supplies it. |
| 16 | PASS | Manual `policy.json` check: copy is plain, messages are at most two sentences and each sentence at most 20 words. No internal codes/IDs/lease/protocol terms, chatter or loan wording. No wording tool executed. |
| 17 | PASS | Failure is inline and explicit: “The export could not be completed; no file was produced.” The adjacent manual Try again is the available next action. |
| 18 | PASS | Clear list/Candidate question, consequence, captured channel scope, Cancel first and Export CSV second. The action is a read-only export. |
| 19 | PASS | Source defines Preparing CSV, definite complete and failure; no missing/failed read is replaced with £0.00, false count or empty successful file. Current failure and completion pixels are clear. |
| 20 | PASS | Visible copy contains user-facing CSV/channel terms and no UUID, technical error code, bank detail or loan label. |
| 21 | PASS | No numeric/date field is fabricated or reformatted in this dialog. Captured channel text is factual; CSV bytes remain server-owned. |
| 22 | PASS | Real buttons and standard focus/keyboard adapter are retained; safe Cancel is the initial confirmation action. Status/alert roles and aria-busy are set. Runtime keyboard journey NOT RUN. |
| 23 | PASS | All four phone states fit 390px without sideways clipping. Footer controls and the final completed Close meet 44px; the next action is reachable. |
| 24 | PASS | Each of the four desktop dark/device-light pairs is byte-identical. App styling remains dark. |
| 25 | N/A | Owner A4 requires actual local product captures with minimal fixed responses; pack regeneration/spec tools were not run. Supplied pack remains read-only. |
| 26 | PASS | Question derives from exact Candidate narrowing, scope label from captured `context.scope.channel`, and result from the actual request/download path. No sample count or financial value is invented; readerAdditions N/A. |
| 27 | PASS | No official pay date appears before execution. The request retains its internal context date without displaying it. |
| 28 | PASS | List/Candidate, CSV, Included, Cancel, Export and Try again are understandable payroll terms. The consequence says what the file contains. |
| 29 | PASS | Export is beside the actual list/Candidate scope in source; its confirmation places Export beside Cancel and failure action beside the failure. |
| 30 | PASS | Export CSV is the obvious next step; on failure Try again is explicit, and on completion the visible header Close exits. |
| 31 | PASS | This task asks which scope will be exported, not which money will be executed. The exact list/Candidate question and channel answer that question without pretending to be a payment review. |
| 32 | N/A | No Waiting/Blocked/Action-required payment rows or new remedies are presented in this dialog. |
| 33 | PASS | The current child visibly changes to an explicit no-file failure or “CSV downloaded.”; no toast or result hidden in a dismissed window. |
| 34 | PASS | Only question, consequence, factual scope and outcome are shown. No unsupported count, bank data, selection estimate or future action stub. |
| 35 | PASS | Export CSV, Candidate, PAYE, Umbrella, Included and Try again are consistent with the owning card and actual parent surfaces. |
| 36 | PASS | Read-only export cannot change money or membership; safe Cancel precedes the primary action. Malformed/stale/late/incomplete responses cannot produce a downloadable partial file. |
| 37 | PASS | Cancel aborts the active attempt and dismisses the real child; completed header Close/Escape returns to the same parent. Source retains actor/frame checks and cleanup. |
| 38 | PASS | One adjacent Export control and one confirmation action; no extra selector, viewer or automatic retry detour. |
| 39 | N/A | This dialog displays no amount/count arithmetic. CSV Included-total/headline equality belongs to separate server/application acceptance and is not inferred from these PNGs. |
| 40 | N/A | This is export, not Create Draft. Whole selected-payment confidence retains its actual later owner/join. |
| 41 | PASS | Failure, completion, phone and dark/light evidence pass; source bounds request/response/time, checks actor/frame and handles cancellation/late streams. Large-data/slow/empty business execution and keyboard execution NOT RUN. |
| 42 | PASS | List/Candidate question and captured channel remain visible with explicit outcome; completed header says Export CSV. Parent choices/window are preserved on return by the real adapter. |

## Evidence identities

SHA-256 of the final reviewed captures/source:

| File | SHA-256 |
|---|---|
| list-confirm-dark.png | `06de81c86fa235864c14969fd9888e3623fc4c35b696cddd1297c349e420f2f1` |
| list-confirm-device-light.png | `06de81c86fa235864c14969fd9888e3623fc4c35b696cddd1297c349e420f2f1` |
| list-confirm-phone-dark.png | `119bb905e746d1b6503d4d8afabd969b4311f6208ff95c772f569cdc72c906e6` |
| candidate-confirm-dark.png | `680769dccac46401b13dcd3ee7642392d574a9f358fda476c800d94b38155dcb` |
| candidate-confirm-device-light.png | `680769dccac46401b13dcd3ee7642392d574a9f358fda476c800d94b38155dcb` |
| candidate-confirm-phone-dark.png | `ccde1f4ab9f53ff7f2c41827f85112a852f14a9cec5a64a449c376e17101696b` |
| failed-dark.png | `b29581e7d18c1a3c517cf971ba32887f14aeeaedbdb84051311f11728ae35528` |
| failed-device-light.png | `b29581e7d18c1a3c517cf971ba32887f14aeeaedbdb84051311f11728ae35528` |
| failed-phone-dark.png | `7ff1579fc122ca6e623df7692169b39b0e55867534973dca983009346bf0fd22` |
| complete-dark.png | `123fdbe419a85cf7dfb1d89b4e71e741b68b4f310b53c3fcf77149bc7b9b0aa8` |
| complete-device-light.png | `123fdbe419a85cf7dfb1d89b4e71e741b68b4f310b53c3fcf77149bc7b9b0aa8` |
| complete-phone-dark.png | `a7d0e8559a6ee6c71aa5e67da59d10ba6f8880d9bc1295de3132fcbacad4cdb5` |
| fixed-read-responses.json | `71bdbc4cf70518c605f1332d84b9aaf8e45ef6d651a99f3ef92976706698553b` |
| BUILD-FE js/bpay-next-workbench.js | `d03d7da007c80602449795d9c4c34111c20909f040c2446bcad1b722bda4b684` |
| BUILD-FE js/bpay-next-office.js | `62b5df74a6307cccb5eb8e47dee9b89648ff913030dc30772e26809139a40a82` |
| BUILD-FE js/bpay-next-ui.js | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
| BUILD-FE css/bpay-next-office.css | `5f9101ab09eb4ede8997f853cf8cd555a1217dcf83914f7fd06819503d2b5651` |
| BUILD-FE css/modal-modernisation.css | `a4003bf69088f282c7883ab394a5b679826e8937707da0593456a5045b8e3606` |
| BUILD-FE index.html | `0473c69cdcabf691f1b71ef1255f806886b8205391ad6dcaec0b7f62ba65ab52` |
| supplied SCREEN_DESIGN_POLICY.md | `8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5` |
| supplied tools/policy.json | `45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d` |
| supplied specs/SCR-15_export_csv.json | `3bb50503208f9afcaf7ce5bb721baca3f48d75cf447191538fe50c90c531ae9b` |
| supplied specs/SCR-15b_export_failed.json | `3437b8b9526a4bb0845153bfebf520f240a50941ed690fe5b66b5259f19833c3` |

All approval is bounded to these saved bytes. No hosted/data/performance acceptance or pack regeneration is claimed.
