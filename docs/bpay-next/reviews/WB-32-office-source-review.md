# WB-32 Office source review

13 October2026. Independent read-only source review against full WB32 card1193–1217, all current checklist rows (only D03 owns implementation), R2 W12–14/WB32 and BD173. Read the new native pay-channel module, closed matching read DTO, component-choice channel adapter and existing Workbench/native frame/controller seams. No product edits, business SQL/RPC, tests, fixtures, browser/screens or network actions. Root alone fixes product.

## Initial checkpoint — SOURCE OPEN

| Finding | Evidence / smallest correction |
| --- | --- |
| O32-01 — lost saved prefill | `reload` initializes a rate with `saved_quote?.quote.rate ?? suggestion`. When a saved owner exists but its quote is unavailable, this loses the actual saved manual operand or substitutes a suggestion as if it were Saved. Prefill its genuine saved manual rate, or its known saved suggested quote; keep unavailable absent rather than invent a replacement. Saved selection and suggestion must remain distinct. |
| O32-02 — unbounded / duplicate finance pages | `nextFinance` pushes each Case/whole Preview directly into a permanently growing `finance[]`, then advances the cursor only after the complete page. A failed later Case/context check leaves partial members; retrying the same cursor duplicates them. Retain a bounded native page window/history with exact failed cursor, stage a page before adoption and preserve the original Case/context identities; no global Candidate scan or new reader is needed. |
| O32-03 — ambiguous CLEAR consent | `clearOwners` confirmation contains weeks and bucket labels only. Actual incompatible saved owners with the same target/channel label can differ by version, policy date and manual operand, yet look identical. Disclose the chosen native owner's stored rate/amount/mode and saved target/effective date (plus known native amount where available); do not use the compatible pair or price a total in the browser. The final exact request remains the original owner IDs/versions. |

Findings were sent promptly. Further finite review is continuing while root fixes these; this checkpoint is not a final source/A4 verdict.

Additional finite findings sent promptly:

| Finding | Evidence / correction |
| --- | --- |
| O32-04 — native resolver Ready lost beside incompatible owners | SQL initially sets `resolver_ready` only on a no-owner metadata row, DTO prohibits it alongside an owner, and UI `owner()` chooses only compatible/null metadata. A real READY CHANNEL_RESOLVER row containing only incompatible owners therefore becomes falsely To decide/mandatory in this window. Preserve the actual native row's resolver-ready fact independently of owner presence and read that fact from the exact row metadata; no NEED/classifier producer changes. |
| O32-05 — stale error property | `WorkbenchReadError` exposes `reloadRequired` (existing workbench659–688), but the new `fault()` checks only reload/reload_required and three codes. Include that exact native stale flag so a known obsolete offer remains sticky and cannot be edited/resubmitted without explicit Refresh. |
| O32-06 — stale parent return focus | New frame dismissal checks owner/actor/connected previous element but not caller `options.current()`. Match the WB31 return-focus fence to avoid restoring focus into an obsolete same-actor parent. |
| O32-07 — native units truth | The new quantity column is universally Hours, including RATE_BEARING Additional with an arbitrary actual non-hour `unit_label`. Use truthful native quantity/unit wording for those rows; no quote or reader change is needed. |

The core reuse is appropriate: common write context comes from native W1, explicit Preview checks original basis, Position/current row contexts and expected owner before dispatch, original shared controller owns persistence/unknown readback/exact retry, deliberate zero uses explicit consent, and Work CLEAR refuses mixed owner families. Native Finance uses CASE_LIST/CASE_COMPONENTS/CASE_CHANNEL_SUGGEST and direct CaseChannel.open; no financial-clear fallback exists. Its current quote panel uses native amounts only. The new component adapter chooses one exact disclosed Work/line from the server channel envelope and reuses the original child/controller; it does not discover browser money or linked membership.

Initial inspected SHA-256: pay-channel module `297c95bbe4d4a2e76811326d1d56855675dfc506c74572f030bb68a35ebfeb3a`; FE closed helper `16e961e07de53db3c69e49240c591577b2fea7793e6d40a065e301ea79dcf581`; component adapter `dc5147ca04317a234a03f753b15f9cecb1773814078a41486f453b5f584f2e9a`; Workbench `b78f270e8af9a0ff1c86216f03b992e699b99206e9193bcf7d2da8333e6df1ee`; CSS `dcd8781106fbd631da28ae2d257c3638b7fae1912c436a7ad0958a6b2fffad21`; index `14b9c0b523781822cbd7f8e9c722e693d11cd9472ac13a3566fc7374f202c0ec`. Final hashes follow fresh finite fixes. No runtime, A4 or full financial-consumption acceptance is claimed.

## First finite repair review — SOURCE remains OPEN

O32-01 CLOSED on actual saved manual operand / genuine unavailable blank prefill. O32-03 CLOSED on exact chosen native saved mode/rate/amount/target/date disclosure. O32-04 is corrected by the dated0452 metadata successor (0417 remains immutable), DTO's independent native resolver-ready boolean and exact row fallback. O32-05/06 CLOSED on native reloadRequired sticky handling and caller-current return focus. O32-07 CLOSED on Units where actual quantity is not Hours and the exact Standard Day/Night/Sat/Sun/BH display order.

O32-08 CLOSED: manual reason eligibility now derives from the actual selectedWorks intents, so unrelated unchanged saved manual choices/reasons do not enter this request or impose a new reason on an all-suggested selection.

O32-02 is **partly closed**: staged pages prevent duplicate partial adoption, three retained five-row native Case pages plus32KiB cursor history bound storage, and only owned native panel fields are retained. Remaining required retry: an exact failed `{direction,requestedAfter}` and visible Try again for page/transport failure. At this snapshot the finance-only error tells staff to refresh while the whole-window Refresh is hidden unless global error/stale; mere scroll can rederive a request. Preserve exact failure identity and explicit read recovery through the existing reader.

Two new finite integration findings sent promptly:

| Finding | Current evidence / correction |
| --- | --- |
| O32-09 — redirected-window ownership | Rates reroute awaits `frame.close()` but ignores its boolean; Keep editing / close refusal must not still open Channel. Require confirmed close and current actor/owner/caller after that await. ComponentChoice reroute discards its returned Channel handle; delegate alive/close to that owned child so existing parent retirement still owns the redirected window. No new modal host/protocol. |
| O32-10 — finance comparison basis | Existing panel Remaining total uses `source_outstanding` (including reserved) while suggested total is quoted only from `context.source_basis` (unreserved). For outstanding160/reserved120 it presents160→quote40 without the basis distinction. Use comparable native unreserved basis, or explicitly label outstanding/unreserved and native reserved fact; never derive/subtract in the browser. All operands already belong to FC13. |

No financial-calculator, Candidate writer or server-clear fallback expansion is required by these corrections. A4 remains separate; latest source hashes will be recorded after these finite gaps close.

## Final finite source review — logic SOURCE PASS; cache packaging pending

Fresh saved-source review closes O32-01 through O32-10. This section supersedes the earlier OPEN checkpoints and the earlier instruction to expose an internal valuation date. The final CLEAR confirmation discloses the exact stored mode/rate/amount and saved target for each selected owner, while its unchanged original request carries that owner's exact ID/version and native fingerprint qualification; no internal valuation date is displayed.

O32-02 CLOSED: each five-row native Case page is staged before adoption; a failed page leaves the original cursor/window intact, with no duplicate partial page or false complete result. At most three pages and32KiB cursor history are retained, and the visible native Refresh covers both page errors and individual arrangement errors. Refresh is an explicit staff request for fresh authority, respects changed-rate discard consent and refuses replacement of a pending command. An additional exact-read retry protocol is not required here. O32-09 CLOSED: Rates checks the close result and current owner/actor/caller after the await; ComponentChoice retains and delegates ownership to its redirected Channel child. O32-10 CLOSED: the existing Finance remaining total is the actual native `context.source_basis`, comparable to the quoted basis without browser subtraction.

The latest0452 reader is genuinely per bucket: its bounded discovery retains exact Work/revision/line/family/code keys in `v_resolver_keys` only for actual native READY CHANNEL_RESOLVER rows without a current need or compatible saved owner. Metadata tests that exact key, rather than attaching a line-wide flag to unresolved buckets. Current positive AVAILABLE/unselected native projection, accepted-membership exclusions, exact current/applied revision, projection/FVR/metadata fences and actual source/target-or-company difference remain. Additional Source UUIDs are not inferred as rate keys; the positively affected unbound arm returns the existing typed unavailable refusal. The helper remains STABLE, SECURITY INVOKER and owner-only; it introduces no money, producer or public membership authority. Installed0417 is preserved.

The matching BE/FE closed validator blocks compare exactly after the existing export/module adaptation. Static text comparison also confirms the original FE command controller is unchanged from HEAD. The complete0417 W1 and W2 inverses remain their native parents after removing, respectively, the finite channel-review delegation and exact CLEAR result-class addition. No new browser pricing, Candidate pay-method mutation, finance CLEAR fallback, linked-closure framework or accepted/frozen selection change is introduced. Native finance discovery and previews use the existing admitted list/point/suggestion readers and direct FC13 opener; the absence of a NEXT FINANCE_CHANNEL_RESTRUCTURE clear owner stays truthful. Full financial-consumption acceptance and joined TEST flows remain NOT_RUN; A4 is separately owned.

One finite packaging condition remains at this saved snapshot: `index.html` still gives the changed Rates script `20261013-wb30-r1` and the changed Office stylesheet `20261013-wb31-r1`. Bump those two existing asset URLs for WB32 before packaging. This was sent immediately to root; no product file was edited by this reviewer.

Current reviewed SHA-256:

| File | SHA-256 |
| --- | --- |
| FE js/bpay-next-pay-channel.js | `d66701e2b7ad5b372bc994cc39bf351f58c076ca7ac6a0bd9339c3b50ede17de` |
| FE js/bpay-next-component-choice.js | `1ff5097d104f5bf827074f4fd93c4d03d4345206a4619ba453b8ebb87048ca1f` |
| FE js/bpay-next-rates.js | `2a244503bfd480338ba4792f6ba61c764d7bf7b290be4dc445b4089b5b3321ce` |
| FE js/bpay-next-resolution.js | `ddb03730f5e717b3ec0cc428821433e5c970f5a78900457e9b919c2435993919` |
| FE js/bpay-next-workbench.js | `c19e85817f57aec3aee0bcf2f9985df32e9d7ccd7c7d42a6c6ce772551711f57` |
| FE css/bpay-next-office.css | `56a575e573b86b549b01b661cdb0e5bbc2b9ae0581ae58c03e880c03792fa6a5` |
| FE index.html (before two URL corrections) | `14b9c0b523781822cbd7f8e9c722e693d11cd9472ac13a3566fc7374f202c0ec` |
| BE broker/src/bpay-next-resolution.js | `050304facba00435c85131935e8c28b4e709344bb046e811dd9f599a3279c6d8` |
| BE repeatable13102026_0417 channel_resolution_review_v1 | `041126bd6ed7bf4ace370829018f46d85b16b7b95ca56fdc44c4e994bd62d449` |
| BE repeatable13102026_0452 channel_resolver_ready_review_v1 | `bde7a01ce2a37208e90f79d4cfda812505ecc08e1a5f58c63f1a38705f67fc7b` |
| Current WB-32-build-boundary.md | `1008da235e2f4daf1c58cd7efe4881ff58f46a49ecf819c9b9313285a3bcd230` |

Read-only source review only. No product code, tests, business SQL/RPC, fixtures, browser/captures or deployment actions were performed.

## Packaging and journal amendment — SOURCE PASS, zero open source findings

Re-read the saved final delta. Rates, Office CSS and the native modernizer now use WB32 cache URLs. The modernizer addition is restricted to the existing bpay-next-read kind and the new Channel body/footer/confirmation secondary-button markers; no generic button or monetary formatting behavior changes. Stale Refresh deliberately removes its secondary marker so the native primary treatment remains correct; danger confirmation keeps its native destructive marker. Candidate confirmation subject, plain footer guidance and phone value wrappers use existing qualified reply values only.

The retained-journal bootstrap now checks both original request agency and Candidate against the current namespace before constructing the unchanged original controller. It also retains exact typed actor validation, byte bounds and original request bytes; it does not replace the saved scope with the current reader or dispatch a guessed request. The new guard closes the bootstrap cross-namespace seam.

This closes the two-URL packaging condition above. O32-01–10 remain CLOSED. **SOURCE PASS; zero open source findings at these saved bytes.** A4/captures and joined business acceptance remain separate and are not asserted by this receipt.

Updated SHA-256 (supersedes earlier hashes for these files):

| File | SHA-256 |
| --- | --- |
| FE js/bpay-next-pay-channel.js | `9a06e8ee596ca47e81ce81866ab926bcc64bb7eb748dd40266c0d38a334e610f` |
| FE js/modal-modernisation.js | `db1409c8c8496550c2271172edf8b7d58cf3995a5a516f65f2b21f51c28eabfb` |
| FE css/bpay-next-office.css | `b5b3b1b1595ffa610cb359b0dbcf08f3861f3bfd3b8c297acbba792b3be66e2d` |
| FE index.html | `8471c26c565192dfa6c2ffd88395c8273918f3779558ea75fbe214135d4d0c91` |

Other inspected source hashes in the previous final table remain unchanged.

## Full-card / final presenter amendment — SOURCE PASS retained

Full WB32 card1193–1217 and all seven current checklist rows have been independently reconciled in `WB-32-final-deliverable-review.md`. D03 is the only owning implementation row; D01/D02/D04–06 remain buildTask:false context. All Build/Rules controls have concrete source paths; every Done-means business/app journey remains AUTHORED_NOT_RUN. A4 remains independently owned.

Fresh presenter fixes reviewed: Candidate/client identity and truthful saved consequence in consent, neutral field-error footer beside original aria-qualified input error, actual finance loading/status and alert/live-copy before its facts, destructive exact-family clear controls, bounded unavailable placeholders, hide exact-zero linked opt-in, scoped empty-success footer and phone value grouping. None changes native amounts/quotes, selected ownership, original context, action/body/receipt/journal/retry predicates or Finance writer authority. SOURCE PASS retained; zero open source findings.

Updated final hashes: FE pay-channel `55c1bc58cab993cd67157b7d99532dbff69d47b890084f87cf05ff41ae8d1865`; FE component-choice `3a558fd0b7f8870d69395cf7c386b628367cd631926a38bacf8c3a9ee90b0dab`; current BD173 boundary `39b528c85abcfcd6c65cb6316162dd90e8f0eb719b9fa53478846881f264f662`. CSS `b5b3b1b1595ffa610cb359b0dbcf08f3861f3bfd3b8c297acbba792b3be66e2d`, index `8471c26c565192dfa6c2ffd88395c8273918f3779558ea75fbe214135d4d0c91` and all other last-final hashes remain current at this snapshot. Reviewer performed source/document reads only.

## Saved Ready Rates entry correction — SOURCE PASS

Independent fresh read of the narrow Rates/index delta closes the saved-Ready entry gap. Earlier same-Umbrella entry evidence based only on a redirect after bucket_review is superseded: native0201 is unresolved ACTION-only and can return STALE before that redirect. Rates now makes exactly one additional bounded READ through existing channel_review only when the original bucket refusal is `BPAY_NEXT_RESOLUTION_STALE`, the addressed existing `rate_set_pay_channel` is UMBRELLA and the native Channel module exists. It keeps the identical original Candidate/Work/expected revision; it never fetches a newer revision, dispatches/retries a mutation or creates another reader/SQL owner.

The shared redirect helper102–107 checks current caller, exact generation and the **addressed root Work** (rather than any linked Work), actual native source UMBRELLA, actual current target UMBRELLA and unequal native company IDs before close. Only a successful native close followed by current owner/actor/caller permits the existing Channel window to open. Invalid/currently changed scope, no actual company difference, fallback read failure or refused close cannot generate a new financial action; the original refusal is retained. Original persisted request recovery remains ahead of live reads, and all original SAVE/CLEAR/quote/journal paths remain unchanged. The existing Rates cache URL is now wb32-r2.

SOURCE PASS retained; zero open source findings. Current Rates SHA-256 `eaefe6ced545df2d6da40f1417140b770c989282553e7f276c0626e75b84a6a7`; index `eb37d5018a381c656ef06d29d68e40b2842995c88f0119e23c02ea1fdd41b4d0`. Current already-saved presenter bytes: pay-channel `a62a343fec841779001ea800660316ceebca846bd6bd76589fca54d78d4fcfc8`, component-choice `3a558fd0b7f8870d69395cf7c386b628367cd631926a38bacf8c3a9ee90b0dab`, CSS `bdca5176c8b24d00fcf26341a80ea52e2abcf330f248f144439662546eb81aa1`. Boundary remains `39b528c85abcfcd6c65cb6316162dd90e8f0eb719b9fa53478846881f264f662`; native0417/0452 and closed BE/FE transport unchanged. No SQL/test/business/app action was performed; A4 and app acceptance remain separately owned/NOT_RUN by this reviewer.

