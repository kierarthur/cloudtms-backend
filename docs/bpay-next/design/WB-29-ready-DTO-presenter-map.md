# WB-29 Ready chosen amounts — DTO and presenter edit map

13 October 2026. Read-only source investigation against approved `WB-29-ready-reader-join.md`. Root alone builds product. No product/checklist/other artifact edit, SQL/RPC, business call, fixture, test or screenshot action. All implementation and joined acceptance checks below are **NOT_RUN**. Relevant repository rules were read: source/schema verification, narrow changes, no live finance fallback, preservation of freshness and Policy X; this task expressly narrows work to saved source and this note.

## Exact transport seams

The paired files are BE `broker/src/bpay-next-workbench.js` and FE `js/bpay-next-workbench.js`. Both currently have identical closed ROWS entries at **GROUP_LINES:234–306** and **CANDIDATE_GROUPS:351–403**, before the shared selection-memory/count additions 405–409. They require every mapped key via `shape` (BE:467, FE:466); nullable means an explicit JSON null, not an omitted optional property.

Add only these approved properties:

| Existing kind | Property | Existing field type |
|---|---|---|
| GROUP_LINES | `chosen_component_ready_amount` | `decimal?` |
| GROUP_LINES | `chosen_leaf_amount` | `decimal?` |
| GROUP_LINES | `rate_set_pay_channel` | `channel?` |
| CANDIDATE_GROUPS | `rate_set_pay_channel` | `channel?` |

Existing nullable handling accepts null at BE:439/FE:438, channel enum accepts only PAYE/UMBRELLA at 428, and decimal handling retains native strings (BE:446/FE:445). Do not use Number, arithmetic, arbitrary quote JSON, new precision limits, or new kind/request/route fields. The 131072-byte transport envelope limit is unchanged (6); native reader packing retains its stricter original 110000-byte checks after enrichment. No schema extension belongs to HEADER, CANDIDATES, ISSUES, EXPORT_ROWS, estimate, choice intent or command payload.

## Narrow crossfield guards

Extend only the existing kind branches: GROUP_LINES at **BE:618–621 / FE:617–620** and CANDIDATE_GROUPS at **BE:630–633 / FE:629–632**. Keep all current component-knownness, physical identity, Source provenance, selection basis and group monetary tuple guards unchanged.

For a member, the three additions form **all-null or all-non-null**. A non-null tuple requires actual Work/revision/approved-line IDs, no Case identity, WORK_COMPONENT or WORK_SEGMENT, taxable, READY, selectable, amount-known and component-amount-known, PAYMENT, AVAILABLE or ZERO, unselected, current progress, no accepted Run/command/epoch, and rate-set channel equal to the row's non-null native target channel. Its existing component Ready amount and Included Source amount must be known. A zero-included leaf has exact decimal zero chosen leaf; compare decimal strings with the existing exact comparison rather than JavaScript truthiness. Preserve native component/segment identity checks; do not infer a segment from a key. Do not require chosen amount to differ from Source: a valid chosen rate can produce the same amount or zero. Do not infer positivity/sign constraints for a component subtotal beyond native qualification.

All-null is permitted even on an eligible member: it represents genuinely absent compatible saved quotation. Saved-but-unavailable quotation remains the server's existing refusal, never a fabricated all-null successful fallback carrying a rate-set badge. Non-taxable, Case, reserved, selected, non-Ready and historical ineligible rows must have all three null. Any repeated non-null tuples in the bounded response for the same Work/revision/approved-line component must agree on component chosen amount/channel; checking equality is contract validation, not financial calculation. Do not require page totals or sum child amounts.

A non-null group channel requires WORK, READY, complete, clean, CURRENT, actual Work/current-revision identity, known group amount, no accepted tuple and channel equal to native target. It does not mean every Work component has a decision: the server explicitly qualifies a compatible ACTIVE decision on an eligible genuine component. Local memory default-selected/counts do not mean financially accepted; do not reject the group based on local selection. Cases and every other section return null. No browser lookup can prove ACTIVE/date compatibility; that is the approved reader's responsibility.

## Existing money and badge presenters

| Source site in FE Workbench | Required narrow use |
|---|---|
| `amount(member)` at **1254** | Keep manual-unavailable, accepted/selected, unknown and reserved precedence. Only the unselected READY branch uses `chosen_leaf_amount ?? included_source_ex_vat`. Nullish fallback preserves genuine chosen zero. WAITING/ACTION/BLOCKED keep original Source money. |
| Component/day heading at **1286–1294**, especially **1290** | Use the server's `chosen_component_ready_amount` when one actual returned member in this exact component carries a qualified tuple; otherwise retain the known Source component Ready amount. Keep separate component waiting/reserved values. **Do not read only `first`: it may be a WAITING or RESERVED portion with null additions while another returned Ready member has the chosen tuple.** Find a qualified member in the existing bounded component group; never sum visible leaves or calculate a delta. |
| Single-shift shortcut at **1293** | It reuses that same component heading money. Preserve its complete-from-start/all-native-pages conditions; do not reinterpret component total as a shift allocation when the shortcut does not qualify. |
| Native child/portion money at **1296** and attributed shift money at **1303** | Existing `amount(member)` supplies the exact server leaf value. Keep the genuine aggregate-with-no-attributed-shift branch null. No rate×hours, proportional splitting or prefix arithmetic in Office. |
| Existing component title/status at **1288–1294** | Add the exact qualified channel text “Umbrella rate set” or “PAYE rate set” using an existing native badge style, once per displayed component. Take its channel from the qualified tuple, not Target alone. Do not turn stale/selected/reserved portions into rate-set claims. |
| Candidate Work group money at **1516** | Continue displaying native `group_amount`: chosen whole-list indexed amount already belongs to the server. Keep Case estimates, outstanding/due facts, accepted display and estimate-outdated behavior ahead of ordinary group money. No chosen re-sum after paging. |
| Ready group channel cell at **1517** | Keep the native target label and add the rate-set badge only from non-null `rate_set_pay_channel`. Do not infer a rate decision from Target, date sensitivity, reason, or a child's presence. |
| Unresolved/other-section qualification at **1169 / 1518** | Preserve the existing “PAYE/Umbrella, as authorised” Source amount qualifier for rate-resolution rows. Owner F does not reprice unresolved ACTION rows or accepted Drafts. |

`amount(member)` also supplies a read-only Snooze subject at **1257**. A chosen displayed subject remains informational; retain the actual native `seed(member,scopeKind)`, expected IDs/basis and original command journal. Never substitute any chosen display field into Snooze/selection/financial request operands.

## Source totals: visible ambiguity to close

Candidate and header summaries remain their literal native Source amounts. Once child Work rows show chosen target-rate money, these existing plain labels can wrongly suggest equal bases or a payable target total:

| Site | Current copy/fact |
|---|---|
| Office Workbench strip **839** | “Ready to pay” + READY HEADER amount |
| Scope filter count card **925** | Candidate count + HEADER amount + “ready” |
| Ready Candidate phone sort **1060**, desktop headings **1081**, phone cell label **1085** | “Amount ex VAT”; actual Candidate money is `row.amount` at **1086** |
| Candidate window strip **1500–1501** | “Ready to pay” + Candidate-scoped READY HEADER amount |

Close this with copy in those exact desktop, phone and filter sites, for example **“Ready amount, as authorised (ex VAT)”** or Root's equivalent explicit authorised Source qualifier. Do not imply all amounts are one PAYE/Umbrella channel when the scope mixes native source channels, and do not mistake the capitalised Source workflow kind for the financial source-channel basis. Candidate/header totals need not equal visible chosen Work group totals; state the basis instead of inventing a chosen summary or summing pages. Separate NET additions at **1087–1088 / 1501**, Case estimates and estimate summary 1499 retain their own labels and amounts.

Group Amount headings at **1480** can remain “Amount ex VAT”: their actual server amount and whole-list order are both chosen-qualified where applicable. Existing cursor key uses `row.group_amount` (**BE:514 / FE:513**) and retains group-kind/ID tie at 510/509; no new sort key, browser sort or cursor envelope is needed. Candidate amount sorting still uses its original Source `row.amount`. Existing exported native Source facts must not silently become chosen money through this map; no export schema or aggregate is added.

## Source checkpoint

BE Workbench SHA256 `715D3B419F5725845EEB814C6DD3D2A47DB6168B3E3E7DE07D0C01E1E5A82C6D`; FE Workbench `329B23B912C69D06DFAA1063725E6E650667D3A3F53CBF2755203E035073EF24`; approved reader join `1B226BE2B9E569871066BB66B1B858273C2457A9734E6A26F746E30E2899E9B5`. These are saved disk observations, before Root's DTO/presenter edits. Re-read bytes before editing. The note adds no producer, summary, quotation function, money owner, frozen reference, protocol or parked F25 dependency. Required source/pixel review of Root's future presenter changes remains NOT_RUN here.
