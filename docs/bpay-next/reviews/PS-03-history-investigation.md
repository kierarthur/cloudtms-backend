# PS-03 history caller investigation

10 October 2026. Read the full PS-03 card (Plan 06, 1374–1394), exact D06/D07 checklist rows, current registered 1206 history owner, both current history callers and their existing ordinary source/unit checks. Source investigation only: no tests, gates, SQL, fixtures, runtime, network, secrets or product edits. D01/D02 database enforcement and future D05 acceptance remain separate.

**The existing 52-section wire contract has the necessary actual facts. No new MEMBERS field, SQL history successor or cash calculation is needed for D06/D07.** Preserve the history chain for its named later PS-07 work. Current generic rendering already displays every exact member field and signed contribution, and every transfer's stored cash/count/sum. The finite caller gap is that `childLinks()` has no MEMBERS-specific origin navigation, and a reissue TRANSFERS row does not offer its actual original transfer's RETURN_CASH section. These can be supported entirely through existing section/parent identities; adding inferred origin IDs would be wrong.

## Actual stored shape and read contract

1324:727–740 establishes the four disjoint kinds. Current 0400:47–77 adds the genuine Case instruction/result and replaces the old shape: WORK requires its run line and no Case fields; CASE requires instruction/result, no run line, and permits no hold only for an actual zero contribution; NET_ADJUSTMENT/CASH_REISSUE have all four member origin fields NULL. In particular, **a zero Case result must not acquire a fabricated hold**, and NET/reissue must not acquire a fabricated line or Case origin.

| Actual kind | Existing exact origin and relevant read sections |
|---|---|
| WORK | MEMBERS `run_line_id`; RUN_LINES gives the frozen line's captured revision, approved line, work, component, source consumption/channel, target channel/policy/window and frozen ex/VAT/inc. HOLD and POLICY already accept this exact run-line ID as their RUN_LINE parent. |
| CASE | MEMBERS `case_instruction_id`, `case_allocation_result_id`, optional genuine `case_hold_id`; RESULTS takes the actual instruction as its parent and returns the exact result ID and frozen allocation. WORKER_CASE_HOLDS exposes the actual worker's hold/instruction/result links. A zero member can carry its real instruction/result with no hold. |
| NET_ADJUSTMENT | MEMBERS `transfer_id` is the actual containing leg; that TRANSFERS row holds its exact `projection_id`. The same worker's PROJECTIONS section contains that immutable projection, its exact `request_command_id`, input kind and accepted amounts; the existing PROJECTIONS→COMMAND link exposes NET_REQUEST. These are transfer/projection/request facts, not missing member columns. |
| CASH_REISSUE | The containing TRANSFERS row holds exact `original_transfer_id` and `return_cash_id`; 1324:680–718 binds projection-versus-reissue exclusivity, same Candidate original transfer and the compound actual return-cash origin. RETURN_CASH is keyed under the **original** transfer. REISSUE_REQUEST retains the genuine command, return-cash and newly created transfer IDs. |

1206:86–108, Worker history lines 27–47 and Office history lines 28–48 declare these same fields. TRANSFERS carries `cash_amount`, `member_count` and `member_cash_sum`; MEMBERS carries `signed_cash_contribution`. Keep all of these server facts as exact strings. The reader's declared casts are to text (1206:363–366), not floating point; neither caller adds, apportions or recalculates cash. The stored member sum belongs to the actual transfer owner/ready checks, never to a browser page sum: MEMBERS can span several bounded pages.

## Smallest caller support

D06 can retain the exact RPC, section schemas, limits, cursor and scalar transport unchanged. If adding an explicit member qualifier, place it inside the existing MEMBERS response loop: require `transfer_id === request.parent_id`, one of the four actual kinds, and the exact current 0400 disjoint UUID/null shape above. Preserve the real zero-Case/no-hold exception. Reuse an existing exact monetary-string rule if needed; do not convert or aggregate its contribution. A generic STRING_OR_NULL field contract alone currently does not check the member kind or bind its row transfer ID to the requested parent, although the SQL reader's FK filter does so. These are narrow response qualifiers, not a new schema or reader.

D07's existing `Object.entries(row.data)` rendering already preserves subject kind, exact IDs, signed value and NULL-as-Not-recorded. Keep that display and add only finite typed links/labels if needed to make its origin trace explicit:

- On MEMBERS, use its actual `run_worker_id` to expose the existing WORKER parent, retaining an already admitted Timesheet instruction context. For WORK, use its actual `run_line_id` for the existing HOLD/POLICY sections. For CASE, use the actual `case_instruction_id` for RESULTS; display `case_allocation_result_id` and genuine optional hold without guessing a component ID.
- On a TRANSFERS reissue row, add RETURN_CASH with **that row's `original_transfer_id`**, not its own ID. Its returned `id` must match the already displayed `return_cash_id` when presenting an exact origin. Existing ancestry checks remain authoritative; an unavailable scoped parent must not be replaced by a guessed Candidate or current owner.
- For NET/reissue, show the containing transfer's exact origin fields. The current TRANSFERS→MEMBERS button has the actual frozen transfer row available. It can retain that small row in the existing bounded navigation entry for display, without putting an extra property on the closed RPC request. If a MEMBERS page was reached from GROUP_LEGS without that row, expose the existing worker→TRANSFERS path and show the actual parent transfer ID; do not infer a projection/return ID or pick the latest projection. The exact projection can then be identified by its displayed ID in the existing bounded PROJECTIONS pages and its command link used for NET_REQUEST. There is no need to make a new point reader or fetch every section automatically.

This retains whole-Candidate/whole-transfer labels. Existing `TRANSFER_MEMBER_CONTRIBUTION_NOT_TIMESHEET_ALLOCATION`, `amountNotice()` and render text explicitly reject Timesheet allocation; raw field display also preserves server sum rather than reclassifying a member amount as paid history. No Client allocation, PaymentSummary layout, account, export, provider, enqueue or financial owner change is authorized by these two rows.

## Existing source checks and limit of evidence

Backend `tests/bpay-next-payment-history-caller.test.js` already compares all 52 section definitions to the registered SQL source, exercises exact scalar/cursor preservation, closed fields, bounded streams, authenticated fixed RPC and refusals. Frontend `tests/unit/bpay-next-payment-history.test.cjs` already compares the closed metadata with backend, checks graph reachability, bounded paging, actor/close safety and exact scalar/date display. They do not currently contain an explicit four-kind member-origin qualifier/typed-link case. Any finite caller support should be covered in these existing ordinary files; no separate proof programme, business fixture or joined execution is proposed. This reviewer executed none of them.

Fresh read checkpoint SHA-256:

| Source | SHA-256 |
|---|---|
| `broker/src/bpay-next-history.js` | `81d4ca2fb90b1bbec7dea3a41698933f70b306833079484d45a89e4e77738451` |
| BUILD-FE `js/bpay-next-payment-history.js` | `e6efd41005e6c80808570ea1d92c25f3e16adcf1eb8afb5cf1835444cf7323c5` |
| Registered 1206 history reader | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| Core 1324 | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| Current 0400 Case-member shape | `1a7ca4f86eca7e96775ad4cfc47a8165768d1c41ad87f6f81752555d2648c987` |
| Existing backend history checks | `8a09560333f72c485564777980d0c82af4451d0f4e73f9e02f55cc01f54fc98c` |
| Existing frontend history checks | `eaabc0a33acd75568698c218438a64bb61529d1fc211e506bb4d2e5fcd3a5af3` |

No checklist, installation or in-app acceptance tick follows from this investigation. Actual four-origin conservation remains D01/D02/D05 authority.
