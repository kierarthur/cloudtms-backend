# DR-18 native confirmation and created-Draft screen investigation

12 October 2026. Independent read-only UI investigation of complete approved card03:473–496, all current DR18 checklist rows3649–3668 and the four supplied B4 SCR48/49 PNGs/specs. Root alone authors product. This identifies the minimum UI integration; it is not implementation, A4 approval or joined acceptance. No server or Worker replacement is proposed here.

## Exact remaining native UI seam

Current `js/bpay-next-office.js:2192` dispatches CONFIRM directly from Draft review, writes Draft confirmed, then reloads rows. The older thin Sheet2610 also dispatches directly. Neither delivers SCR48's final confirmation or SCR49's factual created-Draft result. Both should use the same small native confirmation/result helper in the existing Office host, retaining the original controller, accepted run and exact reviewed revision. Reuse `CloudTMSBpayNextUi.createStandardWindowAdapter` (`js/bpay-next-ui.js:32–74`) through `options.showReadWindow`, rather than another modal host. Existing Office2122–2139 demonstrates the native question child, Cancel/action footer, dismissal settlement and actor check after await.

The confirmation can use the already-qualified `REVIEW_TOTALS` facts: `channels` is PAYE then UMBRELLA, with native candidate_count, gross_ex_vat and gross_inc_vat (`Office207–208,228,242–246`). Current `readTotals`2026–2030 compares run/review/selection and both totals revisions with the actual PREPARE_ISSUES page. Use only nonempty channel facts and the full native whole count; never count the loaded list, sum pages, calculate VAT or substitute bank cash for gross. The actual Confirm request remains exactly `{run_id,review_revision}` (`Office144,171`), with current permission/state/exact-revision admission owned by the existing server.

Freeze the displayed native facts and original review revision for the question. On confirmation dismissal, require the same actor/NEXT owner/run and parent identity before dispatch. A concurrent revision change must refuse the original request rather than silently refreshing it and confirming different facts. Refresh can let the user deliberately review and open a new question. Cancellation dispatches nothing. No reauth, current financial revalidation, new click descriptor or preflight storage is introduced by this screen.

## Exact existing result and Open addresses

CONFIRM's closed receipt already contains one or two native `drafts` rows, each exactly `{run_id,pay_channel}`. The validator515 requires PAYE before UMBRELLA when two exist; `CHANNEL_DRAFTS`1013–1015 returns the same complete bounded list. `adopt`1400 saves the successful rows in the original V7 actor journal. `setDrafts`1443–1446 checks exact retained address equality; `bindDraft`1448–1451 admits only an address in that actual list.

Create SCR49 from the validated receipt's actual rows, with Open PAYE Draft/Open Umbrella Draft for exactly those channels. A fresh CHANNEL_DRAFTS read can reattach after reopen. Use the server channel totals already shown for counts and gross labels; unavailable facts remain unavailable. The displayed PB-0412-P/PB-0412-U reference and Awaiting approval labels in the picture are not present in the native two-field address contract: omit those picture-only sublines unless an existing qualified reader actually supplies them. Do not derive a reference from run UUID, invent a pay_batch_id or add fields merely to match the sample.

The existing native Open seam is `Office2579 openDraft(address)`: bind the exact returned channel, clear all prior subject/detail caches, then load CHANNEL_WORKERS. The full thin host renders these actions2605–2608. A reusable guarded callable seam can expose this existing behavior to the created result; no new reader/kind/command is needed. Case protected navigation's `selectDraft`2658–2667 illustrates complete cache clearing and qualified native worker binding, but requires a real worker/Candidate and must not be invoked with guessed IDs for a whole channel. A fresh review-run-qualified controller can be preseeded with the actual drafts before entering the existing Office host, avoiding another REVIEW on a confirmed run.

**Payment Batches handoff gap:** current main7170–7171 rejects NEXT for its payment_batches destination. Existing main45793–45795 renders the legacy batch list; legacy `banking:pay:openChild`103303–103307 needs a real batch ID. Its successful legacy handoff45433–45471 changes the legacy frame tab and opens that batch. These are useful shell references, not valid native run/channel adapters. Add only the NEXT-qualified handoff necessary to show/open the same factual channel addresses through the existing Office host, and preserve the later owning DR07 banners/DR28 full surface boundaries. Do not route native addresses through the old batch-ID handler or claim the later whole Payment Batches screen is delivered early. This gap must be resolved explicitly before DR18 screen completion.

## Supplied screen comparison and control purpose

Individually inspected these existing pack images, with their matching JSON overlays; no pack file was copied or modified:

| Supplied B4 PNG | Required integration and justified variance |
| --- | --- |
| `SCR-48_confirm_draft_dark.png` | Small native question: Confirm Draft for the native full Candidate count, one/two actual channel count/amount facts, Cancel and one primary Confirm Draft. Cancel returns unchanged; Confirm dispatches the original reviewed revision. No extra close, filter or money input. |
| `SCR-48b_confirm_draft_refused_dark.png` | Plain refusal and a way back to Banking Pay. Keep technical codes in the journal. Show actual expiry time only if supplied by PROGRESS; do not declare payments back while cancellation/projection is still incomplete. Use returning wording until the native terminal state confirms completion. Native header Close replaces the sample's redundant footer Close under SDP24; one primary navigation action is sufficient. |
| `SCR-49_draft_created_dark.png` | Header Payment Draft created; PAYE then Umbrella factual result rows; right-aligned native counts/money with Gross/Inc VAT labels; quiet Open per row; one primary Go to Payment Batches. Header Close returns safely without undoing confirmation. No fabricated PB reference or unsupported approval state. |
| `SCR-49b_draft_created_one_channel_dark.png` | Same result with only the actual Umbrella row (and symmetric PAYE-only behavior). Do not render an empty second Draft, split Source/ordinary or infer channels from current Candidate settings. |

`DESIGN_INDEX.md:63–64` selects these B4 designs; T07d is explicitly superseded by SCR49/49b, and templates are only patterns. Background Draft-review content in the overlays is the existing parent, not authority to rebuild another card. SCR48 spec purpose/reviewNotes and its PNG annotation expressly omit pay date under SDP12a, overriding card491/checklistD02's older pay-date wording. The owner's later no-preexecution-pay-date direction agrees. No date control or early pay-date display is required.

Result rows are a fixed one/two-row dialog, exempt from whole-list sort under SDP34; no sort API is needed. Native dark/device-light/phone styling, 44px phone targets, wrapping, one primary, live status/error roles and focus restoration require actual A4 screenshots before approval. The question names the consequence of final confirmation; the result Open actions and navigation are required by this card rather than decorative additions.

## Unknown, refusal and lifecycle boundaries

Retain original controller dispatch1410–1417, mutation1427–1439 and exact-byte retry. It saves the original request before dispatch; after a lost response an unrelated later refusal does not clear that original pending identity. Unknown Confirm remains an uncertain request with Try again of the same bytes; it must not show created success, create a new revision or offer conflicting Confirm. A valid replay adopts the original factual drafts and opens the same result. Initial definite refusal can clear the original pending according to the existing controller. A failed result read after successful CONFIRM does not undo confirmed state or redispatch fresh confirmation.

Current review act2024 already maps exact expiry/discard Confirm errors to plain words. Successful Confirm must stop unconfirmed expiry/review/discard presentation and preserve its result on parent remount, avoiding the parent REVIEW refresh after DRAFT. Existing PROGRESS exposes native cancellation/expiry facts; use those for truthful refusal and navigation, not client wall-clock authority. Workbench In Draft refresh is the separately named DR18 D05 owner: this document does not propose a financial owner rewrite or claim that state refresh has run.

Full acceptance remains NOT_RUN: mixed/two channels, each solo channel, same-Candidate ordinary+Source together, concurrent expiry/revision refusal, exact unknown/replay, each Open and Payment Batches handoff, and Workbench In Draft without reopen. DR18-D04 is output-only context, not proof of built feature. No product/checklist edits, tests/build/syntax, SQL/business/database calls, fixtures, browser/render/capture/hosted calls or commits were performed by this reviewer.

## Current source anchors and SHA-256

The current final-definer registry3385–3400 names `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` at physical0847, preserving2359/2018/0559/0746/0814 history. That owner is context only for this UI investigation; SQL successor review belongs to the separate assigned review.

| Saved source | SHA-256 |
| --- | --- |
| FE `js/bpay-next-office.js` | `0cc97eca7aced3cd95cac4370122955814a48f1b6493e9a287f84020fb459ffd` |
| FE `js/bpay-next-ui.js` | `f535e7e75579428ca7b97b57d406ccaedaebbf0fe15e5935574f435b772248d` |
| FE `js/main.js` | `69d84496624ef0f3aebfb204e1e81cd7ebd824a793bc130a8379d4e7a0992f5b` |
| FE `css/bpay-next-office.css` | `3f7a7a4663561092fa3ff95d3599aeb82831fbb74ed0dea06385d36b27fa4522` |

These hashes identify the investigated bytes, not a design freeze or permission to overwrite newer root changes.
