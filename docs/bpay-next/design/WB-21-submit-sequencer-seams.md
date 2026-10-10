# WB-21 submit sequencer: concrete Office insertion seams

Status: read-only source/design investigation, 10 October 2026. No product writes, SQL, tests or runtime/business calls. Current implementation is being authored concurrently; findings below bind the read hashes and do not claim completion. Rule B remains native expected revisions, captured selection Candidate columns, original raw query snapshot/pin. No2153/parked accepted-input reader or new financial protocol is proposed.

## What is reusable now

`js/bpay-next-office.js` already has the right dispatch core:

- `requestBody`/closed receipt validation at243–268 supports the newly authored descriptor CREATE variant `{run_id,command_id,descriptor}` and its actual extended receipt.
- `createTransport` at289–341 sends only existing preparation/command endpoints, protects current actor/environment, bounds response bytes/time, correlates action/IDs and preserves typed definite versus unknown errors.
- `createController` at446–548 saves `pending:{action,bytes}` **before** dispatch. `retry()` reuses those bytes. Only an initial definite SERVER_REFUSED/AUTH_REQUIRED clears pending; a refusal after an earlier unknown result retains it. Keep that distinction.
- Native WORK_PAGE/component/Case receipt validators exist. WORK_CHOICES and SELECTED_COMPONENTS have bounded native cursor/ID validation. Existing visible progress reads WORKERS/STATUS and stops on close/hidden document.
- The new Ready choice store at `bpay-next-workbench.js:679–710` is bounded, exact namespace/descriptor, no network per tick and no money. `mountReadyCandidates.countSelection` at771 qualifies the existing HEADER count against the same context and local revision.

The existing controller is **not** the full submit sequencer. Its journal key is actor-only `cloudtms.bpay-next.v1.<actor>`; state retains cumulative accepted Work/component IDs with a100-ID limit and one current Candidate's Case cursor/counters. There is no accepted namespace/phase/cursor/page continuation. CREATE adoption currently records only selection_state, not its newly returned original command/sequence and accepted choice counts.

Current Office still renders Start selection / Add selected Work page / Prepare selected Work at665–678, using CURRENT_WORK after CREATE and generating a new command UUID at SEAL_ENQUEUE. That is the concrete normal-flow replacement seam. Preserve existing sealed Draft/review/frozen detail users; do not replace the financial controller.

## Smallest concrete caller insertion

Put one finite submit sequencer beside the existing local choice store, using the **existing** Office controller's journal/dispatch mechanism through a narrow injected interface. No new JS framework, endpoint, from-selection server function or command kind is needed.

1. Add one `onCreate` callback from the Ready host/header to Office. Capture the exact choice-store object, local revision, qualified Workbench context, trusted actor and active parent frame. Await the existing `countSelection()`; recheck all captured identities before opening supplied SCR160 confirmation and again before dispatch. Confirmation text uses actual selected Candidate count/channel/scope only. Cancel sends no mutation.
2. Generate one run UUID and one PREPARE command UUID **once** and persist them with the exact descriptor before CREATE. Adopt the actual descriptor-CREATE command/sequence/count receipt. A known accepted command must be reused by SEAL_ENQUEUE; no fresh `controller.newId()` there. Empty acceptance has no nonnull sequence and is not a started preparation. Preserve its real empty receipt and retained per-Timesheet reasons.
3. Extend the existing journal with the exact agency/module epoch/actor/official-date/scope namespace, accepted descriptor/local generation, original run/command/sequence, finite phase and current page cursor/counters, current Work/subset or Candidate only, readback cursor/counts, and **one** pending exact request body. Keep byte bounds; no all-agency member array or every prior body archive.
4. Instantiate the accepted submit branch only after a qualified namespace is available. Leave old actor-only journals, unknown bytes and sealed-run/frozen controls intact. Do not silently attach an old actor-only uncertain request to a new agency/date/scope. Any needed journal-version adoption must preserve exact original bytes and IDs.
5. Advance phase/cursor/counters only after a validated, fully correlated receipt is retained. Persist state and pending together through the existing save path. A definite read failure holds the phase; an unknown mutation permits only identical manual Retry. No timer retries CREATE/page/SEAL, fresh UUID, clear pending, auto REVIEW/drain or re-enumeration.
6. Replace cumulative accepted-ID arrays on this accepted-submit branch with current ≤100 page identities and decimal-string counters. Preserve the original arrays for the old thin manually selected page branch if still needed by retained users; do not raise them to agency size. Case counters belong to the actual current Candidate and move to the next only after its real CASE_SEAL.
7. On accepted CREATE, unsubmitted edits become a new local generation; they must not mutate the retained descriptor. Acknowledge/clear only the exact original store revision once the complete receipt/readback is retained. If the user changed the store meanwhile, its newer intent remains dirty.
8. On complete SEAL_ENQUEUE, perform actual native readback and pass its authoritative run_id to the named progress opener. Retain the sealed receipt even when readback/progress fails or the user closes. Actor/owner/frame checks guard every await/adoption; closing cannot clear accepted/unknown journals.

This is a caller-local continuation over the card's existing seven actions, not an alternative acceptance protocol.

## Seven-step ownership and real current limitations

| Step | Exact caller behavior / native input source | Owning delivery / current availability |
|---|---|---|
|1 CREATE| Frozen original descriptor/run/command. Use current actual descriptor variant and adopt its actual receipt. | WB21-D01/D10/D11/D15. Current draft request/receipt maps present; no source-completion claim here. |
|2 SUBSET| Explicit descriptor Work tuple → WORK_PAGE SUBSET, bounded selected approved-line IDs → COMPONENT_PAGE then COMPONENT_SEAL. Actual WB36 automatic segment certificate/page/seal only where required; no independent local segment boolean selector. | WB21-D05/D06/D10/D15 provides sequencer/adaptation; WB25-D12 later owns full line/day controls. Existing actions available. Do not pick new current revisions/lines after CREATE. |
|3 exclusions| Original bounded Candidate/Work negative overrides → ≤100 combined ID EXCLUSION_PAGEs, including its required empty receipt semantics. | WB21-D14/D10 required schema/integration; WB23-D06 owns the complete future matching/exclusion enumeration. Current inspected Worker/1832 intake has no action. Dispatch remains gated until its actual closed input/output contracts are joined. |
|4 matching or explicit ALL| ALL_MATCHING walks actual immutable MATCHING_PAGE receipts until complete; NONE sends explicit retained Work ALL tuples, ≤100. Subset Work is never sent a second time as ALL. | WB23-D06. NONE has genuine descriptor IDs now; ALL_MATCHING cannot be emulated with live Ready/CURRENT_WORK pages or WORK_CHOICES readback. |
|5 Cases| Per-Candidate accepted component-ID/selected-boolean pages, exact retained request UUIDs and selection revision, then CASE_SEAL. No later current Case/rule/status enumeration. | WB21-D07/D08 caller/owner adaptation. WB26-D15 owns complete default/exclusion controls and required accepted membership transport integration; its D05/D06/D07 name existing current Case/component/page/intake context. See required seam below. |
|6 guard| Only when actual DR10 GUARD says the same-week PAYE decision applies. No guessed PAYE exclusion or successful dummy step. | DR10 later; no current joined GUARD contract. |
|7 seal/readback/progress| Same original command UUID; original counters/completeness, native WORK_CHOICES/SELECTED_COMPONENTS readback; authoritative receipt to progress. | WB21-D04/D10/D15; DR02-D03/D05/D06/D09/D10/D11 owns the named server/button joins. DR06-D01/D04/D08/D09 owns progress later. |

Authoring the sequencer's finite future phases now is appropriate. Dispatching an absent required stage, skipping it as success, or advertising complete ALL_MATCHING/default Case submission is not. Even NONE may require future Case/default or native segment certification: absence of explicit Case overrides is not proof that CREATE accepted no Case members. Use the actual accepted choice_case_count and complete scope; never infer defaults from an empty browser list.

## Exact MATCHING/EXCLUSION contract evidence

There is **no existing exact closed MATCHING_PAGE receipt field schema** in the approved WB23 card or current inspected Worker/SQL source. Do not invent one and label it existing.

The exact card at `02_WORKBENCH.md:912–916` supplies:

```text
EXCLUSION_PAGE input:
{run_id, page_no, excluded_work_ids[], excluded_candidate_ids[]}
combined ID count ≤100; identity (run_id,page_no)
identical replay; changed body BPAY_NEXT_SELECTION_PAGE_CONFLICT
closed after first MATCHING_PAGE or seal: BPAY_NEXT_EXCLUSIONS_CLOSED

MATCHING_PAGE:
materialises immutable accepted normalized scope/membership
≤100 work-choice rows; original bounded append owner
immutable page number/body hash/cursor/receipt
includes empty and only-skipped pages
caller continues until complete
```

Current actual WORK_PAGE receipt is closed:
`{run_id,page_no,item_count,selection_count,selected_candidate_count,replay,skipped}`.
Reusing its native counters/skipped binding is the smallest basis for the later matching owner, but its positive item_count and absent complete/cursor mean it **cannot simply be relabelled** MATCHING_PAGE. WB23-D06 must first define the actual complete closed matching input/receipt/cursor (including zero-row completion), update existing SQL intake + Worker INPUTS/OUTPUTS + Office maps together, and prove immutable scope/no duplicate SUBSET behavior. The sequencer then consumes that exact joined shape. No new transport route is needed.

## Required accepted Case page source seam

Current `CURRENT_CASES` and `CASE_COMPONENTS` requests contain Candidate/Case/current ordinal but **no run identity**. They return current business facts; using them to derive accepted Case IDs after CREATE defeats the accepted scope. The current candidate-scoped controller also switches Candidates through live CURRENT_WORK then reads current Case components. These are factual navigation, not the immutable submit source.

The authored native accepted membership already names the necessary evidence: rows of `bpay_next_run_choice_member` with same original run, member_kind CASE_COMPONENT, actual Candidate/component ID, selected boolean and stable member_no. They contain no Case balance snapshot. A future accepted-run variant of the **existing public preparation-page / Case-component reader integration** must expose bounded retained IDs/booleans and a genuine stable cursor/group identity from those rows, not join live eligibility or fabricate2153 accepted inputs. Reuse the named existing page transport; do not invent a new route, unrelated reader or new action name here.

WB26-D15 owns complete selected/excluded/default membership across unloaded pages; WB26-D05/D06/D07 explicitly provide the existing Case/current-component/public intake seams, and WB21-D07/D08 consume the resulting immutable manifest. Current source has no such accepted-run page variant; exact additional request/receipt fields are therefore a **future owner contract**, not a field schema this investigation can claim already exists. The builder may author the caller's pause/resume seam now but must gate it until that actual variant is joined. Explicit descriptor Case overrides alone cannot reconstruct the implicit accepted default members/count.

## Readback and progress without premature ownership

After real seal, page WORK_CHOICES using its native after_work_id key, verify monotone unique identities/revision/mode/SEALED shape and accumulate only counts/current page. For each actual SUBSET, page SELECTED_COMPONENTS using its native selection ordinal, validate the exact frozen revision/IDs and compare complete count to that Work choice. No post-seal current revision/amount reads and no cumulative agency list. Readback failure retains the original sealed receipt.

DR02-D10 explicitly allows until DR06: “Preparation started — run <id>”. The full `openBpayNextPreparationProgress(run_id)` window/counters/polling is DR06-D08/D09, not a newly invented WB21 progress owner. Existing WORKERS/STATUS reads can preserve factual original-run continuity; empty workers are not ready/completed enrollment. ACCEPTED_CANDIDATES/ACCEPTED_WORK remain refused/dormant under Rule B, even though schema decoders exist. No automatic REVIEW/CONFIRM as substitute for missing progress.

## Read hashes

| Source | SHA256 |
|---|---|
| FE Office | ea7737dffe26e74d4e064bb1a147be0f8e8459c392d43435423968f53857b646 |
| FE Workbench | b5ce284a12e1d923a7fbd775c01f1010c12600357e48bea35afc95950a98df1a |
| BE preparation adapter | c04cdd54164ac1a534f2ddb366485b3b842568fd88fdb8a187476ecd925404b0 |
| BE preparation raw RPC transport | 642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34 |

Full WB21/WB23/WB25/WB26 cards and exact checklist rows, shared seven-step interface, DR02/DR06 owning rows and current1832 intake allowlist were read. This is source investigation, not implementation or in-app acceptance.

