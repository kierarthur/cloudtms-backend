# XD-02 journal and receipt integration map

13 October 2026. Read-only preparation against the current XD02 card/checklist and current XD01 source. **Mapping complete; product/source acceptance and A4 NOT_RUN.** Root alone implements. No product/checklist/pack writes, SQL/RPC, database/business data, network, tests or rendering were performed. Original SCR121b/121c PNGs were viewed directly and matching specs read.

XD02 remains seq148, depends XD01/F08/F16, no design-first requirement. D01 binder/public wrapper and D02 Worker command are existing reuse; D03 native Adopt/receipt/journal caller remains missing; D04 is historical installed-unit provenance; D05 future joined checks remain NOT_RUN. All revised completion cells are unticked. The previous XD01-02 investigation records the full card/current chains.

## Exact existing seams

| Seam | Current source / purpose |
| --- | --- |
| Stored-credit row/caller | js/bpay-next-office.js openStoredCredits():1124 onward, openRow():1145. Existing options.onStoredCreditOpen(row,{current,active,refresh,frame,transport}) is the narrow hook. |
| Candidate entry | js/bpay-next-case-list.js candidateCases() More → Office.openStoredCredits; original Candidate/parent current/owner fences already provided. |
| Typed body / receipt | Office INPUT/RECEIPT, financial():808 approximately, requestBody(), receipt(). Add only existing BIND_STORED_CREDIT variants. |
| Existing network route | createTransport().mutation() serializes/compares exact retained bytes and chooses financial command route. BIND must be included in financial(), otherwise it would incorrectly post to preparation. |
| Original journal/controller | createController():1723 onward. key actor + optional existing reviewJournalQualifier; version7 adoption unchanged. mutate saves pending {action,bytes} before dispatch; retry sends those same bytes. |
| Accepted creation pointer | createController.adopt() CASE_CREATE currently sets case_id and candidate_id. BIND adds case_id=receipt.case_id while retaining the already-bound original Candidate. |
| Accepted acknowledgement | finishCaseCreation(caseId):1806 requires no pending/run/worker and exact current case_id; clears only that creation pointer. |
| Native Case readback | transport.page(CASE_COMPONENTS,{candidate_id,case_id,after_component_ordinal:null,limit:100}) is existing exact Case/Candidate point, reused by mountCasePreviewFooter(). CURRENT_CASES is the existing Candidate ID-keyset alternative. |
| Backend admission | broker command closed BIND args {command_id,legacy_case_id}; fixed public.bpay_next_bind_stored_credit_v1(actor,command,legacyCase) RPC; native receipt {case_id,command_id,sequence,phase,replay}. |
| Backend apply/read | Binder1827 → original Case-create intake/apply1536 → current private Case pages0585 / current CASE_COMPONENTS dispatch1455. Existing ordered CASE_CREATE enrollment/wake owns materialisation. No new command/job/page owner. |

Current XD01 added the11-field Office row/input/scan-cursor maps plus sorted read support (new private4-argument1452 overload and public1455 dispatch); original3-argument2049 remains. XD02 must consume that real row and original transport rather than rebuilding its own money/account request.

## Minimal implementation order

1. Establish original actor, NEXT owner/current epoch and parent frame/caller identity. Use the existing options/callback namespace. F15 owner-status exposes only active_owner/owner_epoch, **not agency_id**; never assume it contains another field or invent a new owner DTO. If an existing verified agency namespace is already supplied, it can qualify the original journal; otherwise retain the existing page/environment/actor/Candidate contract rather than creating an endpoint proof framework.
2. Reuse createController with a stable Candidate-qualified reviewJournalQualifier, e.g. stored-credit plus actual original Candidate identity (and already-known agency where provided). Do not include a live sort, row position or changing owner epoch in the lookup key. A row-qualified key alone becomes undiscoverable when a successful native bind already removed that row from the live list. On reopen read this same original journal **before** a fresh list or newly selected row.
3. Require no foreign run/worker, Case edit/pause or unrelated pending action in that journal. For a fresh action bind its original Candidate before mutate. For pending BIND parse the exact stored bytes and require action and original legacy Case identity; never bind a different Candidate over an unknown pending request. Accepted case_id is a retained authoritative pointer, not permission for another bind.
4. Add Office INPUT {command_id:uuid,legacy_case_id:uuid}; RECEIPT {case_id:uuid,command_id:uuid,sequence:positive,phase:caseCreatePhase,replay:boolean}; financial command routing and exact case_id=command_id qualification. No money, bank version/hash/details, tax, reason, actor substitute or caller-selected RPC enters the body. Worker supplies actor.
5. Safe-first native confirmation uses the validated original row: Candidate, amount, Not taxable, beneficiary and ****last4. Confirming generates one UUID and calls controller.mutate(BIND_STORED_CREDIT,args). Original mutate saves pending before network; failure to persist must prevent dispatch. Typing/selection/confirmation opening dispatches nothing.
6. Initial typed definite refusal may clear pending under the existing dispatch(false) rules. A lost/malformed/timed-out reply retains pending. Explicit controller.retry() sends the same original command/legacy IDs/bytes; dispatch(true) never clears it on a later auth/refusal because that says nothing about the earlier possibly committed attempt. No new UUID, replacement credit, financial readback protocol, automatic resend or broad storage scan is needed.
7. Qualify the accepted receipt and retain case_id=command_id with original Candidate. Show “Credit adopted; the Case is being added” or equivalent actual pending copy, then reread only that original native Case. Do not infer materialisation from disappearance from the stored list: the origin exclusion happens in the admission transaction, before queued Case creation finishes.
8. Once the native exact Case read proves this Candidate's Case, invalidate/refresh only affected Candidate/current stored list. Clear the accepted pointer through finishCaseCreation only after that qualified acknowledgement/callback. If acknowledgement, storage or parent refresh fails, the accepted receipt stays final and the remedy is read-only Refresh/Check progress; do not offer another Adopt.
9. Guard each awaited response/adoption/close/focus by the original actor/owner/current parent and owned active frame. Confirmation makes the parent temporarily inactive; use parent lifetime for retained ownership and active confirmation for consent. Close retains the original controller journal. Returning must focus only the still-current original row/control; no unrelated parent refresh or reopen is necessary.

The exact-native CASE_COMPONENTS reply identifies Case/Candidate and real component family/tax. For this original binder it should be the CREDIT / MANUAL_CREDIT / NON_TAXABLE native creation. Do not require the new Case component's source channel to equal the stored-credit DTO's original UMBRELLA: the binder records original Umbrella origin separately while Case-create admission captures the current PAYE Candidate. Preserve that native distinction, never apply browser conversion.

## Failure traps to avoid

- Routing BIND to preparation by omitting the financial action list.
- Regenerating command_id after timeout/reopen, or waiting for a live row before looking at its already-retained original bind.
- Treating removal from the masked list or ACCEPTED_PENDING_CASE as proof the Case is already visible/paid.
- Clearing accepted pointer before exact Case readback, or losing it because a later list refresh failed.
- Reusing an actor-wide unqualified run/Case journal and overwriting another pending operation.
- Including owner_epoch in a new lookup key, then silently forgetting an old original request on epoch change. Original native authority may safely refuse old replay; the unknown identity still must not be replaced.
- Translating every not-eligible/404/read failure into “being added” or false zero. Preserve authoritative adoption plus honest inability to check; do not fabricate a completed Case.
- Financial retry for a known accepted response. Only the unknown original command may be explicitly replayed.
- Printing bank hash/version/full account/sort code/UUID or picture-only “Train fares refund” labels absent from the11-field reader.
- Showing a fresh live amount/account as the original unknown confirmation. Use retained known presentation if available; if it was not retained, omit/mark unavailable instead of guessing.
- Repricing the stored amount or promising immediate next-Draft payment. XD03–06 own final frozen allocation, bank safety, execution/settlement and advice.
- Retrying or modifying an unrelated accepted Draft after adoption. This is only current/future stored-credit binding.

## Supplied design qualification

Original B9 SCR121b shows safe Cancel / Adopt credit, masked account and original amount. SCR121c shows a success receipt in the same list after the original row is removed. Native source has no human description in its11 fields; generic “Stored credit” is truthful. The illustrated assurance “It now shows in … Cases” is permitted only after actual Case readback; before then display accepted/pending creation. “With the next Draft” is not payment execution or universal eligibility assurance.

The new confirmation/result must reuse the native adapter, dark theme, header-only Close outside an active confirmation, one primary, accessible status/alert and phone44px controls. A4 remains pending actual native screenshots; do not generate/modify the supplied pack or copy its mock values into product.

## Current saved identities

| Input | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js (XD01 caller, no BIND caller yet) | 085A96194FCA1921E4BA35D1B3A7E4E71C3137636BC53CA895EE54F17671F0B9 |
| FE js/bpay-next-case-list.js | D67CAE3FE04BB77CBDFBCC7ACE83FAC63F7E9C523AC91C7944A86616E26D1E14 |
| FE js/bpay-next-case-preview.js | 90A38C4CB3C204248F3FE974934E94ED4D8A3A485A1742AA7341E3C1CE3956D1 |
| FE js/bpay-next-owner.js | 4947D2E61CD4FA734CBAA99B05C415BAD8552865D286233123CC48F34E1CA922 |
| BE binder1827 | 33891038C2865684C091F7BD3DF4CE188BB42E026129163216E3BC341FEB198C |
| BE current Case pages0585 | EC0AB1EC0CE95B690F50C711023A6DDA431E562ED075A9716361D1EDE2AAC436 |
| BE original Case create1536 | 853DDCE45455DEFED9F715A2593267794F3B224E469A9EACCDE2AECA6E44A470 |
| BE XD01 sorted reader1452 | 58EEC52DEC8F9DD78ED6AC764A06522784FDB96A3AD4940802D5859D5482E4D6 |
| BE XD01 public dispatch1455 | B92BFE45D52DE9A6B1DCB6FD960DB1D1C1864B186ED05DCE6B48B7CDA07A2618 |
| BE broker command | 2AD2580B7FBF86FD53434E0D54AE48A28125607FE7508D6073897EDBF5819BF7 |
| BE broker command RPC | A1EE88CFC37DEAB4E20FBD109D5F4BD70740B04BDEA0004E6B505C3E3A383BAB |
| Current working checklist | C6F818898EFE286394F13AEC7F16B0E9FBDC32A720D343C3F2566902EB41F3D9 |
| Current final-definer registry | F01B2DECAD4D006E84E457F1B774C21D2668B61EDE72C2E5336CB36F44BA52B9 |

These are read-time source identities, not live metadata/install proof. Parent root separately reports local gates; none were executed or certified by this investigator. Ready for the actual XD02 source/A4 delta review when supplied.
