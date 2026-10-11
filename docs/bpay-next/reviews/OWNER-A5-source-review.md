# Owner A5 independent source review

11 October 2026. **SOURCE PASS — no essential defect found.** This is the requested owner A5 boundary correction, not a new plan item or a new acceptance claim. Review is read-only product inspection; only this review note was written. No database, tests, browser, render, fixtures or hosted calls were performed.

Read `docs/bpay-next/design/OWNER-A5-current-investigation.md`, the current saved diffs, applicable repository rules and Banking Pay Bible. The authoritative scope is the owner's A5 direction recorded at the current boundary: sole descriptor CREATE, removal of the old thin manual selection surface, and plain module-unavailable copy. Existing acceptance, journal identity and frozen/sealed behavior must remain intact.

## Sole caller and receipt

BE `broker/src/bpay-next-preparation.js:17,60,273–279,310–335` and FE `js/bpay-next-office.js:42,63,309–326` now use only:

- Input: exact `run_id, command_id, descriptor`.
- Receipt: exact twelve native fields `run_id,status,selection_state,selection_count,selected_candidate_count,official_pay_date,command_id,agency_sequence,choice_work_count,choice_case_count,empty,replay`.

Both obsolete request fallbacks (`run_id,pay_date` and `run_id` alone) and the six-field receipt fallback are removed. Exact shape checking still rejects missing/extra keys, including a caller pay date or actor. The real finite descriptor, its namespace/intent/current revision qualifiers, optional memory baseline and 32 KiB bound are unchanged (BE268–271; FE217 and existing field definitions).

Native UUID matches, integer-string field validation, official date equality with the captured descriptor namespace, empty iff zero selected Candidates, empty iff NULL agency sequence, and selection_count ≤ choice_work_count are preserved and now unconditional for CREATE. No descriptor is synthesized or defaulted. Invalid/unknown receipts retain the existing failure/uncertainty behavior rather than become acceptance.

The actual POST route `BE/broker/src/index.js:198896–198901` reaches the closed preparation handler before the fixed RPC adapter. `broker/src/bpay-next-preparation-rpc.js:7–12` continues to send original validated args with the authenticated actor; it adds no alternative HTTP caller form.

## Original submission and recovery

The only remaining production `mutate('CREATE',...)` in Office is the real `createChoiceSubmission.create` at702, with the original stored run ID, command ID and descriptor.

A static comparison with current HEAD confirmed that the complete `createChoiceSubmission` block and the complete `createController` block are unchanged after line-ending normalization. The following remain the original owners:

- Journal key/version `cloudtms.bpay-next.submit.v1.` plus exact descriptor namespace (581–596).
- Captured descriptor clone, once-created run/command UUIDs, immutable snapshots and existing bounded persistence (583–586,638–651).
- Original pending bytes and SHA, identity recheck after response, and deliberate exact-byte retry (653–659,691–704).
- Native empty/accepted phases, accepted-count checks, same-command SEAL, retained readback and last-run reference (660–687).
- WORK/MATCHING/COMPONENT/SEGMENT/CASE/EXCLUSION upload actions, their original cursors, replay validation and sequencer.

There is no journal migration, namespace/version change, clearing of unresolved bytes, replacement UUID, reconstructed descriptor from current ticks, new request protocol or retry shortcut. This review concerns real descriptor journals; it does not preserve obsolete thin caller acceptance.

## Removed surface and preserved sealed users

FE Office's old contiguous manual selection block is deleted. It no longer renders Choose Candidate, descriptor-free Start selection, raw Resume run ID, raw Run UUID/refresh, the old current-approval checkbox table or Next Work page. The adjacent inaccurate “choices are not saved on Close” and retired thin-view summary are removed.

The unsealed rendering still returns to the actual Workbench at1461–1465. Existing old-only helper definitions are harmless without these controls; authoritative schemas/controller bindings have not been deleted merely because the old surface is gone.

The accepted sealed rendering begins with original Open progress at1476. A static suffix comparison confirmed that this sealed render and all following module source are unchanged after line-ending normalization. Native frozen channel Draft addresses, Review/Confirm, worker/status/frozen lines, accepted PAYE/transfer/cancellation/recovery controls and their existing guards are preserved. No DR15 redesign or Umbrella execution is introduced.

FE `js/bpay-next-workbench.js:640` changes only the displayed MODULE_NOT_ACTIVE sentence to **“Banking Pay is not available for this agency yet.”** Exact error code, serverCode, retryable and reloadRequired are unchanged. There is no LEGACY/NEXT terminology or switch action in this notice. `FE/index.html:3027–3028` bumps both changed Office/Workbench asset versions coherently.

## Existing SQL path and limits

No SQL change is part of this correction. Current public intake is0814, not0454: its descriptor CREATE branch at2079 stores the accepted descriptor through the existing accepted-choice owner. Current0454 Work append at126–127 checks that descriptor first and delegates to existing `bpay_next_ack_choice_work_page_v1`; current2232 v1 delegates to v2's native descriptor/replay/current-open/expiry validation. Real CREATE therefore supplies the manifest required by the unchanged append path. No missing-manifest, late/sealed or replay-conflict refusal has been relaxed.

Existing SQL-only overloads are not claimed removed. The original three-argument private preparation helper is still an internal dependency of real accepted creation; removing it is outside A5 caller closure. Policy X is preserved: no money, eligibility, source/query, economic identity, accepted/frozen artifact, provider, settlement or remittance owner changes.

## Fresh raw SHA-256

| File | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-preparation.js | `18574b10d874f31aec53a4855fe5bf740ccdd022e092ede61961d873a53b3548` |
| BE broker/src/bpay-next-preparation-rpc.js (unchanged) | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` |
| FE js/bpay-next-office.js | `95cbe4f60d8b2da5ad9793b93a823bcc37754e5bc0085732a1ab214b6bfd8bf5` |
| FE js/bpay-next-workbench.js | `7fad175a5dada59a04ea5967d203cbb5e6d6a078f83c8de9b4ccf54507459178` |
| FE index.html | `d3be45262bb5fc974e6f2317184889f70b7c6ab39ddca7794caa07c8f5c3915f` |
| Existing0454 SQL (unchanged) | `d40cc95cb6675f09b1fe06a87801e42a6a28ca1f02a563534ae5e1a65263b5cb` |
| Existing0814 SQL (unchanged) | `29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c` |

Source PASS is independent static evidence only. Root's normal checks are recorded separately; joined actual Create/unknown recovery and UI acceptance remain NOT_RUN here. Item counts are unchanged.
