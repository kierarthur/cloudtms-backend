# PS-04 recovery-link guard investigation

10 October 2026; independent read-only source. Full card06:1394 onward and exact checklist D01/D02/D05/D06 plus shared D03/D04 reread. F-05 dependency; no Worker/Screen delivery. No product edits, SQL, tests, fixtures, business/runtime, network, secret or data operations.

**Reuse the existing immutable original paid links and financial owners. The evidenced remaining gap is an independently enforced, unique event→effect link for the current WORK_COLLECTION_RECOVERED representation. The actual link already exists in `financial_effect.operation_item_id`; no new recovery writer or link column is necessary.**

## Exact current owners and relation

| Object | Current final source | Evidence |
| --- | --- | --- |
| Original paid WORK collection | migration1519; guards/origin1519 | Original effect, frozen line/revision/approved line/transfer/source channel/tax/policy identities are real FKs and immutable on update. `original_effect_id` is unique; `(work_id,component_key)` is unique. |
| Case event typed insertion | repeatable0310 | For FUNDED/RECOVERED/PAID/SHORTFALL requires actual instruction/result and exact DRAFT/NET stage and allocated amount; schema0300 binds instruction/result/worker/Candidate/Case/component/week/purpose and transfer worker. |
| Case posting | repeatable0410, `post_case_transfer_member_v1` | Exact accepted settlement/internal request, complete original Case/destination build, real CASE member/instruction/result/hold/capacity, matching source/target PAYE values. Creates one RECOVERED event, maintains Case/component/period counters, realises hold/capacity and calls the effect helper only for real work_collection instructions. |
| WORK recovery mirror | repeatable0316, `apply_work_collection_recovery_v1` | Exact event/instruction/collection and leased original settlement owner; requires realised hold/capacity, exact outcome request and CASE transfer member; inserts effect with `operation_id=event.operation_id`, `operation_item_id=event.id`, kind WORK_COLLECTION_RECOVERED and the **negative** exact source/target event tuple. |
| Collection reconciliation | repeatable2018 | Retains original captured basis, maintained actual source position and original paid provenance. Recovers source disposition once; no cash-return inverse or history reconstruction. |
| Effect/event immutability | repeatable1342 | Both tables have BEFORE UPDATE/DELETE attachments to the unchanged `effect_immutable_v1`, which raises for every non-INSERT. |

Case event's `operation_item_id` is the actual Case hold ID in0410. Effect's `operation_item_id` is that event's native UUID in0316. Do not confuse these two native keys. Case event `original_transfer_id` is the settling original transfer. Its instruction/allocation result retain frozen Case provenance; no current Case re-selection is needed.

`case_event.original_effect_id` is **paid-seed provenance**, used by APPROVED/CORRECTED collection events in2018. Several genuine corrections may retain that same original paid effect. A UNIQUE recovery link on that field would be incorrect. Keep it immutable, without treating it as the new recovery effect ID.

The separate2010 `financial_effect.source_case_event_id` is populated by0316 only when the original frozen run line has a source binding. Its FK is `(source_case_event_id,case_id)→case_event(id,case_id)`, but it is not unique. The2010 bound-shape CHECK requires it NULL together with all other Source fields for an ordinary unbound original. Making this field mandatory for ordinary WORK recovery would reject the actual current producer and require an unrelated Source-shape rewrite.

## Existing replay and conservation

Core1324 already makes `(operation_id,operation_item_id,effect_kind)` unique. Current0316 uses ON CONFLICT on that tuple, then checks the full original value/scope tuple plus Source provenance and returns before position mutation. A genuine repeated mirror therefore does not subtract the position twice.0300 separately makes `(instruction_id,event_kind)` unique for realised Case event kinds and retains `(operation_id,operation_item_id,event_kind)` uniqueness. Core constraints, complete stored member sums and actual posting owner remain authoritative; do not build a second calculator or sum event and effect as independent recovery amounts.

The real0410 order is significant: INSERT RECOVERED event while the hold/capacity are ACTIVE → maintain Case/component/period counters → set hold/capacity REALISED and assign `realisation_event_id` → call0316 to INSERT the financial effect. An effect-insert guard can require REALISED witnesses. A Case-event INSERT guard cannot require those later witnesses to be REALISED at its earlier insertion seam. Do not require posting_complete while the MEMBERS page is still progressing.

Returns/reissues use their original cash obligation owners; they do not call this collection recovery mirror to create another payroll recovery. The actual0410 posting refuses reissued transfers as original Case funding/recovery. Retain these owners without a successor under PS-04.

## Narrow missing enforcement

There is no current financial-effect INSERT attachment validating the WORK_COLLECTION_RECOVERED event link. The existing operation/item/kind UNIQUE supports the trusted producer's replay, but admits the same event ID under a different operation ID and does not itself prove a real same-scope RECOVERED event.1342 establishes immutability, not insert qualification.2010's optional Source FK does not close ordinary-work linkage or uniqueness.

Minimum supporting delivery:

1. Add a partial UNIQUE index on existing `financial_effect.operation_item_id` where `effect_kind='WORK_COLLECTION_RECOVERED'`. Event native UUID is globally unique; this closes duplicate representation even with a changed operation ID. Existing actual rows/producers already use this identity. Do not backfill or silently repair conflicts.
2. Add a finite BEFORE INSERT qualifier for that one actual effect kind. Resolve its exact event by NEW.operation_item_id, and verify event kind RECOVERED, positive source/recovered amount, nonnull typed instruction/result and exact operation/Case/Candidate/original transfer/occurrence. Bind the real instruction's work_collection_id to its original immutable collection work/component/Candidate/Case/component and original physical Timesheet. Require the negative signed amount tuple exactly equal to the real event's positive tuple. Where Source fields exist, retain exact original frozen line/binding/canonical identities and `source_case_event_id=event.id`; where the original line is unbound retain the all-NULL shape.
3. Reuse the actual point witnesses already required by0316: hold ID=event.operation_item_id, REALISED hold/instruction/result/worker, REALISED capacity with its exact `realisation_event_id`, accepted outcome request command/transfer/worker/Candidate, real CASE member with that hold/instruction/result, and the original current leased MEMBERS owner if qualifying owner admission at this seam. Current tables/keys suffice; no historical aggregate, fresh settings reader or mutable draft reconstruction is needed.
4. Preserve1342's existing UPDATE/DELETE denial and0310's existing typed event checks/attachments. A small dedicated private qualifier avoids rewriting a shared immutability helper used by unrelated tables. D02 can record verified reuse of its named eight guards when no actual body change is required. No posting, settlement, return, reissue, counter, cursor or money formula successor is justified by this gap.

Exact index plus the finite point guard make the already-existing native event ID link unique and immutable without copying it to another column. Existing private owner/RLS/revoked browser/service exposure must remain. Any supporting trigger attachment belongs in its final controlling repeatable; migrations must retain immutable source/lock ordering.

## Immutable paid and moved-work provenance already delivered

1519 guard:171–184 pins original work/component/Candidate/effect/run-line/revision/approved-line/transfer/source/tax/policy tuple and creation time; once bound, Case/component IDs are pinned.190–225 validates real PAYROLL_SETTLED positive original disposition, frozen monetary tuple, REALISED work hold and actual WORK transfer member.300 onward separately freezes managed collection Case/component identity. These are actual same-scope original facts, not a latest rotated Timesheet/Contract guess. Original work revision and approved-line FK retain the original captured basis; the Source binding extension retains canonical original identity where it exists. Current economic observation may move, while this original paid tuple cannot be replaced.

Do not broaden PS-04 to rebuild that original lineage or to add an Umbrella/no-Timesheet origin. Existing work_collection explicitly admits original SOURCE channel PAYE, and core financial_effect still requires genuine work/component/original Timesheet. A genuine no-Timesheet manual debt recovery can remain represented by its own typed Case instruction/result/event without a fabricated WORK effect. A new mirrored Umbrella/no-Timesheet recovery shape is the explicit later CM-14 owner and its Done-means recheck. It must remain deferred in PS-04 completion notes.

## D06 exact existing-owner check

Source evidence establishes the immutable original1519 tuple, the actual event native ID relation in0316,1342's unchanged effect/event immutability, original typed Case identity, and current trusted-producer replay/conservation. The partial unique link and INSERT qualifier above are still required before claiming the cross-representation guard complete. After root delivery, D06 should check precisely that index predicate/column, the actual same-scope event relation, attachment and unchanged immutability owners; this is not permission for a new fixture/hash-proof programme.

D05 future joined checklist remains NOT_RUN: one genuine recovery30 appears once in Candidate totals while both linked history facts are present; exact replay/return/reissue does not repeat recovery; wrong/foreign/changed operation/item/instruction/result/link/amount refuses; concurrent duplicate event mirror refuses; original paid/moved-work links do not change; a genuine no-Timesheet Case retains its actual origin with no fabricated Client/Timesheet. CM-14 future origin delivery is separately owned. Source inspection does not establish actual installed, Worker/queue/minute or app behavior.

## Raw saved-source SHA-256

| Source | SHA-256 |
| --- | --- |
| migration1519 | `924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3` |
| schema0300 | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| Source binding2010 | `d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea` |
| Case guards0310 | `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe` |
| immutable guards1342 | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| collection guards/origin1519 | `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988` |
| current recovery helper0316 | `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6` |

Investigation complete; supporting root product is not yet reviewed here. No whole PS-04 completion or actual result claim.
