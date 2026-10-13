# WB-29 collection root identity substitutions

Read-only design assessment of the current saved sources, 12 October 2026. **The five existing collection functions below require narrow Source identity support under BD155. No collection formula or financial admission expansion is recommended.** This is source/design evidence; SQL, business, joined and runtime acceptance were not run.

## Identity rule

Use `private.bpay_next_treatment_approval_revision_v1(uuid)` from `12102026_2135_banking_pay_next_treatment_native_publication_v1.sql:5–33` for a positively qualified treatment only. Its direct same-Work original approval, immutable native revision payload, sealed predecessor chain and genuine one-member POSITION_APPLY command/publication are the authority. The seal-time complete line/rate/schedule/shift/break/segment payload assertion is at :36–76. Do not use the nullable root pointer alone, search approval history, infer a root from a head, or follow another treatment as a root.

For an existing native revision without a treatment link, retain its original identity and predicates. For a linked revision, resolve the helper and join its same-Work root as an additional Source-authority alias. Use that alias only for the existing Source/PROTECTED event/head origin predicate and original Source binding approval/line identity. The actual revision aliases remain separate. A missing or invalid treatment proof must not fall back to another approval.

The original approved Source line, when required by a canonical binding join, is the root's exact `component_key` line with unchanged source-component identity, kind, tax and native payload. Never overwrite `run_line.approved_line_id`, `work_collection.original_approved_line_id` or `original_revision_id`: those are immutable actual paid anchors. A REMOVED component has no current approved line; retain its existing classification line and explicit REMOVED proof, rather than inventing a root line or a zero.

## Exact current successor minimum

All paths below are under `supabase/repeatable/`; line anchors refer to the inspected saved parent, before root's new successors.

| Function and exact parent | Required identity substitution | Keep literal |
| --- | --- | --- |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)`, `12102026_1849_banking_pay_next_manual_collection_retained_approval_v1.sql:530` | At :637–645, qualify the **captured** revision's Source origin through its root; retain actual `v_approved` component facts. At :751–762, qualify the paid-original and applied-current Source origin predicates independently through their own roots. At :789–813, bind the Source book to the applied revision's qualified approval root and root event/head, while separately checking the actual treatment publication/current job. | Paid effect/run line/transfer/hold tuple :605–622; paid anchor insert :716–727; native current/applied readiness :774–785; Position, lease and exact job :555–577; observation argument and mode :814–822. |
| `private.bpay_next_work_collection_observation_v1(uuid,text,uuid)`, `12102026_1934_banking_pay_next_removed_source_collection_observation_v1.sql:6` | At :56–107, use qualified original/current Source aliases for raw event predicates; compare `bv.approval_revision_id` and `bv.publication_revision_id_1` with the qualified **current approval root**, not observed treatment ID. Keep the genuine binding publication joined to those immutable operands. At :111–129, make the same root/event substitution in common-HEAD qualification and independently prove the actual observed treatment publication. | Initial current/applied/withdrawn-retained proof :21–30; original captured line :31–33; PRESENT component/channel/tax/current amount :42–52; explicit REMOVED state, null approved line, classification facts, receipt/bundle/head/auth/one-publication scope :70–107; all segment and nonsegment arithmetic :130–173. |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)`, `12102026_1822_banking_pay_next_classified_work_collection_v1.sql:115` | Add root aliases for actual paid `original` and applied `r`; only replace raw Source/PROTECTED event qualification :145–153 with the corresponding root predicates. | Original paid line :125–128; observed Position equality :136–140; actual applied line/segment/zero checks :154–163; PAYE/tax/Case/rule/financial contribution and balance guards :164–176. |
| `private.bpay_next_work_collection_guard_v1()`, same `1822` file :554 | Only the original paid Source origin predicate at :618–622 needs the qualified root alias for the actual line's revision. Preserve existing manual Source admission :617 and all other original branch conditions. | Immutable paid identity :564–603, actual realised hold :604–607, settlement/outcome/transfer-member proof :623–634, actual leased job/publication tuple :654–699, literal observed Position :702–705, segment/manual assertions :708–724. In particular :664 must still match `publication.revision_id=new.observed_revision_id`, not root. |
| `private.bpay_next_case_allocation_plan_v1(jsonb)`, same `1822` file :873 | Repeat the exact origin helper's root-only Source qualification in its existing inline managed-collection lookup :987–1018. This existing duplicate read predicate must not be left raw while the writer/helper is qualified. | Full original stable lookup, actual paid line, actual Position/current line/segment identities :989–1028, existing Case/rule eligibility, all mode parsing, principal, age/order, floor, capacity, allocation and instruction/result formulas. |

These are **five function successors from three physical parent files**, not whole-file adoption of old definitions. The observation body embedded in 1849 or 1822 is superseded by 1934; the reconcile body embedded in 1822 is superseded by 1849. Preserve final-definer provenance accordingly.

## Two publication operands are essential

The reconciliation common-HEAD block currently joins `publication` to the immutable binding's approval revision (:798–799), but also requires that publication to be the current POSITION_APPLY command (:810–812). After a treatment clone those are different genuine publications. Merely changing `v.approval_revision_id=v_position.applied_revision_id` to root equality leaves the job comparison false.

Retain the binding publication's command/Work/Candidate/revision relation and genuine Source receipt/bundle/head/authorisation checks. Add an actual-publication alias for the exact applied treatment revision, same Work/Candidate. Keep `v_work.current_revision_id=v_position.applied_revision_id` (the native comparison currently implied at :803/:809); do not turn it into a comparison with the root. Apply the existing current job condition to the actual publication: POSITION_APPLY must match the real job command and QUEUED/APPLYING; the later-owner branch must see actual APPLIED/DONE. For a treatment, the old binding publication is already applied; preserve that retained evidence rather than relabel its command as the treatment command. For a native revision the two aliases resolve to the same publication and retain original behavior.

Observation has its own original status contract, QUEUED/APPLYING/APPLIED at 1934:107/:129. Keep that contract for its genuine binding evidence and independently require the actual observed publication in the same allowed states. Do not globally substitute the root revision in publication/job or Position joins, or harmonise different functions' existing status contracts.

## Money and E2 boundary

`12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql:200/:253` invokes reconciliation for each applied and removed component. No change to apply is needed: without the qualifications above, an unchanged sibling's retained Source liability can become CURRENT_SOURCE_UNBOUND and then MANUAL/AMOUNT_UNBOUND despite unchanged money.

Leave the actual approved line and segment monetary basis on the treatment revision: its native payload was cloned exactly and its own seal regenerated native segments. `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4076–4141` contains no raw Source event/book equality requiring this substitution. Keep those native balance/collection helpers, including their original ordinary-only admission, unchanged. Do not make Source automatic collection cover new component kinds or broaden the old no-effect paid-anchor fallback at 1849:705–714.

Preserve every original R/W/H contribution, source/target/paid basis, principal adjustment, event, active recovery, collection-mode/manual amount, capacity and captured-floor formula. Preserve certified-zero, explicit REMOVED, withdrawal and unknown-amount distinctions. No reset/reseed of a READY Source book, paid anchor rewriting or historical amount reconstruction.

This bridge does not replace expected/current/applied treatment pointers used by accepted Create, capture, claim, hold or job ownership. A Work revision changed after the click remains held back/system-released under Rule B/E2; immutable root qualification never revives that accepted offer or reprices it.

## Inspected source hashes

| Source file | SHA-256 |
| --- | --- |
| `12102026_2135_banking_pay_next_treatment_native_publication_v1.sql` | `56D91E37EDE376018C72245EE02FD1D30237F987C656754B2FD0EC668582DDC1` |
| `12102026_1849_banking_pay_next_manual_collection_retained_approval_v1.sql` | `DE67314666B76892EF6B4AC25554FA44E7F7A89A96068EABE6B7EA9A3B79A830` |
| `12102026_1934_banking_pay_next_removed_source_collection_observation_v1.sql` | `C52B03C259630BFED375CD3E21E6E25EE0668B5A1A64E73B42979198787C3020` |
| `12102026_1822_banking_pay_next_classified_work_collection_v1.sql` | `B9650E9DA89A3DCA3B59BC33F73EFC8ACF132038DAE163DF826D1F18730990FD` |
| `12102026_0016_banking_pay_next_stale_snooze_publication_v1.sql` | `2D9822DA9881179DACE5A2FF97C65FCAAF1DFBAAD920337A4701133D8FE487AE` |
