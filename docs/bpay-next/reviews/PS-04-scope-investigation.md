# PS-04 independent scope/schema investigation

Read-only investigation/design,10 October2026. Full normal R3 card06:1394–1411 and exact current D01/D02/D04/D05/D06 rows read; shared D03 installed1519 baseline is context. Dependency F-05; no Worker/Screen work. No product edits, SQL/runtime/tests/fixtures, database/hosted/network/data calls. Proposed source scope is sound with the exact identity conditions below; no implemented/installed/runtime verdict is inferred from a proposal.

## Existing genuine link and precise gap

Current final recovery owner is `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` in `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:414–490`. It inserts financial_effect with:

- `effect_kind='WORK_COLLECTION_RECOVERED'`;
- `effect.operation_id = actual recovery case_event.operation_id = original settlement/internal job.command_id`;
- **`effect.operation_item_id = actual RECOVERED case_event.id`**;
- exact Candidate, work_collection WORK/component/Case, actual recovery transfer;
- negative of the Case event's source/target ex/VAT/inc amounts; exact original event occurred_at;
- actual WORK original_timesheet_id and original frozen line's conditional Source binding fields;
- `source_case_event_id=case_event.id` only when that actual original run line is Source-bound, otherwise NULL.

Its existing composite unique `(operation_id,operation_item_id,effect_kind)` prevents duplicate same-command effect. It does not alone forbid another operation ID from linking the same event ID. A partial global unique index on `financial_effect(operation_item_id) WHERE effect_kind='WORK_COLLECTION_RECOVERED'` is the smallest schema addition closing that per-event duplicate. Keep the existing composite uniqueness for original ON CONFLICT/replay; the additional partial index does not change the original same-command conflict target. Existing positive replay finds the existing exact effect and does not reapply position or reconcile counters.

Generic effect immutability1342 rejects UPDATE/DELETE and already protects operation/item and every identity/amount. Case-event append-only fact guard0310/1342 also preserves actual event ID/operation/item/instruction/result. The missing enforcement is an insert-time qualifier asserting that the overloaded effect.operation_item_id really points to the correct realised recovery event and its exact frozen origin, rather than a merely syntactically valid UUID. No new link column is needed.

## Correct insert seam and exact qualifier contract

The existing Case owner0410 writes the RECOVERED event at430–437 **before** realising the Case hold/capacity use at520–522, then invokes0316 recovery helper at524–525. A Case-event BEFORE INSERT callback cannot require the not-yet-written financial effect or already-realised hold and would reverse the original sequence. Preserve all eight actual0310 guards and the full original financial owner. The smallest necessary support is one new private BEFORE INSERT trigger/function on `private.bpay_next_financial_effect`, doing nothing for other effect kinds and validating WORK_COLLECTION_RECOVERED from the already-created facts. No Source/public trigger, replacement Case writer or counter mutation.

Finite qualifier reads should bind the inserted effect's operation_item_id to one actual case_event PK, its actual instruction/allocation result, and that instruction's real work_collection PK. Require actual RECOVERED kind and positive source/recovered amount, exact operation/Case/component/Candidate/worker relationship, original event occurrence and exact negative source/target tuple. Bind effect WORK/component/Case/Candidate to the actual work_collection; check original Timesheet against the genuine retained WORK identity, never invent one. Bind event's actual recovery-transfer identity to effect.original_transfer_id. Verify actual realised Case hold/capacity event and the original outcome request/member/instruction/result relationship, using the same event/hold IDs and tuples the current0316 owner already checks. Existing immutable original work_collection links and original frozen line/Source binding give the original paid basis; no current/latest WORK revision or approval is a substitute.

Use fixed PK/equality reads and the actual already-held original-owner state. No aggregate scan, new agency clock, extra business locks, request mutation, cache counter rewrite or new source financial authority is needed. Negative representation of the actual event amounts is the existing effect's accounting sign, not a new recovery calculator. Generic matching of the existing full source/target tuple avoids hard-coding a new permanent PAYE/VAT-zero bridge; the actual current owner remains PAYE-limited and future Umbrella execution is still CM-14.

### Identity distinctions that must remain exact

1. **Case event operation_item_id is the actual Case hold ID. Financial effect operation_item_id is the Case event ID.** They are intentionally different layers. Realisation uses capacity_use.realisation_event_id=event.id.
2. **Current RECOVERED event.original_effect_id is normally NULL.**0410's actual event INSERT does not populate it. Do not demand or manufacture that backpointer. The original payroll effect is already retained on work_collection.original_effect_id and validated independently by its original guard/FK.
3. **Effect/event.original_transfer_id is the actual recovery cash leg.** It is not work_collection.original_transfer_id, which is the original paid WORK leg. Compare each at its correct relationship; do not equate the two to falsely bind origin.
4. **source_case_event_id is an optional Source-bound adjunct, not the universal canonical recovery link.**2010 schema's unbound arm requires it NULL alongside all Source binding fields. Current0316 sets it only if the original frozen line has source_binding_version_id. A universal nonnull requirement would reject correct ordinary recovery and contradict this schema. When supplied, verify it is the same actual recovered event, with the actual frozen bound-line tuple.
5. Instruction/allocation/hold identity is already frozen and typed. Existing0310 validates RECOVERED result amounts and DRAFT/NET stage;0410 validates original member/state/scope, realises holds/capacity and then calls0316. Do not move effect creation to the earlier Case-event hook or replace original evidence with current mutable balances.

## Current original provenance and no-Timesheet boundary

1519 work_collection schema has actual unique original_effect_id, frozen run-line/revision/approved-line, original transfer, Candidate/WORK/component and valuation policy links. Its current1519 guard compares all original fields on every update, pins attached Case/component, and validates genuine original PAYROLL_SETTLED effect/member/hold/approved WORK basis. Those originals are immutable; current observed-position/counter fields are labelled separate caches. New PS04 support does not loosen or rewrite them.

Current work_collection is explicitly original PAYE and its original target line guard is PAYE; current0316 recovery owner requires its original supported PAYE amount shape. Umbrella/moved-origin recovery is explicitly assigned by the card to CM-14 and must be rechecked there; PS04 should not claim current Umbrella recovery execution or jump that writer chain. Its generic one-to-one link support can be reused when the genuine future owner supplies that origin.

A genuine no-Timesheet/manual debt Case retains real Case/component/request/instruction/allocation/hold/event evidence and can have `instruction.work_collection_id IS NULL`.0410 calls the WORK recovery-effect helper only when RECOVERED **and** work_collection_id IS NOT NULL. Financial_effect currently represents actual WORK-position source disposition, not every Case principal event. A Case-only recovery must therefore remain a single real Case event with its typed Case origin, without a fabricated WORK, Client, Timesheet or financial effect. The new effect qualifier must not impose reverse mandatory effect existence on all RECOVERED Case events. Where both representations genuinely exist, they represent one recovery linked by event ID; future readers must not add both as two economic recoveries.

## Exact deliverables and evidence

| Row | Required minimal completion |
|---|---|
| D01 named1519 schema scope | Confirm existing immutable original work_collection schema/guard; add only the partial per-event recovery-effect unique index in a new scaffolded migration as necessary link support. Preserve1519 table and all original FKs; no new origin column or data backfill. |
| D02 eight named0310 guards | Confirm/reuse exact existing actual eight definitions; explicit function-name census finds their sole current definitions in0310. Add only the new private financial_effect insert qualifier support at the correct later seam. Do not rewrite the Case event guard to require a future effect or overwrite its installed receipt. |
| D04 historical0310 baseline | Preserve its saved source and immutable append-only attachments; historical installed receipt is not proof of the new qualifier's installation. No blind reinstall or restoring old writer. |
| D05 future joined checklist |30 recovery once with both genuine facts linked; replay/return/reissue do not recover principal twice; wrong/foreign event/operation/Candidate/WORK/component/amount/time/Source-bound adjunct and cross-operation duplicate refused; genuine no-TS Case-only recovery retains actual origin without fake effect/Client/Timesheet; CM-14 future Umbrella/moved origin boundary; all actual joined clauses NOT_RUN and no fixture/hash programme. |
| D06 exact existing owner verification | Source-only receipt can record canonical0316 event-ID link and replay, both immutable guards, index/qualifier scope, immutable1519 original links, actual0410 event→realised hold/use→effect order, Case-only effect absence and future CM-14 provenance. Later actual installation/app financial check is distinct and NOT_RUN. No new proof framework or unrelated source owner is needed. |

## Raw source checkpoints

| Build repository relative source | SHA-256 |
|---|---|
| supabase/migrations/03102026_1324_banking_pay_next_core.sql | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql | 924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3 |
| supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql | 5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988 |
| supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql | 7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe |
| supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql | d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af |
| supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql | f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8 |
| supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql | 98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6 |
| supabase/migrations/09102026_2010_banking_pay_next_source_book_binding.sql | d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea |
| docs/bpay-next/final-definer-closures.json | df6f1364473ecd77c67285a7a56f13a419478044ab287e5ba1224d26526966d6 |
