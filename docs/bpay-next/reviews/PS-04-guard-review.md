# PS-04 recovery-link source review

10 October 2026. Independent read-only review of full0604 migration/repeatable, BD034 scope and D05 checklist against exact PS-04 card/checklist and current0316 recovery writer,0410 posting order,0310 typed event/hold/capacity guards,1342 immutability and1519 original-basis guards. No SQL, tests, fixtures, runtime/business, network, secrets or product edits.

**SQL link qualification/index passes source review. One saved-document immutability claim requires correction or its two missing attachments before the full documented delivery can pass.** No new financial-owner rewrite is required.

## PS04-01 — event/effect TRUNCATE assertion exceeds current attachments

BD034 scope says existing full append-only guards protect both facts from UPDATE/DELETE/TRUNCATE, and D05 repeats that TRUNCATE remains forbidden. Current1342 attaches `effect_immutable_v1` to financial_effect and case_event only BEFORE UPDATE OR DELETE (lines255–262). No current NEXT source attachment rejects TRUNCATE on those two tables.1519 does have a separate collection BEFORE TRUNCATE attachment, and ordinary browser/service table privileges remain revoked; neither establishes the claimed event/effect owner-level statement-trigger refusal.

Close narrowly by correcting the saved assertion to the actual UPDATE/DELETE and ordinary-role privilege boundary, or by adding two BEFORE TRUNCATE FOR EACH STATEMENT attachments on these exact two tables in the final controlling0604 repeatable, reusing unchanged `private.bpay_next_effect_immutable_v1()`. That helper already raises for every non-INSERT operation, including TRUNCATE. No new helper, body, business counter or producer is needed. Do not call an uninstalled new attachment an existing applied baseline.

## Passing SQL boundaries

- The migration's partial UNIQUE index is exactly financial_effect.operation_item_id WHERE kind=WORK_COLLECTION_RECOVERED. This is the real existing link native event UUID, independently of operation ID. It leaves the original operation/item/kind arbiter and actual0316 replay path intact and rejects duplicate mirrors rather than repairing historical rows. No duplicate identity column, FK fabrication, backfill or exposure is added.
- The INSERT qualifier resolves event by NEW.operation_item_id, verifies RECOVERED and positive source equal to recovered_delta, and binds instruction→work_collection→real work. It checks original valuation policy, exact operation/Candidate/Case/component/original transfer/physical Timesheet/occurrence and the full negative signed source/target tuple with null-safe distinct comparisons. Existing0300/0310 typed FKs and guards bind the event's result/instruction/worker scope; the new REALISED witness query binds exact hold, result, worker, capacity realisation-event, actual accepted outcome request and immutable CASE member. No null or foreign-scope admission hole was found in these predicates.
- Timing is correct:0410 inserts the event while hold/capacity are ACTIVE, maintains Case/component/period, realises hold/capacity with event ID, then calls0316. The new qualifier attaches only to effect INSERT after those witnesses exist, and does not demand a later posting_complete. It adds no lock, tail or monetary mutation to the existing owner.
- A matching current NEXT module epoch, Candidate owner epoch and leased settlement/internal MEMBERS job are required. The exact original accepted command/transfer tuple is checked independently; the owner query does not choose a current revision, draft or latest instruction. Other effect kinds return immediately and preserve their original writers.
- Source witness reconstruction exactly matches0316's conditional six-field tuple from the collection's original frozen line. For unbound ordinary originals it requires all Source fields, including source_case_event_id, NULL. For bound originals it requires the retained line/binding/canonical identity/channel and event native UUID. No2010 shape relaxation or invented original Timesheet is introduced.
- Existing1342 UPDATE/DELETE denial and1519 immutable paid-basis guards remain unchanged. The qualifier is private SECURITY INVOKER with fixed pg_catalog/private search_path, logical ownerpostgres and revoked PUBLIC/anon/authenticated/service_role execution. DROP IF EXISTS/CREATE attachment is repeatable; no public trigger or role grant is added. The new non-INSERT refusal is defensive but the actual attachment is INSERT only; it does not replace1342.
- Registry adds the one new private guard final0604 and leaves named0310 family,1342, actual0316 recovery writer,0410 posting,1519 basis and2018 reconciliation finals intact. No editor-chain jump, second calculator, count/cursor change or financial-owner successor was found.

## D05/D06 scope

Apart from PS04-01, the saved checklist accurately covers one linked recovery30, wrong/foreign typed identities/amount/occurrence, concurrent duplicate native link, replay before another position mutation, immutable original paid/moved-work provenance, return/reissue conservation and write-off distinction. It keeps all actual installed/app/business/full Worker/queue/minute checks NOT_RUN. Manual/no-Timesheet debt may retain only its genuine typed Case origin; CM-14 explicitly owns future Umbrella-origin mirror shape. No fabricated WORK/Client/Timesheet, report aggregator or whole-card app acceptance is claimed.

The exact D06 existing-owner confirmation can establish this new index/qualifier linkage and the reused current guards from source once PS04-01 is resolved. It cannot substitute for installed catalogue or the actual joined Candidate-total result.

## Raw saved-source SHA-256

| Source | SHA-256 |
| --- | --- |
| migration0604 | `cf4e57122b7ba503aa724f21dd42218d9d807461d32f033b8a20b2bb229e48d7` |
| repeatable0604 initial candidate | `2c0f093c8d900899b61a40b4316757958c20246d4c197001c96674bbd7785cb8` |
| BD034 scope | `0ec998684a420aa991e55c3e03867df7e9e41f682203308877106ce6c67cd2f1` |
| D05 joined checklist | `ef6764423ef5fc2245923ab1f21e54b6a56eb33cd2b337fbec2783e3e919094e` |
| final-definer registry | `df6f1364473ecd77c67285a7a56f13a419478044ab287e5ba1224d26526966d6` |
| retained writer0316 | `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6` |

No reviewer execution or product edit. Source-only findings do not claim deployment, actual linked totals or future CM-14 completion.

## Fresh final source closure — PASS

Reread the corrected full BD034 scope and D05 checklist and inspected the exact registry diff. PS04-01 is closed by accurate description: event/effect UPDATE/DELETE denial and revoked ordinary-role authority are retained; only collection1519 has the existing TRUNCATE attachment. The corrected docs do not claim event/effect owner-level TRUNCATE refusal or introduce unrelated protection. This is the appropriately narrow resolution within the current card's link delivery.

Fresh hashes confirm both reviewed SQL files are byte-identical to the passing candidates above. Registry diff now contains only the new private qualifier entry, preserving all existing order/final definitions/editor chains. Final source verdict: **PASS**, with no open concrete findings for the reviewed PS-04 index/qualifier, reused original-owner boundaries and D05/D06 source scope. Installation and actual joined acceptance remain separate, unperformed evidence; CM-14 future origin delivery remains deferred.

| Final saved source | SHA-256 |
| --- | --- |
| unchanged migration0604 | `cf4e57122b7ba503aa724f21dd42218d9d807461d32f033b8a20b2bb229e48d7` |
| unchanged repeatable0604 | `2c0f093c8d900899b61a40b4316757958c20246d4c197001c96674bbd7785cb8` |
| corrected BD034 scope | `0d7c6976e5ae9c3600feaf4ae6386e1c304f1ee06a27e64b3ee6802ac2ae0eb4` |
| corrected D05 checklist | `3dcca9195f73591c6bf3d6e405e226d0f7367983b5914cd2aec775bb4921bd2c` |
| one-entry registry addition | `3a287412ccf38235bd500987c25870531d071569da2f4eaa4224b78111e16c9a` |

No reviewer SQL/runtime/tests/product edits.
