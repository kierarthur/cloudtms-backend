# SP-08 caller and interface census

10 October 2026. Read-only source investigation of the full SP-08 card (`05_SOURCE_AND_PROTECTED_PAY.md:305–331`), all seven checklist rows (ITEM, context D01, deliverables D02–D06), and current `SP-08-SCOPE.md` / BD042. No product edits, SQL, tests, fixtures, runtime, hosted or secret operations. This supplement maps actual callers; the plan-required closing record is separately `SP-08-closing.md`.

## Exact scope

The card explicitly says **Worker/Screen: none** and **fix only a confirmed mismatch**. No caller change is needed merely to carry flat provenance: current callers send real approval/publication identities, and private DB owners produce and qualify the origin. D01 is a context row; its first-authorise physical edit belongs to SP-03. D02–D05 verify/reuse existing schema and owner composition; D06 records source closure and the finite errata. Do not add origin fields to a browser request, new route, money calculator, evidence protocol, producer hook or Rule B resolver.

## Public caller boundaries

| Actual boundary | Current authority / transport | SP-08 implication |
| --- | --- | --- |
| Office single/bulk Authorise → `weeklySourceAuthoriseRouting` | `weekly_source_office_authorise_scope_v1` returns the strict two-field applicability contract; indeterminate applicability fails closed. Paid evidence is not consulted. | Preserve existing routing. No origin selection or book binding is done in JavaScript. The existing single/bulk route confirmation belongs to SP-03. |
| Source command `FIRST_AUTHORISE` in `weekly-source/routes.js:105` | Positional `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` receives Timesheet ID, optional expected Timesheet/signature and trusted Office actor. | No financial/origin object is accepted or synthesized. Existing permissions/stale checks stay with the real owner. |
| Protected action `completeLocalDecision` in `protected-action-orchestrator.mjs:285` | Calls `weekly_exceptional_pay_complete_local_v1` with schema, actor, original publication request ID, expected SHA and original `:local-complete` idempotency key; accepts only genuine `PUBLISHED` / `SAVED_PENDING_FREEZE` result and matches returned request ID. | Preserve exact identity/replay and returned refusal. Do not derive an origin in Worker/Office or switch to a C1 path after a refusal. Save-only/pre-authorisation behavior is owned by SP-06/SP-07, not a new SP-08 screen. |
| NEXT consumers | Private `bpay_next_bound_source_snapshot_v1(uuid,bigint)` and initial certificate/bind/assert helpers. | These are owner-only DB interfaces, not browser endpoints. Snapshot REGISTERED / source-book READY / queued position applied are distinct facts. |

No explicit ApprovalOriginV2 arm, `before_origin`, `source_origin_json` or source-generation digest is transported or constructed by the inspected Source Worker/Office modules. Keep qualification in the private codec/context/proposal/publication chain rather than adding a JavaScript mirror of DB provenance rules.

## Current DB interfaces and chain evidence

**Initial first-authorise success tail.** The registry's current physical and final root for `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` is `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`. Its lines 3594–3609 check successful `v_core` and `active_owner='NEXT'`, obtain the genuine `root_authorisation_id`, stage the real source work/revision, publish the exact staged pair, then call `bpay_next_initial_tsfin_bind_v1(v_event)`. This is after the real outer first-authorise core, not inside the reusable core or before successful staging. The registry still identifies the core's physical owner as `26092026_0202`, closed by `28092026_1234`. Never invent a HEAD/Final to supply this typed initial arm.

**D02/D03 real book schema.** The old checklist/staged name `06102026_1245_banking_pay_next_source_book_binding.sql` was re-scaffolded by F-06 as the actual managed migration `09102026_2010_banking_pay_next_source_book_binding.sql`; use that real release identity rather than searching for or importing a duplicate old live migration. It adds original component/publication/receipt identity constraints and private binding tables, nullable native book evidence while UNBOUND, and a finite READY seed shape. The current snapshot signature is `private.bpay_next_bound_source_snapshot_v1(uuid,bigint)`, physically/finally owned by `06102026_1246`. Its closed 21-column result includes qualification/selection, actual canonical/approval/binding/book identities, actual source channel/classification/policy and exact approved/native realised/native held values. Missing ID/book revision refuses; it qualifies the actual NEXT owner and Candidate control. No caller treats missing provenance as zero.

**D04 receipt integration.** The registry's current `private.bpay_next_source_receipt_publish_v1()` owner is `06102026_1301`. It is attached AFTER INSERT on the real private Source publication receipt table. Under NEXT it sorts actual receipt members, stages each real HEAD, validates Candidate scope, acquires the actual binding lockset, publishes the exact staged pair, then registers every current/prior component binding. `REGISTERED` deliberately does not assert POSITION_APPLY completion, book seeding, quotation or capture readiness. No extra Worker enqueue or fabricated publication receipt is necessary.

**D05 producer/consumer chain.** `05102026_0201::weekly_source_local_origin_canonical_v2(jsonb)` explicitly requires the closed Local outer origin. A committed-HEAD `before_origin` has exactly these twelve keys: `kind`, `head_id`, `head_revision`, `decision_bundle_id`, `bundle_revision`, `root_authorisation_id`, `authorisation_generation`, `root_timesheet_id`, `root_version`, `source_generation_digest`, `inventory_digest`, `entitlement_digest`. The initial-TSFIN branch has its separate exact shape. Unknown/mixed/noncanonical keys refuse. This codec is encoding, not financial authority.

`05102026_0212::weekly_source_local_publication_context_v2(uuid,uuid,uuid)` first compares the complete current I1 inventory against the sealed prior inventory and qualifies the actual Local request/generation/family/source evidence. Only afterward (lines 222–224) it removes `source_revision` from a committed-HEAD witness. It does not recalculate the HEAD's `source_generation_digest` over the flattened witness.

`05102026_0224::weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)` reuses the codec/context and compares actual immutable receipt origin/digest, generation, before inventory and complete component vector. `04102026_1257::weekly_exceptional_pay_complete_local_v1(jsonb)` constructs the Local origin using these qualified DB facts; it does not accept a browser-made origin. `04102026_0556` integrates context qualification and inserts the canonical `financial_request.source_revision` beside its unchanged digest only at genuine new-HEAD allocation.

The actual migration `04102026_2137` defines nullable `source_origin_json` as an object bounded to 32,768 bytes, with no update/backfill statement. `05102026_0355` is a one-time cutover migration: its finite old-protocol census precedes activation and never becomes a standing refusal on all future Local publication. Root must record its actual local release-ledger count separately; no ledger or installation read was executed in this investigation.

## D06 closing record and limits

The current `05102026_0114::private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid)` is a STABLE owner-only ReceiptV3 branch. It performs no publication, query, approval, financial, Draft or invoice write. For the committed local publication it checks the original HEAD, stored origin/hash, actual immutable components/receipt/qualification/family event and sealed prior inventory. It flattens the retained committed before-origin exactly as the producer does, then compares it to the stored Local origin and codec result; it does not replace that evidence with today's HEAD/root. This is the concrete Source T-26/flat-origin reader seam to cite in the closing record, rather than claiming a new full query gate. RETAINED_C1 / DIRECT_NEXT and complete sibling census remain distinct owners.

Record the C30 PRIM erratum precisely: “effect NULL only for ZERO” is withdrawn by the real SEED rules. Current 1246 seed proof allows `ORIGINAL_POSITION` with no effect where no positive realised money requires one (for example a genuine held basis); positive realised source requires its actual seed effect, positive held source its actual seed hold. ZERO describes the maintained actual book, not absence of all historical children. Do not fabricate an effect to satisfy the withdrawn shorthand.

The closing document should state: no recursive ancestry or backfill; exact producer/context/consumer/head allocation source lines; real first-authorise bind order; real receipt-to-book identity; distinct binding/position/seed states; 0355 local ledger count if root has current evidence; and future in-app checks NOT_RUN. The three amendment payload-size journey, seam-token second-use refusal, source book first use and hosted/app behavior were not exercised here. Same-transaction token/immutable HEAD guard correctness belongs to the existing named DB owners; do not replace it with a Worker token or producer framework.

## Read source hashes

| Current source | SHA-256 |
| --- | --- |
| Authorise routing | `fec2954202a0a326738937540ed1ef129d8839991d1354f21873ef3e7d445dcd` |
| 2018 first-authorise physical winner | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Actual 2010 book migration | `d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea` |
| 1246 bound snapshot/book owners | `2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340` |
| 1301 receipt owner | `24cf44354735d4a452eb3334847294e486ee17905bdc085f402e77b7ed1d75d9` |
| 0201 codec | `641e417ad4fb2669f5bb59af583e3a18373c80bbf0cb3431c77d4df225aa1812` |
| 0212 context | `209a157af5d7ec1e67568905b5cb828638aab09a11a1d8c4a8ebd59bca8c1d36` |
| 0224 proposal | `bdfcf633afa089d5307ca66b1ea70f51ff7b5b2dba9b086596bd136fb2c4e81f` |
| 1257 Local owner | `2a4f0d619ec2800f3d5a880459d0c0421f41c890bb6706750eb0464cab24fb03` |
| 0556 factual/publication integration | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| 0355 one-time cutover | `0d74a542023102d246fedbf26cceda01c4b88f94c79cf1dbb01eb7eea35be98f` |
| 0114 ReceiptV3 Local evidence reader | `bb6290374ea7857d1ec995565c78267ff9dc761b7d7f4fe05f460ed2903f9b2f` |

No confirmed caller mismatch requiring SP-08 Worker/Office code was found. This is not a whole DB-owner review, release receipt, first-use proof or app acceptance; root's full named DB and closing-note checks remain separate.
