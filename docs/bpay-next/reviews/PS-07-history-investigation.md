# PS-07 history scope investigation — 10 October 2026

Read-only source/design investigation. No product source changed; no SQL, runtime, app, fixture, test, hosted or network operation executed. This note establishes the required source scope, not installation or in-app acceptance.

## Conclusion

The current final `public.bpay_next_payment_history_page_v1(jsonb)` in `supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql` already meets PS-07's state-independent, captured-identity behavior for every presently supported section. No missing current subtype, Payment Advance field, detail link, or readiness/current-revision predicate requiring a behavioral hunk was found. Preserve the existing body and both caller modules.

The card expressly requires PS-07 to occupy the first successor position. A reviewed controlling successor that recursively includes the exact original 1206 physical body and adds an accurate function COMMENT/chain record is a proportionate first successor. It must remain an actual reviewed final controlling closure, not a claim that new behavior was added. Register its `finalRoot` as the new root and `finalPhysical` as the preserved original 1206 definition; retain the original as the allowed physical definition and keep the chosen root pending whenever its included physical source changes. EX-41 subsequently re-reads this closure; CM-18 subsequently re-reads EX-41. Neither needs a duplicate competing body now.

## Exact scope and deliverables

Plan `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1457–1478`, BUILD-BE checklist `IMPLEMENTATION_CHECKLIST.md:773–793`, and `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:71,115,293,355` prescribe sequence 31, dependencies F-06/F-08, and history chain **PS-07 → EX-41 → CM-18**. F-04's chain rules require preservation and one designated controlling closure.

| Row | Required source scope | Investigation/result |
| --- | --- | --- |
| PS-07-D01 | `public.bpay_next_payment_history_page_v1(jsonb)` captured historical sections | Existing 52-section body correct; create first successor closure and accurate provenance/comment rather than duplicate or invent behavior. Preserve installed original body and existing ACLs. |
| PS-07-D02 | `broker/src/bpay-next-frozen-read.js::handleBpayNextFrozenRead` REVIEW/DRAFT guard scope | Source-confirmed: this route delegates only its four Draft detail kinds to the existing guarded frozen-detail service/private helpers. Keep these guards. History has a separate route and reader. No Worker rewrite required. |
| PS-07-D03 | `broker/src/bpay-next-history.js` exact captured-section caller | Source-confirmed: calls only current public history RPC, supplies authenticated actor, validates exact 52-section manifest/cursors and bounded string/null fields. Reuse unchanged. |
| PS-07-D04 | Future joined in-app TEST checklist | Author finite clauses for paid/cancelled detail while current worker not READY; old paid revision after later approval; genuine Case instruction provenance; Draft mutation READY guards; foreign scope denied. All actual app/Worker/queue/minute-tick execution remains NOT_RUN. |

No Screen, agency Summary, advice expansion, new reader, read session, snapshot protocol, extra field/column migration or new scenario programme is required by this card.

## State-independent current reader and captured detail links

1206:134–164 checks the service JWT and active admin. It does not check module owner, pay-run REVIEW/DRAFT, run-worker READY, or current approval/workflow state. 168–179 resolves a durable Timesheet through exact unique original Timesheet/booking WORK identity. 234–314 verifies exact Candidate/WORK ancestry for each declared parent, never a bare foreign UUID.

The only `current_revision_id` use in the history SQL is the WORKS explicitly **CURRENT** field declaration at line 70. It is not a join/WHERE selector for historic detail. Current CASE/component/collection/control counters are likewise labelled current caches, not paid authority.

1206:81–83 emits immutable RUN_WORKS `captured_revision_id`, RUN_POSITIONS `captured_revision_id` + `approved_line_id`, and RUN_LINES `captured_revision_id` + `approved_line_id`. Exact REVISION/LINE/SHIFT ancestry follows sealed retained revisions (237–248), not WORK.current_revision_id. LINES/SHIFTS/BREAKS/RATES/SCHEDULE then read their actual retained parent keys. POLICY reads the actual frozen line's policy/window (253–256,320–321), not live finance settings. The source permits sealed prior revisions as well as successors; the source graph lets the caller choose the paid capture's revision ID.

RULES/INSTRUCTIONS retain the actual rule ID and revision; RESULTS are children of the captured instruction. CASE_EVENTS retain instruction/allocation/worker/operation identities and original transfers/effects. Candidate scope includes genuine zero-WORK Case workers through COMMAND_WORKER and INSTRUCTIONS. A Timesheet-scoped Case-only worker uses the exact supplied instruction ID and existing work_collection ancestry (298–305); it does not fabricate a Timesheet or allocate a whole-worker/whole-transfer amount to one Timesheet.

The reader fixes an upper key per section/parent and returns explicit `LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT`. It is neither an across-request MVCC snapshot nor a whole-agency financial total. Exact key casts, limit+1 bounded fetch, per-row byte charge, final envelope cap and terminal/cursor rules remain 330–405. Retain these facts in the successor COMMENT without describing them as stronger snapshot authority.

## Present subtype / Payment Advance coverage

The actual schema `04102026_0300_banking_pay_next_case_instruction_core.sql:22–53` distinguishes Case kind and subtype:

| Actual kind | Actual subtype(s) | Historical retained source already exposed |
| --- | --- | --- |
| LOAN | LOAN | RULES, COMPONENTS, INSTRUCTIONS and CASE_CREATE where applicable |
| ADVANCE | PAYMENT_ADVANCE | Same retained rules/instructions and immutable approval receipt; no invented `advance_kind` field |
| OVERPAYMENT | OVERPAYMENT | Actual captured rules/components/instructions/events |
| CREDIT | UNDERPAYMENT / MANUAL_CREDIT | Actual captured subtype and taxable/non-taxable identity |
| MANUAL_DEBT | MANUAL_DEBT | Genuine Case origin, including no-Timesheet Case instructions |

1206 RULES (97) includes `case_kind,case_subtype,tax_treatment,case_created_at_utc,original_funded_at_utc,original_funding_event_id,schedule_start_monday,schedule_week_count,weekly_due_source_ex_vat,minimum_earnings_threshold,take_home_floor_override,next_due_monday,captured_at_utc`. This already includes actual original funding and schedule evidence. Original funding's nullable time/event pair is real schema semantics, not missing evidence to replace with today's values.

COMPONENTS (96) and INSTRUCTIONS (99) include case_kind/subtype, instruction kind/direction/stage, source/target channel, rule/revision, amounts and original valuation/resolution refs. CASE_EVENTS (98) includes actual event type, operation, instruction/result, hold purpose, week and source/target amount tuple. CASE_CREATE (112) includes immutable command/actor/Case/subtype/treatment/principal/approval reason/rule/components/schedule/valuation identifiers and acceptance time. CASE/CASES (95,114) are current caches; deriving subtype through current_rule_id for a historical paid instruction would be wrong, but the existing reader does not do that.

The `04102026_0320_banking_pay_next_case_create_request.sql:15–47` approval receipt already uses `ADVANCE` + `PAYMENT_ADVANCE`. This task does not add schema aliases or rename Case types.

## Missing move data belongs to later owner

1206 MANIFEST (197–199) truthfully lists `MOVED_ROOT_FLAT_PAID_BINDING`, `CANONICAL_MULTI_CHANNEL_VECTOR`, `MOVE_COMPARISON_RECEIPT`, and general approval actor/reason not captured in NEXT rows as unavailable. The full CM-18 card at 06:750–774 explicitly owns the new move sections after EX-41, including comparison receipt, selected quote, flat binding member, CM-08 decision reason and actual old/new Contract/rates/outcomes. Its actual dependencies are CM-02/CM-08/CM-09/EX-41/F-06/F-08/F-16. Do not promote imported comparison references, infer latest revisions, fabricate old labels, or pull that complete move reader into PS-07.

PS-07's 'extend only missing subtype/move data' limits legitimate needed hunks; it does not require inventing a present field gap or jumping CM-18. Supported current subtype/provenance is already exposed. Future move sections remain explicitly unavailable until their owning chain supplies real durable facts.

## D02/D03 route and privacy evidence

`handleBpayNextFrozenRead` (331–380) authenticates admin, validates its finite SHIFTS/BREAKS/RATE_DETAILS/RATE_SCHEDULE request, and calls only the frozen detail service. It contains no history dispatch. `public.bpay_next_frozen_detail_page_v1` in 0250 authenticates service + active admin (24–29), requires NEXT for this operational Draft route (30–32), and dispatches only four kinds (33–76). The actual private frozen-line guard in 0025:21–31 checks pay-run REVIEW/DRAFT and worker READY; rate schedule has the same guard at 162–171. Shift v2 uses that same frozen-line guard. These Draft-specific guards must stay intact; PS-07 does not remove them or make Draft mutation state-independent.

`createBpayNextHistoryRpc` (history.js:234–245) calls `/rest/v1/rpc/bpay_next_payment_history_page_v1`, supplies its validated Office actor in the service request, and uses the existing bounded route class. `handleBpayNextHistoryRead` (248–274) only authenticates admin, validates bounded request/result and returns truthful errors. It does not call the Draft detail helper, current-readiness route or money command. index.js has separate history-page and frozen-detail branches under authenticated NEXT Office routes; their environment check is a deployment boundary, not a WORK/READY filter.

The SQL and Worker registry literals were compared by static JSON source inspection: **52 SQL entries / 52 Worker entries; every parent, key tuple/type, field list and amount_scope matches**. This was source inspection, not executing application tests or queries. Preserve the exact finite manifest while no new field is added. Source refs are IDs/hashes only; SOURCE_DETAIL does not expose raw payload, and every output data scalar stays string/null.

## Review/acceptance boundaries

This source investigation found no concrete body defect requiring a PS-07 behavioral successor. A closure scaffold must retain exact original definition/owner/grants/schema reload and integrate current final-definer metadata; its concrete saved bytes still need independent review after authoring. Preserve all unrelated dirty work and earlier permanent reader/caller changes. Future EX-41/CM-18 must re-read this chosen closure and update the one controlling root, never fork a parallel history reader.

Installation/readback, paid/cancelled app interactions, Draft rejection and foreign-scope denial have **NOT_RUN** status in this investigation. Reuse the existing finite source gates and explicitly named normal units; no new fixture/hash proof programme.

## Current raw source SHA256

| BUILD-BE relative path | SHA256 |
| --- | --- |
| `supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql` | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| `supabase/repeatable/04102026_0025_banking_pay_next_frozen_detail_pages_v1.sql` | `d7433fd91345a02ea75a596577c66514465c007d622dd0d225ca7edf96708ce9` |
| `supabase/repeatable/04102026_0250_banking_pay_next_frozen_detail_service_v1.sql` | `ca6e50fcb6324e9e3a39e840a664ddfa53a7a7d96d9d0fb9859d65743614e091` |
| `supabase/repeatable/04102026_1134_bpay_next_frozen_shift_page_v2.sql` | `7a65a389a1fc34cd06f7a1ba7d9fb4b448bb50b03ba77c807ffa11a6b56eff48` |
| `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` | `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e` |
| `supabase/migrations/04102026_0320_banking_pay_next_case_create_request.sql` | `870ff18f1d7d9af464c19f0138da2bc4438ec1953dc82800653069cdd53e042b` |
| `broker/src/bpay-next-frozen-read.js` | `3c7d75b485db6824a38000d580eb96c37864c01a61fdaf114d5551637e10c3b2` |
| `broker/src/bpay-next-history.js` | `b5bb109e546787fc65a83df1916dfd29c1b21d32ae6372fd3fd04ae97baa0d50` |
| `broker/src/index.js` | `9610a75e0cd0d83f023a050104f7fad3de3995a5592b1b26ea9b4e3bfadd9fa3` |
| `docs/bpay-next/final-definer-closures.json` | `3a287412ccf38235bd500987c25870531d071569da2f4eaa4224b78111e16c9a` |
