# WB25 component-choice SQL review

2026-10-11. **PASS — finite SQL source review; F1 closed by new0008 successor. Create final0005 and public count final0008 have no open finding in this reviewed slice.** No whole WB25/app/joined acceptance claim is made.

Read-only source/diff inspection. No database, SQL execution, product runtime, tests, app/data, fixture, network or hosted operation was performed. The builder's reported `db:check` 332/864 is separate evidence, not this reviewer's execution.

| Current source | Raw SHA-256 |
| --- | --- |
| `supabase/repeatable/11102026_0005_banking_pay_next_component_choice_validation_v1.sql` | `115f2619b240a046440d389289bb6f17d1915db52834f86adc01564ab3306a95` |
| `docs/bpay-next/final-definer-closures.json` | `5ad6d7568031eea29fbc15187db728b33724c8d968fa738def46da73a66dd9d2` |
| Create predecessor `10102026_2306_banking_pay_next_actor_selection_memory_v1.sql` | `3f77bbb57500f1320d3873935c8504264a46e50287be9740267f838f8d1dc750` |
| W1 predecessor `10102026_2340_banking_pay_next_selection_count_acceptance_v1.sql` | `545ab73faec8f1331f33986b91ee743340f96be7bcecfb36b8a9c4ed12b38b14` |

## Original0005 snapshot and F1 finding (now closed)

The table and observations above preserve the initial0005 provenance. The current registry and winner hashes are recorded in the closure below.

### F1 — original memory namespace versus narrowed display scope

The new public count absent-Work validation uses `v_ns->'scope'` (0005:636–638 at this snapshot). However `v_ns` is constructed from the current display request scope (608–610), and the already-resolved `v_memory` deliberately retains an explicitly supplied original namespace scope (612). The existing resolver allows an original Candidate-null namespace to be viewed through a narrower Candidate child.

A carried component-only raw override for Candidate B remains valid in the original ALL-Candidate namespace. A HEADER count for Candidate A using that original memory reference and raw intent now fails the new absent-parent eligibility branch because B is not A. The existing request boundary/resolver accepts this original namespace relation; a display narrowing must not turn hidden valid intent into `BPAY_NEXT_WORKBENCH_REQUEST_INVALID`.

Smallest correction: in this new **public count absent-Work branch only**, validate Candidate/Client/channel eligibility against resolved `v_memory->'scope'` rather than current `v_ns->'scope'`. Keep the count/read calculation scoped to its actual current request. Create correctly uses its original descriptor namespace and needs no equivalent replacement. Matching supplied raw Work identity handling remains unchanged. This is source contract parity, not a request for an extra reader/mode/protocol.

## Reviewed finite correctness

- Exactly two function definitions are present: `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid)` and `public.bpay_next_workbench_page_v1(jsonb)`. Extracted definition diffs show only the component-parent validation blocks and their explanatory comments changed from the exact 2306/2340 predecessors. Manifest grouping, original counters and all other body statements are identical.
- Create:215–238 correctly distinguishes a supplied same-Work parent from genuine absence. Supplied Candidate/revision conflicts refuse; non-memory descriptors retain selected SUBSET-parent requirements; memory descriptors preserve matching ALL/false hidden intent. Absence requires memory, actual native Work/Candidate/revision/line, exact current and applied revision, eligible maintained row and parent completeness/revision coherence in original scope. Invalid old revisions cannot fall through to today's defaults.
- Original immutable approved-line identity checks remain immediately before that guard. New native comparisons are plain reads; they do not acquire new locks, publish facts or invoke a financial gate. Current native Work projection sets selectable positive Work rows to PAYMENT/GROSS_ADD; its negative recovery rows are INFORMATIONAL/nonselectable (current Work core1946:209,321 and129/241 in wrapper predecessor1438). The new READY/selectable/AVAILABLE-or-ZERO test therefore does not admit its negative recovery row. No signed SOURCE amount or claim is rewritten.
- Source admission → module → Candidate/Umbrella/Work/revision/Case preholds → command clock ordering is unchanged. Exact eligible component-only rows already belong to the scoped READY parent preholds. The held-membership closure and final workbench/adoption context check remain intact. No post-clock parent lock or new FK is introduced.
- Original pure accepted replay and collision checks are unchanged and remain before the new guard; current eligibility is only required for fresh acceptance. Existing query availability/reply semantics and actor/permission/epoch checks remain unchanged.
- The choice helper and Work manifest are unchanged. For absent-parent component intent, exact current/applied validation makes `l.revision_id` equal its submitted expected revision. Selected genuine native leaves produce ALL or SUBSET exactly as before. Component-only overlays preserve untouched memory/default siblings; explicit Candidate/Work false still dominates hidden subordinate intent.
- Original pay-hold, future-week, Snooze/control-cut, Source query gate, basis availability and signed-line checks remain unchanged. Original ALL/SUBSET work_choice insertion, bounded100 native component pages, original seal/counters, existing segment materializer, once-only captured Candidate settings and PREPARE reservation/empty outcome retain their bodies. No financial owner, command kind, secondary clock or parked F25 mechanism is introduced.
- Public count retains finite request/identity/byte bounds, Candidate-existence check, baseline validation, exact current/applied Work and approved-line identity checks, original counts/cursors/ten read kinds/response caps and typed error envelope. Known invalid identity remains typed request refusal; no fake zero or positive readiness is added.
- Both definitions retain original SQL signatures, volatility/language/security/search-path declarations. The new CASE/EXISTS aliases and boolean branches are coherent with the validated typed JSON shape; current/applied NULL does not satisfy the exact native EXISTS branch. The source closes both dollar-quoted bodies, ACL tail, schema notify and transaction.
- Private Create remains postgres-owned with public/anon/authenticated EXECUTE revoked. Public W1 is postgres-owned, browser roles revoked and service_role granted. The registry points only these two reviewed signatures to0005 with original physical predecessors preserved; the common reader/helper/native component owner winners are unchanged.

## Scope boundary

This new successor is the narrow support documented in `design/WB-25-component-only-review.md`. D12 UI/local-store wiring and joined acceptance remain separate builder work. No schema/memory-marker/helper-resolution change is necessary. Existing accepted RuleB choices/PIN and original component append/seal owners remain authoritative. F1 was the only concrete SQL source finding in the initial0005 snapshot and is closed by the reviewed0008 successor below.

## Fresh0008 closure — F1 CLOSED / finite source PASS

The builder preserved installed0005 and authored a new public-count-only successor. Current source snapshot:

| Current object/source | Raw SHA-256 |
| --- | --- |
| Create final: `supabase/repeatable/11102026_0005_banking_pay_next_component_choice_validation_v1.sql` | `115f2619b240a046440d389289bb6f17d1915db52834f86adc01564ab3306a95` |
| Public W1 final: `supabase/repeatable/11102026_0008_banking_pay_next_component_count_scope_v1.sql` | `f06c42312917da60c70ed9e5b95daafb5f816053075e8a73274a340082f563f7` |
| Current `docs/bpay-next/final-definer-closures.json` | `725d07954151132c095352917ab92bf3045e4acca076b613eef28c4cb4428424` |

Extracted full public W1 definition equality proves new0008 equals its0005 predecessor after replacing exactly the six `v_ns->'scope'->>` operands in the absent-Work eligibility predicate with `v_memory->'scope'->>`. These are Candidate, Client and channel, each appearing twice. No other body statement, declaration, signature, security setting, count/read scope, request validation, native identity check, cursor, response/error shape or lifecycle statement differs. The new source contains exactly one public W1 definition and its unchanged owner/revokes/service grant, schema notify and transaction tail.

The eligibility predicate now validates carried component-only intent against the resolved original memory namespace. The existing count/read still uses the actual current display request scope. Narrowed Candidate reads therefore retain valid hidden original-namespace intent without counting another Candidate or manufacturing selection. This closes F1.

Current registry verified: `public.bpay_next_workbench_page_v1(jsonb)` finalRoot/finalPhysical/finalClosures point to0008, with0005 retained as physical predecessor. `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid)` remains final0005. Its reviewed raw SHA is unchanged. Other component/choice helper/native append/seal owners remain untouched by this successor.

**Final verdict: PASS for the reviewed SQL/Create/count/registry source slice.** No open SQL source finding remains. Actual application/timing/joined WB25 receipts and UI D12 remain separate; this review performed no execution.
