# FC-13: accepted component-resolution consumption

Source investigation, 12 October 2026. Product is read-only for this reviewer. No SQL, business RPC, data, fixture, runtime or acceptance check was run. This supplements `FC-13-investigation.md`; it does not authorize another owner's implementation.

## 1. Exact contract and scope boundary

The owning card is approved-plan `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1290–1314`. Lines1294/1299/1307 require the accepted suggestion/manual total on **remaining selected source basis**, an independent approved resolution receipt for automatic Work collections, and FC-12 future-rule authority for manual scheduled Cases. Lines1298–1300 require the existing shared calculator and unchanged non-tax reimbursements. These requirements cannot be met by overwriting the immutable original Work-collection rule or by repricing an older accepted Draft.

Working checklist FC-13-D01 expressly preserves installed creation/ledger/hold/settlement owners; D03–D05 own the new resolution repeatable, `CASE_CHANNEL_SUGGEST` and ordered `CASE_CHANNEL_RESOLVE`; D07/D08 own transport/UI. D02 and D10 list prepare as **CONTEXT, buildTask:false**, retaining the single plan and captured-input contract. No FC-13 row expressly names the NET staging, Case-transfer member/build/post owners below. Master08:43/61/62 separately places allocation, Case PAYE NET and Case-transfer changes in their approved successor chains; FC-13 is absent from those chains. R2:38 requires FC-13's Candidate-wide financial fence, but does not grant a general channel-economics rewrite of those owners. WB-45's later sequence alone is **not** a blocker to a small FC-13-native receipt.

## 2. Immutable origin versus independent accepted decision

Current `private.bpay_next_work_collection_case_identity_guard_v1()` is `04102026_1519_banking_pay_next_work_collection_v1.sql:300–323`. For a native collection, it freezes Case current-rule identity/revision and component rule/kind/source identity (306–319). Existing component resolution fields can change without changing that original identity. Current collection capture guard in `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:5548–5602` retains the original collection/period/instruction valuation binding. Preserve these guards and all original P/R/W, periods, age and protected holds.

The current accepted Case LEAF in `11102026_0121_banking_pay_next_case_choice_control_point_v1.sql:9–39` captures original Rule ID/revision, component revision, static type/channel and controls. It does **not** capture a native resolution receipt ID/version or its remaining source/accepted target basis. `component_revision` also advances for financial mutations, so it is not an implicit immutable decision ID. The smallest sound decision support is one immutable FC-13 request/outcome association with exact native Case/component/source identity, old/new observed versions, remaining eligible source basis, selected quote mode/operands, accepted target tuple, policy/window, effective date/terms and original command/reason/actor. Native page/input limits and exact replay apply. Capture that exact accepted association at the existing choice cut; later preparation must consume the captured association, never whichever decision is latest when the job runs. This is small card-required support, not a second WB-45 framework or a producer rewrite.

## 3. Existing typed storage is already separate

`04102026_0300` instruction schema stores separate nominal/capacity source and target amounts, source/target/allocation channels, VAT and policy/reference (248–292); holds and capacity-use store source reservation separately from target cash (417–436). Current `private.bpay_next_case_result_insert_guard_v1()` in `12102026_1215_banking_pay_next_case_net_replacement_v1.sql:1817–1861` enforces exact staged ownership and separately caps each source/target amount against its immutable instruction (1853–1857). **It does not require source amount = target amount.** It need not be relaxed just to allow a correctly qualified unequal tuple. Current hold/capacity guards retain exact result-source/target/source-capacity identities (1215:1740–1742/1796).

## 4. Current capture and single kernel do not consume a decision

Both current `private.bpay_next_case_allocation_plan_v1(jsonb)` and `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` are in `12102026_1536_banking_pay_next_case_restructure_v1.sql`.

DERIVE uses the original captured Rule (2661–2689), retains automatic collection origin (2704–2749), refuses Source comparison (2697–2701), and currently refuses source/worker target other than PAYE (2791–2792). It values the **full original approved source** through the automatic PAYE→PAYE helper (2971–2978), returns identical nominal/capacity source and target operands (3024–3038), and uses original collection/Create/edit/restructure references (2982). The writer overwrites component resolution with this automatic full-source PAYE quote at3416–3422. Thus saving a manual100 decision on remaining115 alone would later be overwritten.

ALLOCATE (3049–3100) correctly reuses `private.bpay_next_case_allocation_row_v1(text,numeric,numeric,numeric,numeric,numeric,numeric,numeric)` from0290. It consumes target nominal/capacity/E/H and returns target `taken_amount`; there is no accepted-decision source disposition. Prepare writes that same taken amount into allocated source, target, hold and capacity-use (3504–3527), then source Case/component/period counters (3529–3543). Keep the existing row kernel. A qualified decision consumer must produce both truthful source reservation and target cash from the same accepted remaining-basis quote, including partial takes and penny residuals, while live source balances remain locked. Merely replacing nominal target, or decrementing source principal by target100, is incorrect. No new proportional/inverse formula is justified by these owners.

The shared selected quote `06102026_1157:10–88` already handles SUGGESTED / MANUAL_REPLACEMENT_RATE / MANUAL_AMOUNT and the existing converters. Its legacy component owner passes `remaining_source` as the basis (452;604–636); the automatic helper at932–991 does not preserve an accepted manual decision. Reuse the selected quote once for exact accepted operands, with an explicit source-residual authority; do not quote full nominal again or silently replace MANUAL_AMOUNT with SUGGESTED.

Additional concrete legacy evidence:1157:728–734 derives target ex/VAT/inc per source from the **accepted remaining-basis quote**, and751–754/764 stores those values and `applied_basis_source_amount_ex_vat` in the native selected evidence. This is an existing legacy reference, not a newly invented ratio formula. The native NEXT instruction/ALLOCATE writer above has no corresponding receipt-consumption/source-disposition path. Reusing the legacy reference still requires exact finite remaining-basis qualification and penny residual conservation; it does not authorize scaling an unproven full-balance quote or rewriting a protected allocation.

## 5. Exact downstream assumptions outside the named resolution owner

All signatures below are current registry owners, not proposed or historical owners.

| Signature | Current physical owner and exact restriction | FC-13 scope status |
|---|---|---|
| `private.bpay_next_net_precheck_page_v1(uuid,integer)` |1215:699;723–725 requires source/target/allocation PAYE, zero VAT and equal allocated source/target.|Not named by FC-13;0390 chain is master08:61.|
| `private.bpay_next_stage_case_net_page_v1(uuid,integer)` |1215:756;790–795 repeats same-channel/equal-result/hold qualification;817–824 caps source then writes the same kernel amount into source/target.|Not named; relaxing checks alone would not correct the amount computation.|
| `private.bpay_next_begin_case_transfer_v1(uuid,uuid)` |1215:2899;2931–2934 requires targetPAYE and a genuine accepted NET projection with no pending replacement.|Not named; preserve ordinary readiness/replay.|
| `private.bpay_next_build_case_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` |1419:1960;2057 requires instruction source/targetPAYE and zero nominal VAT. Actual cash contribution already uses targetinc (2068/2074–2077).|Not named;0410 chain is master08:62.|
| `private.bpay_next_case_transfer_member_guard_v1()` |`04102026_0410_banking_pay_next_case_transfer_post_v1.sql:143`;178 requires equal source/targetex and zero VAT.|Not named; original exact membership/state/hold checks remain required.|
| `private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint)` |`11102026_2129_banking_pay_next_case_audit_v1.sql:1565`;1624–1625 requires same PAYE/equal source-target. Source counters use source at1630; event stores source and target independently at1643–1650.|Not named; D01 explicitly preserves settlement owners.|

Stored-credit destination accumulation1215:1887–1892 also has an equal-source/target assertion, but it belongs to its exact stored-credit destination branch; it must not be generalized to every Case or altered without that branch's named scope.

## 6. Narrow conclusion for implementation

Independently safe FC-13 work includes the exact read-only suggestion, immutable accepted decision/replay, expected-version/FC-16/fence checks, component resolution update, manual FC-12 future-authority association, targeted refresh and native confirmation/transport. Existing captured selection support may attach that decision if Root's concrete design establishes the exact accepted-cut seam without changing original identities. Missing/unprovable values stay null with a reason; no new Underpayment producer.

There is a **proved execution-consumption gap**, not proof that every FC-13 deliverable must wait for WB-45. An automatic unequal/channel decision cannot be consumed and settled by the current unchanged plan/prepare/NET/transfer chain. The card's resolved-component Done example is distinguishable from a later paid execution. Do not claim that stored resolution alone governs later reservation/payment. Under the strict named-owner scope guard, the exact NET/transfer/settlement extensions above require their approved named boundary or an explicit owner scope correction; they are not licensed by calling them a small receipt. Root must record the affected incomplete consumption requirement narrowly, and must not widen FC-13 to a second allocator, generic conversion protocol or future owner rewrite.

For manual scheduled Cases, FC-12 supplies an immutable predecessor/request/new-rule lineage and future unopened-period authority, not permission to relabel original source principal/rule identity. A channel decision can reference that lineage and independently retain remaining source→accepted target operands; those operands still face the unchanged consumer gates above.

## 7. Source byte checkpoint

SHA256:1536 `14364DA406573B3031C30E9F2F74148862109EB2C03613E0035B3897B6F0665B`;1215 `23AC3D72354EAC877D66E61E249DE0DB94F1F820BB7F96A90373C5D5425D72BB`;1419 `16E437BB839CC8A91CD499303CD0D6D574797E3FB0DA6570AB24858D42B6CE87`;2129 `CDFD284BF1026875C30B8B472719DC31650E5A4F783E50CE9E241024407C84A8`;0410 `F56898DC7E992FC271D2A90C0C7FDB85D77DDE4BAC5E8B852E008C0DA90793D8`. These are saved-source observations only. All joined/business acceptance remains NOT_RUN.
