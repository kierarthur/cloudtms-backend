# WB-29 eligible leaf join — independent design review

13 October 2026. **DESIGN PASS — 0 open findings.** Reviewed the corrected `WB-29-eligible-leaf-join.md` SHA-256 `d183403891dad551341e1f160785820c5b84523191230381704a28f267d43798` against current native owners. No product, SQL/RPC, database, fixture, test, browser or hosted action was performed. This does not approve unwritten product or the whole WB-29 item.

The proposed reuse is appropriate: universal existing immutable LEAF capture, original ALL/SUBSET modes and choice matrices, exact segments frozen at accepted Create, one shared private eligibility predicate, and common nonsegment hold admission. It closes the current unresolved-sibling expansion without a new need store, protocol or monetary calculator. The following conditions are necessary.

## EL01 CLOSED — retain native excluded segment provenance

The initial design (`657ac5a7…`) said materialisation and seal enumerate only immutable eligible segments. Current0121 LEAF capture contains READY only. Consequently a natively snoozed or captured query-held sibling would disappear from selected_segment; current2223 segment capture:1073–1078 would turn it into NOT_SELECTED, losing the original SNOOZED state and retained selected query reason. The corrected design explicitly retains the original excluded-state branch and original guard identity checks, closing EL01.

Use two exact branches within the existing ALL/SUBSET component scope: an eligible immutable LEAF authorises effective INCLUDED; the original snooze-at-cut or captured query-exclusion tuple permits the existing excluded member/state only. Require LEAF when `disposition='INCLUDED' AND query_hold_reason IS NULL`. A native SNOOZED member remains SNOOZED, and a query-held member retains its exact original query reason and effective NOT_SELECTED. An unresolved ACTION sibling lacking either original excluded cut or an eligible leaf remains NOT_SELECTED. No excluded member supplies Included target operands. Materialisation, completeness seal and insert guard must use the same predicate; no later Workbench/EX-08/ACTIVE classification is allowed.

## Choice and acknowledgement compatibility

- Current2306 capture casts the optional baseline field to UUID; an absent key yields NULL. Preserve the original `v_memory` flag and its choices branch, and do not call baseline resolve for an absent key. Universal capture must not switch non-memory descriptor choices to memory choices.
- Keep HEADER command/agency binding and acknowledgement gated by the original optional-memory path. Current2306 resolve:63–73 requires an acknowledged HEADER; current2223 final selection seal:761–763 acknowledges only command-bound headers. An absent-key universal HEADER with NULL command/agency remains invisible to remembered-baseline lookup. It can still prove immutable native eligibility against its exact accepted Run/descriptor/context.
- Capture contains every eligible leaf, including unticked units. Selected component/unit scope remains the original descriptor/run_choice_member/selected_component authority. A leaf alone does not select its Work or Case.
- Non-memory SUBSET may seal its original explicitly selected approved-line IDs during Create using the existing page100 owner. Use the original sorted UUID order. Current Office sequencer:1450–1473 already reads SEALED subsets, leaves remaining IDs empty and replays COMPONENT_SEAL with actual counts. Its WORK_PAGE still acknowledges the original mode/revision/member identity. No response shape or journal change is needed for that timing correction.
- CASE-only selections retain their original members and case pages/seal; no fictitious Work is introduced. CASE leaf budget resets by actual case_component_id, not NULL Work/line values.

## Nonsegment and historical truth

Current1236 selected-position ALL branch:2798–2805 intentionally captures every native Position. Current1419 common hold:1303–1308 calculates nonsegment positive Included from residual minus native snooze exclusion. A segment-only check would leave unresolved Additional/Adjustment siblings payable, including no-ref valuer paths. The proposed common hold check must therefore apply to every descriptor-backed nonsegment payment/authorised-zero line: known absent WORK_COMPONENT leaf sets Included=0 and freeze_zero=false before valuation/hold; complete captured A/R/H, collection liabilities and original reservation safety remain intact. Do not repurpose a snooze reason or fabricate a resolution receipt to mark this exclusion.

An exact HEADER and original accepted descriptor/context are required before treating absence as known exclusion. Missing/mismatched historical capture is a typed refusal. The helper must prove Run, Candidate, Work, choice, captured/expected revision, approved line and exact component/segment identity with PAYMENT direction; JSON missing/type/identity mismatches fail closed. Legacy non-descriptor paths retain original behavior. Captured positive INCLUDED members recheck this proof before PINNED or NATIVE_DEFAULT composition; absent proof there refuses instead of silently changing captured monetary operands. Qualified zero remains separately admitted.

## Bounds and lock/order conditions

Streaming is compatible with no new global Draft cap. The proposed 10,000 rows/8 MiB is an explicit per-component supporting bound, not an existing page100 theorem. Reset by exact Candidate/Work/approved_line for Work, actual component for Case; charge complete encoded persisted leaf identity/basis (including repeated namespace overhead), reject the overflow sentinel before insertion/acceptance and never truncate. A live ORDER BY plus no retained giant JSON avoids a browser/server memory aggregate. It does not bound total Draft cardinality or replace the composer's full historical/removed-parent census and separate 10,000/8 MiB working budget.

Universal capture occurs after the original complete parent prelocks and before accepted_choice_descriptor becomes non-NULL, as required by current2306 memory guard:1835–1848. The existing final W13/adoption check and original E2 expected=current=applied guards remain. Capture eligibility at original Create cannot revive a later revision-changed offer. Freeze segments after component choices have SEALED, maintain genuine existing page/seal counters and leave later ACK unable to expand membership.

## Evidence

Current native sources read: 2227 descriptor Create/choice guard;0121 leaves;2306 capture/resolve/memory guard;1832 materialisation;2232 ACK;0454 component append/seal;1236 native segment seal/selected-position keys;2223 segment guard/capture/final seal;1419 common hold; Office native upload sequencer. Exact source hashes for the shared parents are recorded in sibling `WB-29-captured-unresolved-admission.md`. The memory schema SHA-256 is `51cdfb58b668afb18bfa83083d2a14ac3c33b701a6d635feb2b4e592db8c49bc`.

EL01 is closed by the corrected design. All implementation/source/runtime and joined business acceptance remain NOT_RUN.
