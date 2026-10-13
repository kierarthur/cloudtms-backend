# WB-29 current selected quote registry and gate review

SOURCE PASS — scoped registry/gate review; 0 open findings.

Compared final saved changes with HEAD `adb2184c31b241ee658d8e880e3dd2d9c499a2dc`. Final scope is the three private quote owners in 0045 and the existing public workbench reader successor in 0055. Root separately reports local compilation/catalogue checks below; this reviewer performed source inspection only. This receipt does not approve whole WB-29 or runtime/business acceptance.

## Registry

The registry retains the original 1,021 signature entries in their original order and appends two new entries (1,023 total). The existing six-argument input quote changes final owner from 2357 to 0045; 2357 remains allowed and is recorded as predecessor. The existing public workbench reader changes final owner from 2241 to 0055; 2241 remains allowed and is appended to predecessors. The two new private entries have only 0045 as allowed/final physical owner, with empty predecessor lists. FinalRoot/finalPhysical/finalClosures agree for all four owned signatures. No other chain changes.

| Exact signature | Current final physical owner |
| --- | --- |
| private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text) | 13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql |
| private.bpay_next_component_resolution_input_quote_v1(private.bpay_next_component_resolution,uuid,jsonb,numeric,uuid,text,jsonb) | 13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql |
| private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text) | 13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql |
| public.bpay_next_workbench_page_v1(jsonb) | 13102026_0055_banking_pay_next_selected_treatment_estimate_v1.sql |

The four-argument input quote, captured operands gather and captured quote facade retain their 2357 ownership and unchanged registry entries. Public signature membership and public SECURITY DEFINER membership have no additions/removals; only the existing public reader's physical owner is promoted. SQL0045 declares exactly the three corresponding private STABLE SECURITY INVOKER functions and explicit revoke-all for browser/service roles; it does not redefine those retained 2357 entry points. SQL0055 declares only the existing public STABLE SECURITY DEFINER workbench function with its original search_path/timeout and explicit owner/revokes/service-role grant.

## Meaningful gate coverage

The seven-argument shared body and fresh current facade are added to ROUTINE_WORK_SIGNATURES, so the existing final-owner traversal invokes assertRoutineHistoryPoints on their exact physical bodies. They are not inert allowances on typed-only reader entries.

The six-argument existing entry gains only the same-family input-quote callee needed by its explicit seven-argument delegation with final NULL current-working operand. Its prior finite allowances are retained. The seven-argument entry uses the existing captured gather/basis/1157 quote authorities and exact official-date context plus finite native JSON/array/numeric aggregate operations. The fresh facade additionally admits only the existing current active/need classifiers, official-date context and native bounded working-set operations. Active/need resolution occurs in the fresh current facade; no active/need edge is added to the captured gather/facade or four-argument entry.

The shared seven-argument branch refuses simultaneous captured Position and current-working input. The old six-argument delegate passes NULL current-working, preserving captured/scalar dispatch. This was checked only to establish that the gate's new delegation corresponds to the actual declared successor.

The existing public workbench signature was already in both ROUTINE_WORK_SIGNATURES and TYPED_TRANSPORT_SIGNATURES. Its final successor therefore receives the existing routine/callee and typed checks without new public membership. Its exact new callee entry contains five names: original-approval-root helper, current component quote, existing Umbrella VAT calculator, and native upper/array_cat. The first three are the required qualified Source/selected/reimbursement joins; the final two are native operations. No broader financial/history delegation is admitted. Earlier callees remain in the original signature-specific map.

The structural JSON_CONCAT hazard detection is byte-unchanged, with no file/signature waiver. The final 0055 consumed version collection uses jsonb[] plus array_cat/coalesced array_agg and UNION ALL/unnest distinct merge; no direct extracted monetary JSON concatenation is introduced to make the new reader pass. The version comparison/budget semantics are covered by WB-29-selected-estimate-source-review.md.

No new generic callee wildcard, history-reader allowance, dynamic SQL exemption, type metadata map, constructor waiver, public signature list, historical relation exception or workset algorithm change appears in the diff. Existing history relation checks and EXECUTE rejection remain active for both new signatures. The fresh classifier delegates inspected here read native current identities/finite decision metadata; no payment-history/audit-history reader delegation is introduced by the additions.

## Evidence and limits

Read-only saved-source, Git diff and parsed registry inspection. No source gate, SQL load, RPC, database, fixture, browser or runtime test was executed by this reviewer. Root reports final source gates 1552/30/170/37 PASS; local PG17 compilation 0 migrations/5 closures, exact four current body/config/owner/ACL/volatility matches, public catalogue/security 839/76/0 and both DO checks, contract export c807… seal/coupling PASS. These are separately reported execution evidence, not reviewer execution. Economic correctness and complete parent census have separate SQL source receipts; joined business/runtime acceptance and whole WB-29 remain open.

| Saved source | SHA-256 |
| --- | --- |
| scripts/bpay-next-source-gates.mjs | E2D21F4F1406C24718FE941F0B2DEF989B2604E21B165126A61BF5DA3A954874 |
| docs/bpay-next/final-definer-closures.json | E24ACFF2818AC4BD4D1B770C42D148178B1FE7367CE8465EFDF8080B4C99264F |
| supabase/repeatable/13102026_0045_banking_pay_next_current_selected_resolution_quote_v1.sql | 6488C85F18850119D0A64812E3420C7AAC765A4801924AF737E08D9BEA39960B |
| supabase/repeatable/13102026_0055_banking_pay_next_selected_treatment_estimate_v1.sql | 373FC87826C62B88524504B9F3C673A79E75CAC1C55D4746BC6CE3D3B39C4D6A |
