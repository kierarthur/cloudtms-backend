# WB-28 four-definition memory SOURCE review

13 October2026. Independent read-only review of unapplied13102026_0721_banking_pay_next_frozen_decisions_clear_v1.sql against scoped reset design, exact current choices2306 and capture0016 parents. Root alone writes product. No business/SQL/RPC/runtime/tests/database or product edits. **Scoped SOURCE PASS for the four current memory definitions.** Marker migration/schema/operation admission and whole WB28 are pending separate review; this is not an installation/whole-card/A4 claim.

## Finding and closure

**M28-01 — inherited NONE erased partial reset/recorded selection — CLOSED.** Initial SHA5CE9C6B5... had p_intent.mode NONE override both reset and recorded boolean. Current saved defaults53–58 preserve the original detached-null-baseline mode branch and changed-unit behavior. Matching reset now returns true unless NONE is a genuinely different explicit mode from baseline HEADER; recorded boolean is preferred only for absent/inherited-equal mode. A different explicit mode falls through to the original mode/exclusion branch. This fixes broad NONE + A-only reset + captured A=true/B=false across later Create, without changing typed fresh NONE or overriding actual local Candidate/Work/component/Case choices. Original global Set NONE detaches the baseline in current Office; no new descriptor flag or protocol is introduced.

## Current four definitions

- private.bpay_next_selection_remembered_unit_v1(bigint,bigint,boolean,boolean): all-NULL historical flags return NULL; a known unchanged unit requires complete recorded leaf count and equal nonnull booleans. Contradictory/partial known unit flags raise exact23514. The actual sole caller passes native COUNT/bool_and/bool_or facts from the unit grouping; changed-unit branch retains original true behavior instead of treating missing new leaves as invented false. No pricing or browser arithmetic.
- private.bpay_next_selection_reset_matches_v1(uuid,uuid,uuid,uuid,uuid,uuid): marker authority joins baseline acknowledged HEADER agency/epoch/actor/official-date, requires later native reset_sequence, and matches actual native current Candidate plus exact Work/revision/approved-line/Case-component tuple. Client and target channel come from that actual Workbench item; NULL Client cannot match a specific Client. Case work_id/revision_id/approved_line_id often NULL are handled with IS NOT DISTINCT FROM, with exact case_component_id and Candidate, so native Case-only units are not accidentally omitted or broad-matched. This source uses no guessed Contract/Client/history/name fallback.
- private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb): original leaf/basis/new-changed detection, unit identity and current explicit overlay order are retained. New flags are grouped by the same exact Work-component/Case-component unit, never per-segment toggles. Marker matching and remembered fallback are inserted between original changed and legacy mode/exclusions. Native Candidate-false dominance and SUBSET semantics remain exactly the original output.
- private.bpay_next_selection_memory_capture_v1(uuid,jsonb): one materialized native choices computation under the original descriptor/baseline/raw intent is joined to the original native eligible leaf stream by Candidate/Work/revision/line/Case-component/direction, repeating the same unit value for its segments. Missing joined boolean raises23514 rather than silently defaulting. It stores the metadata boolean at original capture, includes it in the existing10000/8MiB per-component encoded budget and leaves HEADER/body/baseline/carry/new explicit exclusion inserts unchanged. No later PIN/capture reread or accepted-member rewrite.

The reset predicate relies on the pending marker schema making nonnull reset_sequence authoritative only at the genuine accepted effective reset outcome; this must be proved by that migration/operation guard before whole support loads. Discovery/pending/unconfirmed markers must not match. The new nullable column must have HEADER-null CHECK and retain the existing immutable update/delete/truncate rules. Those are outstanding schema/integration prerequisites, not defects in these four definitions reviewed in isolation.

## Exact parent retention evidence

Static normalized-text inverse comparisons (no product execution):

- Capture: after reverting only the materialized choice join/missing-flag check, new encoded boolean and explicit LEAF column/value, the complete function is text-identical to current0016 capture. The entire original suffix from “Carry the original basis reference” is independently identical. HEADER and inherited original basis references/raw intent remain exact.
- Choices: after reverting only unit count/uniform-flag CTE and reset/recorded default branches, the complete function is text-identical to2306 choices. Return signature/typed result, original basis equality/current leaf qualification, new-changed selection and local overlay behavior are unchanged.

Original resolver/four caller contracts, native accepted replay, accepted leaf eligible helper, immutable descriptor/RuleB membership/PIN/query/snooze ownership are not edited here. Current/explicit historical baseline resolve may continue returning the retained run; only requested future defaults interpret effective scoped reset. No deletion or rewrite of old accepted baseline rows is present.

Both new small helper signatures and the two exact existing successors receive owner postgres with PUBLIC/anon/authenticated/service_role EXECUTE revoked. No public definer/route/producers are added by these four definitions.

## Source receipt

| File | SHA-256 |
| --- | --- |
|Current0721 repeatable, four-definition checkpoint|4E0057DC800BA366FDFC90FB5E44EE5E547565184EE4A12C5BFFA75144A42973|
|Parent2306 actor_selection_memory_v1.sql|3F77BBB57500F1320D3873935C8504264A46E50287BE9740267F838F8D1DC750|
|Parent0016 accepted_eligible_leaf_v1.sql|35648FDE73F982466ED72C065EB9B6916D6EF998364BD64AF0F491625A73CFD3|
|Scoped reset design review|D8429BDD59DAE9D85FA13050181B23376EF506F8A99EFEA4C3FF352F76EAED64|

Zero open findings in this four-definition memory source slice. Root is still authoring the complete frozen clear-operation support; further appended definitions require separate source review and will change this checkpoint whole-file hash. No runtime/compile performance or actual business acceptance was run or inferred.

