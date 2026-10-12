# WB-29 exact carry guard source review

12 October 2026, sequence140, EO03/BD161. **SOURCE PASS —2313 guard successor only.** Root alone writes product. This reviewer authored the earlier exact-carry design and now reviewed the independently root-authored implementation; it is a design-conformance/static source review, not a second independent design approval. No product edit, SQL/RPC/database execution, fixture, test, hosted action or commit occurred.

Reviewed complete `supabase/repeatable/12102026_2313_banking_pay_next_resolution_exact_carry_v1.sql` against the exact current2100 statement and `design/WB-29-exact-carry-guard-design.md`. No remaining concrete source defect found in this guard batch.

| Check | Result |
|---|---|
| Complete predecessor retention | PASS. A static text inverse removing only the three new declaration lines, carry proof block and conditional saved-Position equality reconstructs the complete normalized2100 CREATE FUNCTION statement exactly (4,367 characters). UPDATE state-only immutability, INSERT state/native Basis/mode checks, current Position and complete published-basis/Source checks remain. |
| Closed carry body | PASS. Exactly outer carry and inner decision_id/decision_version/request_id; all required string fields, canonical lowercase UUIDs and positive bigint text checked before casts. Overflow at the native excluded bigint maximum refuses before bigint cast. A malformed body containing carry cannot use fresh admission. Existing body/Position JSON bounds remain in table constraints. |
| Exact old version / cleared event | PASS. Exact indexed old ID/version and unique CLEARED event; old must be CLEARED, new ID differs, new version is exact old+1. Same Work/Candidate/physical/component/family/code, actual new child command/current audit actor and old based-on event revision/line must match. No latest/max/history substitute, collision suppression or old-row metadata update. |
| Native publication boundary | PASS. Non-null new published pair; actual Work/revision publication matches command/Candidate/predecessor. Sealed treatment marker and exact sealed POSITION_APPLY one-Candidate member are checked, followed by existing2135 treatment_approval_revision_v1 for full native root/parent/command continuity. No ordinary publication or naked JSON flag becomes carry authority. |
| Old→predecessor mapping | PASS. Either exact old original pair/basis or its real published pair/basis maps to the new based-on line. Published arm explicitly requires non-null native pair. Existing FK/non-null shape plus structural equality keep the proof closed; absence is explicitly rejected. |
| Retained intent and saved scope | PASS. Null-safe tuple equality retains mode, both manual operands, target channel/Umbrella/VAT, policy/date, reason and original_position_basis against that exact old immutable version. Incompatible target/date siblings are not retargeted or freshly quoted. Only current-Position JSON equality is conditional on the proved carry; original applied revision, approved Source amount and finite realised/held checks still apply to both branches. |
| Source / financial preservation | PASS. The complete predecessor published-basis checks remain exact, including actual Source kind/head/channel/Umbrella/amount/rate/charge, physical/component/date/unit/tax/units/policy. No Position, hold, pricing, publication, selected amount or Source ledger write is added to the trigger. The existing quote still refuses an old manual amount on a changed Position scope. |
| Audit / lock / ACL scope | PASS. New carrier actor must equal the same-child CLEARED event actor; old original actor/time/body remain immutable and reachable through the exact pointer. Trigger adds point reads and one existing read-only native helper, with no new lock/clock. Original security invoker/search_path, postgres owner and revocation from public/anon/authenticated/service_role remain. No new table, field, public input or command protocol. |

The existing native event sequence remains mandatory for the future W2 constructor: publish/seal the real child; insert CLEARED while old ACTIVE; retire only old.state; insert new carried version with the exact +1 version expected here; insert SAVED; write the original immutable W2 receipt; commit with existing deferred audit. `carry.request_id` is typed audit metadata at INSERT because the receipt is written last; the exact event/publication/basis proof supplies admission. This guard does not claim that the W2 constructor or complete mapped closure is already implemented.

Registry currently promotes exactly `private.bpay_next_component_resolution_guard_v1()` to2313. Existing event and deferred audit owners remain2100. The exact existing callee closure must include treatment_approval_revision_v1 at the normal source-gate check; this review did not run that gate or compile SQL.

| Saved evidence | SHA-256 |
|---|---|
|2313 exact carry successor | `eccd78cc642eda212420fbf336e8cae72827a8f567bd3c2eb31195df791d1e1b` |
|2100 predecessor | `54a3b81cf74d1f86f7547b8948bca3d24a48493b23ed47e9cb949f9142b2f556` |
|Exact carry design | `2864898f4529f4de94812b1892bf0b7005816890417b8c0c46d007bace4b5d0b` |
|Current final-definer registry | `d419e5cbe5d9dc84c90ede6a6d302c7e990c986b398add359380797cd4af4f42` |

SOURCE PASS is limited to these saved bytes and native guard conformance. Compile, installation, real W2 caller/enumeration/event ordering, joined acceptance and whole WB29 remain outside this receipt and are not marked complete.
