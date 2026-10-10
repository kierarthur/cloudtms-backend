# F-25 canonical comparison INSERT stage review

10 October 2026. **PASS — bounded source-only hook review.** Reviewed `supabase/repeatable/10102026_0044_banking_pay_next_query_comparison_stage_v1.sql`, 2,674 raw bytes, SHA-256 `89a4bab3f7c4f693f2231f058584af2004720933f4adbd996b89089a1e9a762c`. No product edits, SQL/runtime/tests/fixtures, hosted calls or business execution. Installation/behavioral pass is not claimed. Actual original owner entry/whole-tail and broader Source roots remain separate pending work.

## Current canonical contract and exact stage

24–25 builds exactly the five canonical comparison fields from saved census: incident_id, revision_number, contract_id, issue_family, material_comparison_fingerprint. Real row ID is separately new.id/e.owner_row_id. Stored fingerprint is represented as exact hex; no rehash or replacement of original fingerprint owner. Contract is genuinely nullable and retained as JSON null. No publication/upload/Timesheet/source-row/time/financial/prose/contact snapshot is added to the canonical tuple.

27–28 stages relation `public.weekly_issue_comparison_revisions`, actual inserted UUID, real parent's known Candidate, canonical root NULL, old_present=false/new_present=true, OLD={} and typed NEW tuple. Comparison contains no Candidate column;23 retrieves actual parent by new.incident_id using plain SELECT INTO STRICT. Signed candidate_timesheet_id is existing comparison evidence, and does not prove a canonical financial root. NULL root is truthful; Candidate is never dropped when known.

INSERT-only guard11 and AFTER INSERT trigger35–36 are consistent with comparison IMMUTABLE_APPEND_ONLY contract in `15092026_1534_weekly_source_acl_contract_v1.sql:72`. Existing comparison immutable guards/ACL remain untouched. No comparison UPDATE/DELETE operation is newly admitted or staged.

## Real current INSERT owners and parent scope

Bounded current repeatable census found exactly two actual physical INSERT statements:

| Current writer | Actual INSERT and parent ownership |
|---|---|
|`public.weekly_source_query_sync_atomic_v1(jsonb)`, original `15092026_1534_weekly_source_query_delivery_v1.sql`|4968–4986 comparison INSERT; existing incident FOR UPDATE4886–4889 or newly inserted parent4895–4911, before actual pointer UPDATE4987. Work event locked4881. Original body remains sole saved physical writer at this checkpoint; full new owner successor pending.|
|`private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz)`, `15092026_2203_weekly_source_candidate_app_contract_v1.sql`|1805–1827 comparison INSERT; existing incident FOR UPDATE1745–1749 or newly inserted parent1755+, before pointer UPDATE1828. Work event locked1740. Nested under actual Candidate materialise/app/self or Source prefinal paths; permission in hook does not itself supply missing true-root entry/tail.|

These genuine owners already hold/create the actual incident before AFTER comparison insertion; parent Candidate identity is source-authoritative. New comparison INSERT's original parent/publication/upload/Timesheet/source-link/Contract FKs remain original business work. Hook does not rediscover current source/policy/Timesheet/root or replace original comparisons.

Current source search found no later alternate comparison INSERT, UPDATE or DELETE writer admitted by this change. Historical predecessor text is not independently installed authority. This hook is universal on the relation; unsupported current owner paths now refuse under NEXT instead of silently escaping canonical staging.

## Lifetime, admission and unsupported behavior

12 selects only producer for actual current xid;2352 producer xid is unique. Without producer,14–16 plain stable module state read refuses active NEXT with MATERIAL_WRITER_UNOWNED and preserves existing non-NEXT behavior. No late Source admission, module row lock or agency clock is introduced. In an actual producer:

*18–22 requires BUSINESS_OPEN, actual top owner QUERY_SYNC / CANDIDATE_APP_SUBMIT / CANDIDATE_MATERIALISE / CANDIDATE_SELF_SUBMIT, and exact producer xid/epoch input transaction OPEN. Producer stack/schema are required nonnull and one-dimensional; top cannot be null under2352 constraints. No SQL three-valued predicate allows a missing input record to pass (`NOT EXISTS`).
*TAIL_PREPARED, FINISHING, FINISHED and input CLOCK_SEALED/FINALISING refuse before parent SELECT/stage. It does not enable postclock business discovery, reserved tails or a second clock.
*Source Final/Correct Final/Rows/Upload broader root kinds are not directly accepted top owners. A future actual allowed nested QUERY_SYNC may only run before clock with its genuine parent entry/tail; generic2357 edges or this top allowlist are not proof that Source closure is built. Current missing true owners continue named unsupported behavior. Do not widen the top allowlist to mask missing early Source root entry.
*QuerySync/APP/MAT/SELF top permission only says this canonical INSERT stage is legal while actual owner is OPEN. It does not certify Candidate submission financial publication, source group, immutable outer receipt, or whole F25 completion. Existing0017 app owner closure and future protected Source closure still own those responsibilities.

The real parent SELECT is plain MVCC, with no FOR UPDATE/FOR SHARE, source callback or authority/module/clock call. `query_version_stage_v1` takes already-owned private producer/input operational locks and creates exact immutable raw stage/event association. Its existing schema Candidate FK is a **real business key-share check during stage insertion**, before the clock (`09102026_2153:379`); therefore the complete callback chain must not be described as globally lock-free. This is the existing protocol's preclock FK work, not a new explicit parent business-row lock in0044. New parent INSERT already holds original Candidate reference; existing parent Candidate FK must be counted in the actual preclock closure. No late root FK is taken because root is NULL.

## Exact QuerySync material matrix now supported

Combined with current0017 incident hook, this0044 comparison callback supplies the missing original comparison INSERT stage:

*NEW: incident INSERT + comparison INSERT + incident currentcomparison pointer UPDATE =3 stages.
*CHANGED: comparison INSERT + actual prior→new pointer UPDATE =2 stages.
*UNCHANGED_OPEN / genuine UNCHANGED_RESOLVED: no comparison INSERT, therefore no comparison stage.
*Actual omitted resolution: incident OPEN→RESOLVED only =1 stage.

One AFTER INSERT callback fires per actual inserted comparison; comparison ON CONFLICT DO NOTHING (if a future owner used it) would not stage a nonexistent row. Present original comparison INSERTs are actual direct inserts. Original counts/fingerprint/incident-pointer statements are untouched. Full Sync completion must still verify bijection of these actual stages with typed input/omission certificates, not merely total3*new+2*changed+resolved. Additional actual canonical stage cannot be silently discarded.

## Declaration, trigger, ACL and registry

Private function `( ) RETURNS TRIGGER`, PLPGSQL SECURITY DEFINER, fixed pg_catalog/private/public search_path. Definer supplies access to private protocol/parent; it is a trigger-only callback, not exposed standalone RPC. Function logical owner postgres; EXECUTE revoked from public/anon/authenticated/service_role32–33. The change replaces only its task-owned named comparison-stage trigger, and attaches AFTER INSERT row callback on exact comparison relation. Existing business/immutable triggers are not removed. Notify schema reload is normal release SQL declaration, not executed by this review.

Current final-definer registry selects exact `private.bpay_next_query_comparison_row_stage_v1()` to0044 as finalRoot/finalPhysical/allowedPhysical/finalClosures, no predecessor (new helper), owner F-25 canonical immutable comparison stage; actual preclock parents. No duplicate final declaration or stale private signature found. This review did not run source gates.

No actionable defect identified in the fresh source. Source/root hooks still pending are an explicit boundary, not a whole-owner PASS.

## Saved source provenance

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0044_banking_pay_next_query_comparison_stage_v1.sql` | `89a4bab3f7c4f693f2231f058584af2004720933f4adbd996b89089a1e9a762c` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| `supabase/repeatable/09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |
| `supabase/migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql` | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| `docs/bpay-next/design/F-25-core-query-writer-census.md` | `487897c5341efa1e700cf013ec8e968249b85724dc48ddc50ce37d3af726f9e4` |
