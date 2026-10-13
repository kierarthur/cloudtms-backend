# F-25 Rule B selection source review

10 October 2026. **PASS for the corrected0151 migration and four consumer owners. RB01 is closed; no open concrete source defect found in this authorised slice.** Reviewed saved source only, before lock/installation. No SQL, runtime, tests, fixtures, hosted reads or product edits were performed. This verdict does not claim concurrent/runtime acceptance, installation or app testing.

## Final reviewed hashes

| Source | SHA256 |
|---|---|
| `supabase/migrations/10102026_0151_banking_pay_next_rule_b_selection_snapshot_v1.sql` | `7e270d8bdeed7f2c5f52432b13273a107d2b91ca3f3d8ee6dbc2e1adcd0c9645` |
| `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |
| `docs/bpay-next/final-definer-closures.json` at final read | `875f3b481cdd9e617a408ff49ff66556f14714ae7b5d1de421351858d149c28a` |

Retained comparison bases:1590 `47a0df6a06bd7816b0102c1aa5fca0bddd97ebc08a0a04d786254db3179967b3`;1600 `94cd02fa3ffeb152b0281a840c08249ad9c3e0d26e2525d5fc3d769da4dd4de5`; F-05 foundation2018 `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`; F-07 ordinary1958 `c6f7d426fa60d3c6cf5db4d5c386aeb8111f974e455bd475a10dcfecf0e5851b`.

The binding Rule B owner message, current owner-design note, actual F-25 card/checklist and W10/master successor rule were read. Superseded producer/effective-cut requirements are not applied to this replacement scope. The separate design investigation is retained at `docs/bpay-next/design/F-25-rule-B-selection-snapshot.md`.

## RB01 — closed: migration must not attach a later repeatable function

Initial migration SHA `4094b86373cc785f11d38c75c8c889355851e878c8163d2c12951f5978e521e2` attached both snapshot immutable triggers at21–24 to `private.bpay_next_effect_immutable_v1()`. That routine is defined only in repeatable1342:188, so a clean NEW migration phase has no such routine yet. CREATE TRIGGER resolves it immediately and could fail42883. The initial consumer hash was `21dbfa71ae53a32219c95577c8311ac6b014569b920f1b25bb803fbcf26602af`.

Verified final correction: the migration contains only table/column/owner/RLS/ACL DDL; both attachments are now final0151 repeatable1199–1206 with DROP IF EXISTS then CREATE. Canonical date order installs1342 first. UPGRADE reapplication remains idempotent. The existing guard raises23514 for any operation other than INSERT, so UPDATE/DELETE/TRUNCATE are refused; the statement-level TRUNCATE callback never reaches its RETURN NEW. Only the new private snapshot table receives these hooks. RB01 closed.

## Passed schema and exact scope checks

- All six snapshot fields use the actual UUID/JSONB/timestamptz types, with NOT NULL. The PK is exactly `(run_id,work_id)`. Composite FKs reference real existing unique keys: run_selection `(run_id,work_id)` and work_revision `(work_id,id)`; deletion restricts remain. The captured physical UUID is selected from that exact immutable expected revision, and PIN qualifies the same UUID/revision in its exact lookup. No imaginary composite key or current physical-row fallback is used.
- The four selection-candidate columns are nullable for OPEN rows and use actual text/uuid/boolean/numeric(12,2) types. The real Umbrella UUID FK is retained. Fresh seal captures actual raw Candidate/Umbrella values; original CLAIM's CASE expressions still normalize PAYE worker Umbrella fields to NULL. No floor/default gate/channel is manufactured for missing capture.
- New snapshot rows are owner-only, private, RLS enabled without public policies, revoked from PUBLIC/anon/authenticated/service_role and immutable under the corrected repeatable attachments. Existing selection-candidate owner/private ACLs remain unchanged. No Source/query/Candidate/Umbrella table trigger or producer owner is added.
- An in-memory source comparison removed only the authorised hunks and comment-only differences. Each remaining full function code matched its controlling base exactly: seal1590; CLAIM2018; PIN1600; case1958. Thus this is a W10 merge of the existing owners, not an import of parked2236/2308 or older0360. No tests or SQL were used by this string comparison.

## Passed acceptance/read-site checks

- Seal0151:20–46 retains original root lock, exact page/component counts, SEALED replay, case-only counter and status checks. The SEALED replay32–35 returns before admission49 or any capture. Fresh seal performs one actual selection-candidate UPDATE50–56 and the selected-work loop57–69, then the existing SEALED/review_revision UPDATE70–72. Same transaction failure rolls back capture; no independent command/clock/protocol receipt is introduced.
- Query population uses only actual run selections, the same work choice and `revision.id=choice.expected_revision_id AND revision.work_id=choice.work_id`. It exactly matches PIN's existing Source/protected predicate: work_kind SOURCE plus revision.source_kind SOURCE/PROTECTED. It traverses actual selection_no order and calls the existing Source gate with **that revision's physical_timesheet_id**. It neither adopts a newer work revision nor changes a sealed component manifest. Query gate evaluation occurs in a separate INSERT statement after admission; raw results, including defined unavailable/blocked state, are retained without a local query interpretation.
- CLAIM0151:119–138 preserves F-05's exact sealed PREPARE command/module/member-count barrier and the earlier Candidate command barrier. Only143–146 replaces the live Candidate/Umbrella join with the selection-candidate PK lookup. Existing worker replay, pay-channel refusal, target normalization, lease nonce/epoch/deadline and counters remain unchanged.
- PIN retains exact selected-revision/current/applied staleness handling and whole-Candidate REVIEW before holds. Its new454–461 lookup matches run, work, expected revision and physical UUID, and absence raises23514 `BPAY_NEXT_PIN_QUERY_SNAPSHOT_MISSING`. There is no Source gate fallback. All original380–451 gate shape/scope/physical-version/booking/Candidate/client/Contract/week/family/financial authority, unavailable/null shape, digest, disposition and existing pin INSERT code is unchanged. The existing pin guard is untouched.
- Ordinary case0151:687–688 reads the exact run/Candidate captured floor. The complete1958 ordinary comparison refusal, invalid-floor checks, prior-week proof, case selection exclusion, live case/rule/period/balance locks, shared valuation/allocation, holds, cursor and final financial-view counter remain unchanged. No accepted-week origin/effective-cut protocol is imported.
- All four actual signatures retain original security/search paths and private revoked execution. Registry final root/physical/closure selects0151 for exactly those four owners. No1610,1620,0280, enroller, pay-query producer or Worker replacement appears in this successor.

## Verification boundary

The snapshot records the raw committed Source gate actually observed at fresh seal, not a historical browser-clock query. Existing raw55P03 admission failure is retryable through the unchanged Worker fallback; it is not automatically the structured QUERY_BUSY409 envelope. No runtime concurrency, performance or actual business counts are asserted. The root owns permitted schema compilation/contract checks and final status bookkeeping. Prior F-25 protocol reviews remain historical records and do not enlarge this Rule B scope.
