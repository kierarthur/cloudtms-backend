# WP-PP source handoff — 13 October 2026

WP-PP source is implemented for coordinator review. Source/API fixture checks pass; database compilation, installed first use, generated contract, publication and browser acceptance remain separate gates. No feature has been enabled by this lane.

Authority: workspace/repository AGENTS, owner `SEND_5_TO_HEALTHROSTER_BUILD_STAGE_NONSTOP.md` D9, consolidated v2 `03_IMPLEMENTATION_PLAN.md` §§25–25.2 and R-195/R-201/R-216/R-234. The coordinator closed the catalogue gap before coding and owns all releases, fixture manifests, database calls and contract/security sealing. Banking W1–W12/S11 convergence and W17's later agency wrappers remain their respective consumer work.

Authoritative worktree: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\healthroster-v2-backend-13102026`, branch `codex/healthroster-v2-backend-13102026`. Reviewed source predecessor: `e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92`, merged into the worktree at `b8dcdcd1a758abaf09721d827669810abee462e0`. Coordinator-only documentation later advanced HEAD to `8ac8558cc82535ded6e63387a76a0701c696ee63`. These are source identities; none is a WP-PP installed receipt.

## Source review and preserved boundaries

Reviewed the complete `77ffc9d7d15d19b6453ae25441eee8c1bff58c90` → `e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92` successor delta: nine files, 450 additions/eight deletions. It narrows Candidate availability-copy lag, reasserts the unchanged current Rota tiles reader, and adds the payload-free post-commit Google wake. The Rota source dates, complete fourteen-day generation, identity, transition, terminal and source-reconciliation checks remain. The exact specialist wake import and dependency hook in `index.js` are preserved, and its source regressions passed.

The new repeatable extracts the current TEST physical definitions, preserving their signatures, owner/security/search path and final service-only ACL. It does not copy NEXT-only source. Current physical predecessors:

| Scope | Current TEST file |
|---|---|
| Shared Office authority S1 | `15092026_1534_00_weekly_source_private_classifiers_v1.sql` |
| Five C1 actor checks S2–S6 | `15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql` |
| Publication status S7 | `15092026_1534_weekly_source_protected_action_orchestration_v1.sql` |
| Local complete S8 | `04102026_1257_weekly_source_local_protected_decision_v1.sql` |
| Ordinary workspace S9/S10 and Timesheet read projection | `15092026_1534_weekly_source_read_projections_v1.sql` |
| Manual pay-query admission | `05102026_0428_weekly_source_manual_review_commands_v2.sql` |
| Authorise routing read | `05102026_1210_weekly_source_office_authorise_scope_v1.sql` |
| Existing scope reader | `01102026_1201_weekly_source_combined_workspace.sql` |
| Current combined Queries reader | `09102026_1130_weekly_source_released_review_visibility_v1.sql` |
| Actor-specific notifications | `15092026_1534_weekly_source_query_delivery_v1.sql` |

The combined reader retains its newer released NHSP row/client eligibility, source-authority, failed-recheck and ordinary admin behavior. Only the reduced pay view omits finalisation recovery and admin source-recheck items. Before publication, the coordinator must freshly integrate any new canonical Banking source and compose the narrow permission changes with its final bodies/hooks. A clean textual merge does not satisfy that gate.

No Banking Workbench, Draft, provider, settlement, remittance, generic Authorise/Unauthorise SQL owner, protected Candidate source, Rota reader or feature flag was edited. The former SQL definitions remain unchanged historical sources. C1/local writer regression compares every existing financial DML statement, refusal/contract string order and lock count with its predecessor. Policy X and OR-44 are unchanged: no economic keys, source-money admission, frozen post-Draft artifacts, TS_DAY dates or freshness authority changed.

## Product changes

`private.weekly_source_pay_actor_allowed_v1(uuid)` and `broker/src/payment-actor-permission.js` hold the single SQL and Worker predicates: strictly active AND (admin OR payment authoriser OR golden key). The SQL helper is stable, SECURITY DEFINER, has a pinned search path and service-only execution. The Worker helper has a minimal single-user GET, twelve-second bound, missing/inactive 401, unflagged 403 and a caller response hook. Failed reads do not disclose upstream bodies. Trusted transport adapters can retain their caller's response shape and additional private fields without a second predicate.

Shared Office authority keeps the existing missing/inactive ordering, request whitelist, group/activity, dated membership and decision fingerprint checks. Its closed pay operation set adds `ACCEPT_SYSTEM_HOURS`, `OPEN_PAY_QUERY` and `VIEW_PAY_WORK`. Upload/recheck/finalise/correction/invoice/communication operations retain admin admission. Manual pay-query open uses its own `OPEN_PAY_QUERY`, leaving upload/recheck permission intact. S2–S8 retain exact recorded-actor/request/state/replay/locking guards while replacing only their inline predicate. Existing callers through S1 inherit the widening.

`requireUser` returns three additional fields from its already-read current row: strict `is_active`, `payment_authoriser`, `payment_golden_key`. Authentication, session version and tenant decisions are unchanged. W13 admits only a fresh active pay actor and derives reduced view on the server. The HTTP allowlist refuses every admin route/command before dispatch; browser actor or role fields cannot grant access.

W14/W15 preserve ordinary Timesheet admin-only denial. A non-admin pay actor must pass the existing service routing probe for the current resolved Timesheet before any correction-pair or mutation owner runs; uncertain/PGRST202 scope remains a retryable 503. The Authorise path reuses that probe and keeps the existing first-authorisation wrapper. W16 qualifies Source detail/bulk and mutation hints using the authenticated server actor plus the exact service scope. Missing/foreign/inactive actor evidence fails closed; existing explicit false state/lock hints remain false. Ordinary and Daily non-applicable read payloads stay on their existing paths.

## Exact reduced view contract

Both existing workspace contracts keep their identity and add a top-level sibling `permissions`:

```json
{
  "contract": "WEEKLY_SOURCE_PAYMENT_VIEW_V1",
  "view": "PAY",
  "allowed_tabs": ["queries"],
  "allowed_sections": ["questions", "checks", "protected"],
  "landing_tab": "queries",
  "can_upload": false,
  "can_finalise": false,
  "can_manage_settings": false,
  "checklist": {
    "available": false,
    "read_only": true,
    "pay_only": true,
    "action_links": false
  }
}
```

Ordinary parent contract: `WEEKLY_SOURCE_IMPORT_WORKSPACE_V1`; combined parent: `WEEKLY_SOURCE_COMBINED_REVIEW_V1`. The ordinary pay response omits imports/finalise/history/recheck payload/lifecycle-policy controls and has empty `context.controls` and query communications bulk controls. It retains Hours-question pay proofs, individual Accept source actions and Protected shifts. Counts/page admission apply the same reduced row predicate. Its cursor binds the actor and permission view while retaining the original source workspace version for action proofs. Combined pay output omits attention/summary, retains allowed subject sections and computes its cursor/count from the admitted rows.

Office enters through `GET /api/weekly-source/v1/workspace` **without a tab**, receives this capability, then uses `landing_tab` and `allowed_tabs` before restoring a tab/deep link. An explicit forbidden tab is refused 403. The exact fixture and entry rule were sent to the Office lane with coordinator authority.

Checklist reader/expansion are not yet produced: WP-U3 remains pending. No nonexistent Checklist RPC is advertised or dispatched. Its eventual reduced consumer must be read-only, pay-only and text-only with no action links. W17's scoped draft round-trip/agency wrappers remain later producers/consumers; this first batch does not claim them complete.

## Files

- New helper: `broker/src/payment-actor-permission.js`.
- Scoped callers/projections: `broker/src/weekly-source/routes.js`, `broker/src/index.js`.
- New sixteen-definition successor: `supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql`; each function finishes with browser revokes and service grants in this same closure, then schema reload.
- New rollback proof: `supabase/verification/13102026_1126_weekly_source_protected_pay_authority_verification.sql`.
- Existing registered publisher verifier: main admin fixture now has no payment flag and remains the allowed positive; refusal fixture is an active non-admin without flags. It was not executed by this lane.
- New tests: `tests/payment-actor-permission.test.js`, `tests/weekly-source/weekly-source-pay-view.test.mjs`, `tests/weekly-source/weekly-source-pay-authority-sql.test.mjs`.
- Existing test fixture/assertion updates: routes, settings routes, correction orchestrator, report real-owner, Office presentation integration.
- This implementation handoff. Central PROGRESS/setup/install/receipt files remain coordinator-owned.

## Verification and remaining gates

Passed: source `db:check` before editing (295 migrations/802 repeatables) and after (295/803); syntax checks for all three changed Worker modules; `git diff --check`; focused ten-file Node batch **90/90**; separate C1/local DML/error/lock/ACL/source-boundary guards **3/3**. The focused batch includes the new actor/API tests, ordinary route/settings/report/orchestrator/presentation tests, native lifecycle wake regressions and current Rota/specialist successor tests. Task dependencies were installed from the unchanged lockfile with `npm ci --ignore-scripts --no-audit --no-fund`.

Authored, not run here: harmless rollback verifier for six synthetic actors, distinct ROSTER/NHSP clients/groups (the cross-group client-range exclusion is preserved), date membership, all eight pay operations, unchanged admin operations, inactive/error order, empty ordinary/combined reduced first use and all sixteen effective service/browser ACLs. The vacancy guard covers exact fixture prefixes across users/clients/groups/memberships/cycles and fixture emails/codes. Coordinator must record the exact manifest and fresh absence proof before running it, then prove fresh-connection fixture absence after rollback. No financial/publication/communications owner is called by this verifier.

Pending coordinator gates: full source review; both verification arrays/service inventory/security manifest; exact staged/committed-blob PostgreSQL 17 compilation and canonical generated contract/seal/coupling; rollback first use on agency TEST; Banking exclusive-window evidence and fresh source integration; reviewed complete source commit/push; protected database/API/ACL installation; ordered compatible runtime publication; Office patched-asset and actor journeys. Installation remains held while Banking candidate/install records are not proved clear. Latest other-stream release status is coordinator evidence, not this lane's database claim.

This lane performed no database call/DDL/DML, Docker action, deployment, feature activation, payment/provider/settlement/bank/email effect, secret change, Git commit/push or Banking worktree edit/message. No credentials or sensitive payloads were printed. Dependencies remain ignored in the retained worktree for the coordinator's checks. No LIVE action is authorized or pending as part of WP-PP.
