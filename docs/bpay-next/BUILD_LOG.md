Backend branch: codex/bpay-next-build-20261009
Frontend branch: codex/bpay-next-build-20261009
Backend worktree: C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009
Frontend worktree: C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009

# Banking Pay NEXT codex_r3 completion log

Backend base: a6f7423eb71e6e7cd173e705c2290e5408a1d077 (fresh origin/test).
Frontend base: aada67455afe5a3b6828a713c7062832c0617782 (fresh origin/main).
Both worktrees are registered, on their named build branches, initially clean. Existing worktrees retained, including the normal dirty checkouts and all RESET/OFFICE reference worktrees. No pull, reset, stash, clean or force operation performed.
RESET: d75023b57b80c9829ad55edc90d11f86fbab5e1a; 636 dirty paths (575 untracked, 61 tracked), freshly observed 9 October 2026.
OFFICE: 7a75111ec2923693e75feef4a7ccbcb6ecba942e; 17 dirty paths (14 untracked, 3 tracked), freshly observed 9 October 2026.
Backend baseline db:check: PASS; 294 migrations and 795 repeatables, before editing.
SCX-012 source: base contains 8d950051 and supabase/repeatable/08102026_1635_weekly_source_released_client_eligibility_v1.sql. Saved plan evidence records INSTALLED_ON_TEST at 8 October 2026 18:20:10.752Z with matching closure. This is dated installed evidence, not a fresh hosted inspection; recheck before any owner-authorised installation.
Local database: clean NEW schema compiled in bpay-next-build-pg17; image postgres:17.11-bookworm (cached 051f7b7b3abd); loopback port 55439; database banking_modal_v2_test; task volume bpay-next-build-pg17-data-20261009. Owner cloudtms_bpay_local_owner is NOSUPERUSER/NOBYPASSRLS; service_role NOBYPASSRLS; authenticator absent. Existing provider APIs are empty auth/Vault compatibility; the immutable system bootstrap generates its own local HMAC, never read or disclosed. No business fixtures inserted. Local contract hash 8b698c1c15c26bce2da64fbdb3308859dd22dce6055157999da95ad00f4ad5f8 matches base bytes. No connection string recorded.

## Authority and counting

The owner start sheet overrides plan approval/stop gates. Build all local product code and local commits; do not push, deploy, provision a TEST agency, switch an agency, release MyTMS, or perform payment/provider/bank/email/drain actions. Preserve Policy X and the Candidate/MyTMS boundary. No hosted database has been accessed or changed.
Live checklist copies: IMPLEMENTATION_CHECKLIST.csv and IMPLEMENTATION_CHECKLIST.md. Count ITEM rows separately from STEP and DELIVERABLE rows. BUILT and REVIEWED are separate; INSTALLED and TESTED in app remain pending owner-authorised release and acceptance. Earliest unfinished item: F-03; F-01/F-02 independently reviewed PASS; BUILT 2/288 cards, REVIEWED 2/288 cards.

## Builder decisions

- BD-001: use task-scoped Git -c gc.auto=0 after fetch's automatic maintenance attempted deletion of stale registrations and was denied. No explicit cleanup was requested or performed; retain every unrelated registration.
- BD-002: implement the B2.23/F-23 named local-only working-tree loader during F-01 because the current managed NEW command always invokes catalogue rehearsals and fixture verifiers prohibited by start-sheet section 3. Reuse canonical inventory, executable owner mapping, SQL classification, order, writer lock and atomic checkpoints. Record LOCAL_SCHEMA_COMPILED only; never manufacture VERIFIED managed-release evidence. Normal hosted release tooling and verifier inventories remain intact.

## Completion records

BD-003: preserve unchanged historical Candidate 22082026_1402 source/baseline receipt. Its exact 85-function inventory is not a current final closure. Forcing reapplication failed against newer routines (86 functions); corrected loader uses the current dynamic general SECURITY DEFINER browser boundary after all bodies/readers. No historical guard widened.

BD-004: F-02 found one C4 writer-census delta rather than the predicted eleventh C2 SQL path. Preserve current census and defer only its exact channel-reader acknowledgement until F-06 compiles that definition. Do not discard the C4 work.

### F-01 Create the build worktrees and record the baseline — done 09/10/2026 19:49 London

Seq: 1. Part: F0.
Files changed: log/checklists; scripts/bpay-next-local-schema.mjs; local source checkpoint; design/review notes; task-only ignore entries.
Objects: registered backend/frontend build branches and worktrees; one local PG17 schema profile.
Deviations: BD-001, BD-002, BD-003 above.
Follow-ups: full protected verification remains pending authorised release; LL-01–LL-04 independent re-review PASS.
Blocked: none.
Policy X: no economic, frozen financial, paid-evidence or payment identity changes.
Admission/shared safety: no runtime or database application inputs changed; local schema build only.
Boundary lock: no Candidate/MyTMS boundary source edits.
Source status: LOCAL_ONLY. Installed evidence: dated SCX-012 evidence above; no new installation. Acceptance: pending.
Checks: db:check baseline/post-load PASS; ancestor/registration/reference counts PASS; clean NEW load and zero-change resume PASS; node --check PASS; local seal PASS (byte-identical contract); coupling PASS. Review found LL-01–LL-04; root fixed process-only routing, atomic complete attempted checkpoints, captured reader source and exact escaped full resume identity. Re-review PASS: all LL-01–LL-04 closed, see reviews/F-01-local-loader-review.md.

### F-02 Inventory and classify every RESET and OFFICE change — done 09/10/2026 19:49 London

Seq: 2. Part: F0.
Files: docs/bpay-next/PORT_INVENTORY.md and PORT_INVENTORY.json; log/checklists.
Objects: 653 unique records (636 RESET, 17 OFFICE), complete R/B/U/H and owner/action.
Deviations: BD-004; per-class counts and all exceptions in PORT_INVENTORY.
Follow-ups: classified ports in owning cards; F-03/F-06 census acknowledgement; F-14 preserves Source-local completion/replay.
Blocked: none. Policy X: no drift. Admission/shared safety: no application owner changed; Candidate C2 and published Source C1/C2 retained.
Boundary lock: no boundary source edits. Source status: LOCAL_ONLY. Installed evidence: not established. Acceptance: pending.
Checks: uniqueness/count assertions PASS; fixed SHA/history classification captured; coupling PASS. Independent review PASS: reviews/F-02-port-inventory-review.md; all653 path/hash/class records independently matched fresh Git evidence.
