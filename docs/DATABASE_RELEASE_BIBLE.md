# CloudTMS Database Release Bible

## Controlling outcome

The repository is the complete, reviewable authority for CloudTMS database structure, RPCs, views, triggers, permissions, RLS posture, and verification. Database releases must be routine, reproducible, evidence-producing, and fail closed. They must never infer that an unknown populated database is safe merely because tables already exist.

This system changes database definitions only. It does not copy or replace customer, candidate, timesheet, finance, provider, payment, Vault, Auth, or other business data.

## Non-negotiable boundaries

- Never run an unreviewed release against LIVE. `PLAN` is read-only. `APPLY` requires the protected `database-live` GitHub Environment and the exact phrase `APPLY LIVE <MODE> <COMMIT_SHA>`.
- The configured hosted database URL must contain the protected provider-neutral target locator in its host, username, or database path. Free-form target URLs are not accepted through the workflow.
- One-time migrations are immutable. Their path and SHA-256 are locked in `supabase/release/migration-lock.json`; changing or removing an existing migration fails CI.
- Repeatables are re-applied only when the SHA-256 of their complete recursive `\ir` include graph changes. Included support pages are part of the authority.
- Never mark a populated database as migrated merely because it has tables. Existing databases enter control only through `ADOPT`, after an exact read-only contract comparison.
- Never manually edit release ledgers, contract hashes, or installed release evidence.
- Preserve CloudTMS Policy X: pre-draft may use live truth; post-draft uses frozen batch artifacts only; `TS_DAY` remains `YYYY-MM-DD`; no post-draft live finance fallback, invented economic-key ladder, or freshness bypass is permitted.
- Preserve the Candidate/MyTMS boundary locked by `supabase/release/protected-boundary-lock.json`. The protected files may not be removed, renamed, duplicated, weakened, or rewritten. The three security verifiers must pass after every release.
- Candidate features, communications, invitations, and autonomous Workbench activity remain disabled on a new database until separately reviewed and deliberately enabled.

## The four supported modes

### NEW

Use only for a genuinely blank PostgreSQL application database. The release tool proves that `public`/`private` contain no CloudTMS relations, installs the signed schema-only baseline, applies repeatable authorities added or changed since that signed baseline, creates release-control tables, creates a fresh database identity, installs safe system defaults, generates a fresh local HMAC secret in that database, runs security verification, and compares the result with the approved contract. It copies no application rows from TEST.

### ADOPT

Use once for an existing CloudTMS database that predates this control plane, including the current TEST and the existing LIVE database. `PLAN` exports its schema/security contract without mutation and must match the approved repository contract exactly. Only then may `APPLY` add release metadata and record the verified current migration/repeatable hashes. There is no blind baselining.

If the contract differs, stop. Investigate and create a reviewed upgrade or a newly approved contract. Never force adoption.

### UPGRADE

Use for a database already carrying `private.cloudtms_database_identity` and the private release ledgers. The tool validates its environment/customer identity, proves that every installed migration still has its original hash, applies only pending one-time migrations, applies only changed recursive repeatable closures, reruns the security verifiers, and requires the installed contract to match the approved contract before recording `VERIFIED`.

### LEGACY_UPGRADE

Use only for the one-time transition of the existing LIVE database when its historical `public.schema_migrations` ledger is valid but its schema is older than the current repository contract. `PLAN` is read-only and requires exactly one historical bootstrap marker, at least one installed repository migration, no unknown or duplicate migration names, no ambiguous public repeatable ledger, no managed database identity, and the protected LIVE target. `APPLY` installs only repository migrations absent from the historical ledger, records each successful legacy migration for resumability, and applies the current repeatable authority. A bounded transition bootstrap first installs the old Banking Pay structural repeatable and supplies no-op definitions for three trigger functions that the original platform installed outside its migration ledger; application traffic must remain disabled during this one-time replay. The release refuses adoption unless current repeatables have replaced every transition shim, every security verifier passes, and the installed contract exactly matches the approved contract. Only after those checks pass does one transaction create the LIVE identity and complete private migration/repeatable/release ledgers. It never runs against TEST, never mutates the former Supabase source, and never performs a blind baseline.

## Normal developer workflow

1. Start every database/RPC task with `npm run db:check`.
2. Create a schema/data migration with `npm run db:new:migration -- --name=<short_snake_case_name>`, or a complete replacement RPC/view authority with `npm run db:new:repeatable -- --name=<short_snake_case_name>`.
3. Implement the smallest reviewed change. Never modify an older migration. Function files must contain complete `CREATE OR REPLACE` definitions and preserve their owner, `SECURITY DEFINER`/invoker status, controlled `search_path`, and grants.
4. After the new migration is complete and reviewed, run `npm run db:lock:update`. This only appends new migration hashes; it refuses to rewrite, remove, or re-lock an existing migration.
5. Test in a disposable local PostgreSQL database. For a DB-only TEST correction explicitly authorised by the user, install to TEST first, verify it, then ensure the exact installed definition and canonical hash are represented in the repository before publication.
6. Refresh the approved contract only from a verified disposable rebuild containing the intended source: `npm run db:contract:export`.
   A successful protected PLAN does not replace this step because PLAN does not install pending definitions. The reviewed generated contract must be committed with the source change before the first APPLY; do not use APPLY to discover a stale contract.
7. Run `npm run db:check`, the database-release tests, all existing repository tests, the exact security verifiers, and a clean `NEW` replay. Do not publish a change that requires manual ledger repair.
   Exact function-inventory hashes must use provider-neutral catalogue identities (routine schema/name plus input type schema/name in ordinal order), never the `search_path`-dependent display text of `regprocedure`. Prove the same inventory/hash on the clean PostgreSQL 17 rebuild and the protected Miget target.
   The catalogue contract is semantic and provider-neutral: column membership is sorted by name and does not fingerprint physical `attnum` history; optional routine-local `plpgsql_check.*` instrumentation settings are excluded while every functional routine configuration and normalized definition hash remains covered; and the Supabase-only `authenticator` grant on `cloudtms_data_api_mfa_gate()` is excluded while browser/service ACLs remain covered. When a managed database contains a legitimate security policy absent from a clean rebuild, add an exact additive source migration that verifies or creates that policy—never bless unreproducible drift by copying the hosted contract blindly.
   The `NEW` replay installs the immutable structural/routine baseline, executes the current disabled-first bootstrap, then applies every locked migration after the release control-plane anchor before installing only repeatables added or changed since the baseline snapshot. A clean database must therefore finish at the same current contract as `UPGRADE`; never omit post-baseline migrations or persist a temporary bootstrap default merely to make the initial insert pass.
   `NEW` then runs its explicit portable verifier set. Data-dependent regression fixtures that require historical TEST business rows remain mandatory for `UPGRADE` but must not be run against an intentionally empty new agency; the `NEW` verifier set must still include every browser-isolation, RLS/ACL, Candidate/MyTMS, private-helper and rollback-contained first-use verifier.
8. Pushes run source verification only. They never alter a database.
9. Use **Database Release (manual and protected)**. Every APPLY job runs its exact read-only plan after the source gate and before mutation. Managed `TEST` + `UPGRADE` has standing owner authority and does not require a separately typed commit phrase; the workflow generates the engine's exact commit-bound approval internally only for the canonical `test` branch head. LIVE, NEW, ADOPT and LEGACY_UPGRADE continue to require the appropriate protected Environment and exact user-supplied approval.

If an APPLY has already installed the intended definition but stops before recording `VERIFIED` only because the approved contract is stale, do not touch the ledgers and do not guess the contract. Run the protected read-only TEST contract export at the exact installed source, compare every object against the repository contract, require the diff to contain only the intended definitions, commit the exported data-free contract, then rerun PLAN and APPLY against that new exact commit.

## One-button operations

### Upgrade TEST

For a read-only preview, run the manual workflow with `environment=TEST`, `mode=UPGRADE`, and `phase=PLAN`. For an authorised managed TEST correction, dispatch `phase=APPLY` without asking the owner for another phrase. The APPLY job source-gates the exact current `test` branch head, runs the read-only plan first, then generates the exact commit-bound engine approval internally. It must fail closed if the repository, branch, environment or mode differs. The protected `database-test` Environment supplies the Miget database URL and target locator.

### Upgrade existing LIVE for the first time

Do not guess that LIVE equals TEST. First run `environment=LIVE`, `mode=ADOPT`, `phase=PLAN`. If the read-only contract differs, stop and review the generated difference. For the existing valid historical migration ledger, use `LEGACY_UPGRADE/PLAN`, review its exact installed/pending counts, and then run the protected commit-bound `LEGACY_UPGRADE/APPLY`. If ADOPT matches instead, an approved `ADOPT/APPLY` installs only the control metadata. All subsequent releases use `UPGRADE`.

### Create a database for a new client

Create the isolated PostgreSQL service/database and configure its GitHub Environment with the exact database secret and provider-neutral target-locator variable. Run `NEW/PLAN`; it must prove blank. Then run `NEW/APPLY` with the exact commit-bound phrase and optional non-secret customer key. Configure tenant data and enable features only in separate, reviewed onboarding work.

## Miget runtime profile

Each agency remains an isolated PostgreSQL service with independent credentials and an independent PostgREST application. Those services may share one Miget Resource, but only CPU is pooled dynamically: every PostgreSQL service, PostgREST app, and administration app still needs an explicit RAM allocation, and every database/volume needs an explicit storage allocation. Allocations must fit within the purchased Resource and must not be oversubscribed. Never reduce an operational database or PostgREST allocation to fund an optional administration UI.

Repository paths under `supabase/` are retained as historical source-layout names; they are provider-neutral release artifacts and do not make Supabase the runtime authority. Logical Worker variables named `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` may also remain for compatibility, but their deployed values must resolve only to the intended Miget/PostgREST route and its matching JWT.

The PostgREST database URI must retain `options=-c%20pg_show_plans.is_enabled%3Doff`. Miget preloads `pg_show_plans`; allowing its collector on CloudTMS PostgREST sessions caused repeated memory warnings and severe complex-RPC latency. PostgreSQL query diagnostics use the repository-controlled `pg_stat_statements` extension migration instead. Prove both settings after every new database, restore, service resize, or PostgREST redeployment.

Miget's `service_role` is not a PostgreSQL `BYPASSRLS` role. Migration `24082026_1519_miget_service_role_rls_compatibility.sql` creates the explicit `cloudtms_miget_service_owner_all` policy for every existing RLS-enabled public table, preserving the practical service-role behaviour expected by the Worker while leaving SQL grants as the independent table-access boundary. Any later migration that enables RLS on a new public table must create and verify that policy in the same change. After a restore or release, verify the policy catalog and perform a real PostgREST service-role lookup; a direct owner query alone cannot detect this failure mode.

A provider-owner ACL baseline can also remove an explicit `service_role` grant from a private helper even while the public PostgREST RPC remains executable. That failure happens inside a `SECURITY INVOKER` RPC before its work row is leased, so a scheduler can look healthy while the operation remains queued. NEW and UPGRADE releases must execute every role-specific route-guard verifier after the final repeatables are installed. For the email drain this includes `24082026_1721_miget_private_helper_service_acl_verification.sql`, which uses effective privilege catalog checks because Miget's generated owner cannot `SET ROLE service_role`; never repair this by granting browser roles access to `private`.

## GitHub Environment setup

Create protected environments named `database-test` and `database-live`. Each holds:

- TEST secret `MIGET_DATABASE_URL_TEST` and LIVE secret `MIGET_DATABASE_URL_LIVE`;
- TEST variable `MIGET_DATABASE_TARGET_TEST` and LIVE variable `MIGET_DATABASE_TARGET_LIVE`;
- required reviewers and branch/tag restrictions appropriate to the environment.

Managed TEST UPGRADE has standing owner authority and must not require a human reviewer or typed approval phrase; its exact source, target and safety gates remain machine-enforced. LIVE must require a human reviewer. NEW, ADOPT and LEGACY_UPGRADE must retain their exact commit-bound user approval. Do not store a production URL as a repository-wide fallback secret. A new dedicated client database should use its own protected Environment before its first release.

Neither TEST nor LIVE accepts a legacy Supabase database-URL fallback. Each protected Environment must supply its matching Miget URL and target locator, and the release engine verifies that locator against the actual hosted URL before any plan or apply. Migrating LIVE hosting does not authorise upgrading LIVE to the TEST schema: the historical LIVE database remains on its current schema until a separately reviewed `LEGACY_UPGRADE` PLAN/APPLY is approved.

## Contract and evidence

`supabase/release/current-contract.json` is a data-free catalogue contract. It covers extensions, schemas, enum labels, relation/column shapes, defaults, constraints, indexes, RLS flags, effective grants, function signatures and definition hashes, function security/configuration, views, user triggers, policies, and safe application-owned default privileges. It excludes business rows, sequence values, secrets, Vault contents, platform-owned objects, and the release-control tables themselves.

Every successful apply records commit, mode, expected/installed contract hashes, migration hashes, repeatable closure hashes, timestamps, and verifier evidence in private tables inaccessible to browser and service roles.

## Stop conditions

Stop without applying when any of these is true:

- target locator or environment is absent/mismatched;
- `NEW` finds an application relation;
- `ADOPT` finds any contract difference;
- an installed migration hash differs or its repository file is missing;
- a protected Candidate/MyTMS file differs;
- a verifier fails;
- the final contract differs;
- approval is absent, malformed, or bound to another commit;
- a proposed database change would alter Banking Pay economics, Policy X, Candidate/MyTMS business semantics, or provider/payment behaviour without separate explicit authority.
