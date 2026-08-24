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
7. Run `npm run db:check`, the database-release tests, all existing repository tests, the exact security verifiers, and a clean `NEW` replay. Do not publish a change that requires manual ledger repair.
8. Pushes run source verification only. They never alter a database.
9. Use **Database Release (manual and protected)**. Run `PLAN` first. Review the exact commit and outcome. Run `APPLY` only after the appropriate GitHub Environment approval.

## One-button operations

### Upgrade TEST

Run the manual workflow with `environment=TEST`, normally `mode=UPGRADE`, and `phase=PLAN`. If it passes, rerun the same commit with `phase=APPLY` and the displayed exact approval phrase. The protected `database-test` Environment supplies the Miget database URL and target locator.

### Upgrade existing LIVE for the first time

Do not guess that LIVE equals TEST. First run `environment=LIVE`, `mode=ADOPT`, `phase=PLAN`. If the read-only contract differs, stop and review the generated difference. For the existing valid historical migration ledger, use `LEGACY_UPGRADE/PLAN`, review its exact installed/pending counts, and then run the protected commit-bound `LEGACY_UPGRADE/APPLY`. If ADOPT matches instead, an approved `ADOPT/APPLY` installs only the control metadata. All subsequent releases use `UPGRADE`.

### Create a database for a new client

Create the isolated PostgreSQL service/database and configure its GitHub Environment with the exact database secret and provider-neutral target-locator variable. Run `NEW/PLAN`; it must prove blank. Then run `NEW/APPLY` with the exact commit-bound phrase and optional non-secret customer key. Configure tenant data and enable features only in separate, reviewed onboarding work.

## Miget runtime profile

Each agency remains an isolated PostgreSQL service with independent credentials and an independent PostgREST application. Those services may share one Miget Resource, but only CPU is pooled dynamically: every PostgreSQL service, PostgREST app, and administration app still needs an explicit RAM allocation, and every database/volume needs an explicit storage allocation. Allocations must fit within the purchased Resource and must not be oversubscribed. Never reduce an operational database or PostgREST allocation to fund an optional administration UI.

Repository paths under `supabase/` are retained as historical source-layout names; they are provider-neutral release artifacts and do not make Supabase the runtime authority. Logical Worker variables named `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` may also remain for compatibility, but their deployed values must resolve only to the intended Miget/PostgREST route and its matching JWT.

The PostgREST database URI must retain `options=-c%20pg_show_plans.is_enabled%3Doff`. Miget preloads `pg_show_plans`; allowing its collector on CloudTMS PostgREST sessions caused repeated memory warnings and severe complex-RPC latency. PostgreSQL query diagnostics use the repository-controlled `pg_stat_statements` extension migration instead. Prove both settings after every new database, restore, service resize, or PostgREST redeployment.

## GitHub Environment setup

Create protected environments named `database-test` and `database-live`. Each holds:

- TEST secret `MIGET_DATABASE_URL_TEST` and LIVE secret `MIGET_DATABASE_URL_LIVE`;
- TEST variable `MIGET_DATABASE_TARGET_TEST` and LIVE variable `MIGET_DATABASE_TARGET_LIVE`;
- required reviewers and branch/tag restrictions appropriate to the environment.

LIVE must require a human reviewer. Do not store a production URL as a repository-wide fallback secret. A new dedicated client database should use its own protected Environment before its first release.

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
