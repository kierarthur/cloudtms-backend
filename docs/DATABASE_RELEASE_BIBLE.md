# CloudTMS Database Release Bible

## Controlling outcome

The repository is the complete, reviewable authority for CloudTMS database structure, RPCs, views, triggers, permissions, RLS posture, and verification. Database releases must be routine, reproducible, evidence-producing, and fail closed. They must never infer that an unknown populated database is safe merely because tables already exist.

This system changes database definitions only. It does not copy or replace customer, candidate, timesheet, finance, provider, payment, Vault, Auth, or other business data.

## Non-negotiable boundaries

- Never run an unreviewed release against LIVE. `PLAN` is read-only. `APPLY` requires the protected `database-live` GitHub Environment and the exact phrase `APPLY LIVE <MODE> <COMMIT_SHA>`.
- The configured database URL must contain the protected Supabase project reference in its host or username. Free-form target URLs are not accepted through the workflow.
- One-time migrations are immutable. Their path and SHA-256 are locked in `supabase/release/migration-lock.json`; changing or removing an existing migration fails CI.
- Repeatables are re-applied only when the SHA-256 of their complete recursive `\ir` include graph changes. Included support pages are part of the authority.
- Never mark a populated database as migrated merely because it has tables. Existing databases enter control only through `ADOPT`, after an exact read-only contract comparison.
- Never manually edit release ledgers, contract hashes, or installed release evidence.
- Preserve CloudTMS Policy X: pre-draft may use live truth; post-draft uses frozen batch artifacts only; `TS_DAY` remains `YYYY-MM-DD`; no post-draft live finance fallback, invented economic-key ladder, or freshness bypass is permitted.
- Preserve the Candidate/MyTMS boundary locked by `supabase/release/protected-boundary-lock.json`. The protected files may not be removed, renamed, duplicated, weakened, or rewritten. The three security verifiers must pass after every release.
- Candidate features, communications, invitations, and autonomous Workbench activity remain disabled on a new database until separately reviewed and deliberately enabled.

## The three supported modes

### NEW

Use only for a genuinely blank Supabase application database. The release tool proves that `public`/`private` contain no CloudTMS relations, installs the signed schema-only baseline, applies repeatable authorities added or changed since that signed baseline, creates release-control tables, creates a fresh database identity, installs safe system defaults, generates a fresh local HMAC secret in that database, runs security verification, and compares the result with the approved contract. It copies no application rows from TEST.

### ADOPT

Use once for an existing CloudTMS database that predates this control plane, including the current TEST and the existing LIVE database. `PLAN` exports its schema/security contract without mutation and must match the approved repository contract exactly. Only then may `APPLY` add release metadata and record the verified current migration/repeatable hashes. There is no blind baselining.

If the contract differs, stop. Investigate and create a reviewed upgrade or a newly approved contract. Never force adoption.

### UPGRADE

Use for a database already carrying `private.cloudtms_database_identity` and the private release ledgers. The tool validates its environment/customer identity, proves that every installed migration still has its original hash, applies only pending one-time migrations, applies only changed recursive repeatable closures, reruns the security verifiers, and requires the installed contract to match the approved contract before recording `VERIFIED`.

## Normal developer workflow

1. Start every database/RPC task with `npm run db:check`.
2. Create a schema/data migration with `npm run db:new:migration -- --name=<short_snake_case_name>`, or a complete replacement RPC/view authority with `npm run db:new:repeatable -- --name=<short_snake_case_name>`.
3. Implement the smallest reviewed change. Never modify an older migration. Function files must contain complete `CREATE OR REPLACE` definitions and preserve their owner, `SECURITY DEFINER`/invoker status, controlled `search_path`, and grants.
4. After the new migration is complete and reviewed, run `npm run db:lock:update`. This only appends new migration hashes; it refuses to rewrite, remove, or re-lock an existing migration.
5. Test in a disposable local Supabase/PostgreSQL database. For a DB-only TEST correction explicitly authorised by the user, install to TEST first, verify it, then ensure the exact installed definition and canonical hash are represented in the repository before publication.
6. Refresh the approved contract only from a verified disposable rebuild containing the intended source: `npm run db:contract:export`.
7. Run `npm run db:check`, the database-release tests, all existing repository tests, the exact security verifiers, and a clean `NEW` replay. Do not publish a change that requires manual ledger repair.
8. Pushes run source verification only. They never alter a database.
9. Use **Database Release (manual and protected)**. Run `PLAN` first. Review the exact commit and outcome. Run `APPLY` only after the appropriate GitHub Environment approval.

## One-button operations

### Upgrade TEST

Run the manual workflow with `environment=TEST`, normally `mode=UPGRADE`, and `phase=PLAN`. If it passes, rerun the same commit with `phase=APPLY` and the displayed exact approval phrase. The protected `database-test` Environment supplies the database URL and project reference.

### Upgrade existing LIVE for the first time

Do not guess that LIVE equals TEST. First run `environment=LIVE`, `mode=ADOPT`, `phase=PLAN`. If the read-only contract differs, stop and review the generated difference; an upgrade plan must be produced from the actual LIVE catalogue. If it matches, an approved `ADOPT/APPLY` installs only the control metadata. Subsequent releases use `UPGRADE`.

### Create a database for a new client

Create the Supabase project and configure its GitHub Environment with the exact database secret and project-ref variable. Run `NEW/PLAN`; it must prove blank. Then run `NEW/APPLY` with the exact commit-bound phrase and optional non-secret customer key. Configure tenant data and enable features only in separate, reviewed onboarding work.

## GitHub Environment setup

Create protected environments named `database-test` and `database-live`. Each holds:

- secret `CLOUDTMS_DATABASE_URL`;
- variable `CLOUDTMS_PROJECT_REF`;
- required reviewers and branch/tag restrictions appropriate to the environment.

LIVE must require a human reviewer. Do not store a production URL as a repository-wide fallback secret. A new dedicated client database should use its own protected Environment before its first release.

The existing repository predates this control plane and already has encrypted `SUPABASE_DB_URL_TEST` and `SUPABASE_DB_URL` repository secrets. The manual release job accepts those exact names only as a transitional fallback when the selected Environment does not yet contain `CLOUDTMS_DATABASE_URL`; GitHub does not permit an existing secret value to be read and copied administratively. The Environment gate, project-reference check, branch restriction and LIVE reviewer still apply. When each database URL is next deliberately supplied by its owner, store it as that Environment's `CLOUDTMS_DATABASE_URL`, verify a `PLAN`, then remove the corresponding repository fallback and workflow expression in a separately reviewed hardening change.

## Contract and evidence

`supabase/release/current-contract.json` is a data-free catalogue contract. It covers extensions, schemas, enum labels, relation/column shapes, defaults, constraints, indexes, RLS flags, effective grants, function signatures and definition hashes, function security/configuration, views, user triggers, policies, and safe application-owned default privileges. It excludes business rows, sequence values, secrets, Vault contents, platform-owned objects, and the release-control tables themselves.

Every successful apply records commit, mode, expected/installed contract hashes, migration hashes, repeatable closure hashes, timestamps, and verifier evidence in private tables inaccessible to browser and service roles.

## Stop conditions

Stop without applying when any of these is true:

- target project reference or environment is absent/mismatched;
- `NEW` finds an application relation;
- `ADOPT` finds any contract difference;
- an installed migration hash differs or its repository file is missing;
- a protected Candidate/MyTMS file differs;
- a verifier fails;
- the final contract differs;
- approval is absent, malformed, or bound to another commit;
- a proposed database change would alter Banking Pay economics, Policy X, Candidate/MyTMS business semantics, or provider/payment behaviour without separate explicit authority.
