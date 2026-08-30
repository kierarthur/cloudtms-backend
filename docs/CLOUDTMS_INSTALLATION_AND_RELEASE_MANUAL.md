# CloudTMS Installation, UPGRADE and NEW-Database Manual

## Purpose

This manual is the durable operating entry point for moving an existing CloudTMS database to the current approved repository release or creating a new isolated agency database on Miget.

It is deliberately simple:

- Git is the authority for database definitions and application source.
- Miget supplies isolated PostgreSQL and PostgREST services on the shared purchased Resource.
- Cloudflare Workers and gateways supply the application routes.
- Codex prepares a read-only plan first.
- A database or infrastructure change happens only after a separate, target-specific APPLY instruction.

The directory name `supabase/` and logical variables such as `SUPABASE_URL` are retained for source compatibility. They do not make Supabase the runtime provider. Their deployed values must resolve only to the intended Miget/PostgREST route.

## The two commands to give Codex

Use one of these exact requests:

```text
CLOUDTMS PLAN UPGRADE <exact target>
CLOUDTMS PLAN NEW DATABASE <agency>
```

Examples:

```text
CLOUDTMS PLAN UPGRADE CloudTMS LIVE
CLOUDTMS PLAN NEW DATABASE Agency D TEST
```

Both commands are read-only. They tell Codex to inspect the named target, compare it with the exact current repository authority, list every pending database and runtime change, and stop before mutation.

Words such as “upgrade”, “prepare”, “new database” or “what is missing” without the complete canonical command are discussion only. They do not authorise resource creation, database changes, deployment, secret changes, feature activation, paid spend or resource reallocation.

## What Codex must read before planning

Codex must read the applicable workspace and repository `AGENTS.md` files and then inspect the current versions of:

- `docs/DATABASE_RELEASE_BIBLE.md`;
- `docs/DATABASE_CONFIGURATION_BIBLE.md`;
- `.github/workflows/database-release.yml`;
- `scripts/cloudtms-db-release.mjs` and its library;
- `supabase/release/current-release.json`;
- `supabase/release/current-contract.json`;
- `supabase/release/migration-lock.json`;
- all current one-time migrations, repeatables and verification SQL;
- the exact Worker, gateway and PostgREST source/configuration for the target; and
- the exact frontend/backend Git heads intended for deployment.

Never rely on an old copy of this ZIP as proof of what is currently pending. The plan must always calculate from the current approved Git commit and current target evidence.

## How TEST changes remain known

Every database-definition change must be represented in Git:

- one-time structure/data transitions are immutable files under `supabase/migrations`;
- functions, views and other replacement definitions are complete authorities under `supabase/repeatable`;
- one-time migration hashes are locked in `supabase/release/migration-lock.json`;
- the approved data-free schema/security contract is `supabase/release/current-contract.json`; and
- the protected release records installed migration and repeatable hashes in the target database ledgers.

Normal pushes verify source but do not alter a database. Direct, undocumented changes to TEST are forbidden because they would bypass this record. A plan compares the target ledgers/catalogue with the current Git authority and therefore reports the exact missing migrations and new or changed repeatables.

## Planning an UPGRADE

An UPGRADE deliberately brings the selected existing database to the current approved repository release. It is not merely a hosting move.

Codex must:

1. Identify the exact Miget project, Resource, PostgreSQL service, database, PostgREST app, gateway, Workers, repository branches and deployed source identities.
2. Confirm backup/recovery evidence appropriate to the operation.
3. Fetch the approved repository head without altering a dirty user worktree.
4. Run `npm run db:check` from a clean copy of that exact commit.
5. Inspect the target's release identity, ledgers, catalogue, RLS, grants, functions, triggers, extensions and security posture through authorised read-only routes.
6. Select the correct mode:
   - `UPGRADE` for a database already managed by the private CloudTMS release ledgers;
   - `ADOPT` only where an existing database exactly matches the approved contract and needs control metadata; or
   - `LEGACY_UPGRADE` for the first deliberate schema promotion of the current historical LIVE database.
7. Run the protected workflow in `PLAN` and report the exact commit, mode, installed/pending counts, changed repeatables, contract outcome and runtime work.
8. Include any PostgREST redeployment, gateway/Worker configuration, branch deployment, cron, health, security and application acceptance checks.
9. Stop and wait for a separate APPLY instruction.

Moving LIVE hosting from Supabase to Miget did not upgrade LIVE to the TEST schema. Its first deliberate promotion remains `LEGACY_UPGRADE`. After that succeeds and installs managed identity, later releases use `UPGRADE`.

## Planning a NEW database

NEW means a genuinely blank, isolated agency database. It does not mean cloning TEST business data.

The plan must include:

1. the exact agency and environment identity;
2. a separately credentialed PostgreSQL service on the approved shared Miget Resource;
3. explicit RAM and storage allocations that fit the Resource, while retaining pooled CPU behaviour;
4. an independently credentialed PostgREST app and JWT secret;
5. the repository-controlled compatibility gateway and Worker configuration;
6. protected GitHub Environment secret/target-locator names;
7. `NEW/PLAN`, proving that the application database is blank;
8. `NEW/APPLY`, installing the repository schema, repeatables, release metadata and verifiers only after separate approval;
9. target-specific baseline, tenant and initial-user configuration;
10. disabled send/provider/payment/autonomous feature states until separately reviewed;
11. backup/recovery, health, login, representative RPC and security acceptance checks; and
12. exact source/deployment identity evidence.

Resource creation that incurs cost requires the user's explicit cost approval. NEW copies no TEST candidates, clients, users, sessions, timesheets, invoices, payments, provider state, audit history or other operational rows.

## Miget and PostgREST requirements

Every agency keeps separate PostgreSQL credentials, PostgREST JWT, gateway configuration and Worker secrets even when services share the purchased Miget Resource.

Mandatory safeguards:

- run `supabase/release/30082026_0030_miget_auth_compatibility_bootstrap.sql` first for LEGACY_UPGRADE and NEW so historical foreign keys and JWT claim helpers have their provider-neutral `auth` compatibility objects; it must not copy application users or replace `public.tms_users`;
- for LEGACY_UPGRADE, run `supabase/release/30082026_0321_legacy_pay_batch_status_helper_preload.sql` before `supabase/repeatable/08042026_1151_newtablesbanking.sql`, then run the same exact idempotent file again as the final repeatable preload before convergence; interrupted/historical LIVE can retain the pay-summary trigger while lacking its status helper, the Banking bootstrap fires that trigger, and a later historical migration can remove/replace the helper;
- run `supabase/release/30082026_1038_legacy_temp_diag_log_preload.sql` before that Banking bootstrap and again in the repeatable-preload phase; it restores the exact inert/service-only diagnostic dependency that current triggers can call after the historical migration chain removes it, without enabling diagnostics or writing an audit row;
- derive every LEGACY general/Candidate routine and ACL fingerprint only from a fresh ACL-preserving restore of the exact hosted checkpoint after applying the complete current preload/repeatable order; build routine identity from explicit function/type catalogue schemas and names rather than the `search_path`-dependent `regprocedure` display; the sealed general inventories include `_temp_diag_log` and the canonical Candidate profile includes exactly five repository-authorised service grants installed by the current ordering, while a disposable database already changed by a later failed rehearsal must never be used to bless another profile;
- replace immutable Supabase migration `26072026_1219_disable_runtime_plpgsql_check.sql` during LEGACY_UPGRADE with `supabase/release/30082026_0958_legacy_plpgsql_check_database_defaults_replacement.sql`; when the provider exposes the seven settings it applies and catalogue-verifies them on `current_database()`, while Miget's exact unavailable state is accepted only when the `plpgsql_check` extension is absent, no `plpgsql_check.*` runtime setting exists and no persisted database override exists; partial/mixed states and ignored permission failures are forbidden;
- provider mapping may omit only the seven exact unsupported `plpgsql_check` diagnostic function settings, may never cross a SQL-statement semicolon or remove a function body/owner/grant/search-path/timeout, and the Banking Pay catalogue pre-apply must resolve every include through the same provider-mapped executable tree in repository release order; prove this with a non-superuser disposable replay before hosted APPLY;
- generated catalogue SQL must use the PostgreSQL syntax construct `overlay(...)`, never the invalid schema-qualified form `pg_catalog.overlay(...)`; source tests must forbid the invalid prefix and the disposable replay must execute the generated SQL;
- the provider-neutral catalogue hash may reconstruct only a manifest-declared suffix of the seven exact `plpgsql_check` settings for comparison with an existing sealed Banking manifest; validate parameter name, value, suffix order and the canonical `AS` insertion point, while the installed Miget routine remains diagnostic-free and its identity/body/non-diagnostic configuration/ACL remain exact;
- replace immutable migration `23082026_1337_manager_email_candidate_identity_defaults.sql` during LEGACY_UPGRADE with `supabase/release/30082026_1155_legacy_manager_email_template_defaults_rls_replacement.sql`; Miget's non-superuser owner cannot bypass the preceding table's forced RLS, so the replacement must verify the exact owner/table/RLS/no-policy shape, temporarily remove only `FORCE` inside the same atomic transaction, preserve the original conditional template update, restore `FORCE`, and verify the final state;
- rehearse any migration that writes after forced RLS as a real non-superuser owner; a PostgreSQL superuser-only rehearsal is not provider-parity evidence;
- compare pre/post business data separately from repository-authorised seed data: `26082026_2057_candidate_system_actor_seed.sql` may add exactly one inactive non-login/non-payment MyTMS Candidate system actor only when the configured system actor is absent, and the manager-email migrations may add their exact versioned default-template rows; still prove existing Client, Candidate, Timesheet, Contract, invoice and payment row sets were not deleted or rewritten;
- a resumed LEGACY_UPGRADE may accept `public.schema_repeatables` only when the earlier structural bootstrap created its exact three-column/default/primary-key shape and it still contains zero rows; any other shape or any row remains ambiguous and the release must stop without clearing it;
- a hosted LEGACY_UPGRADE stop can leave already-completed migration files and public migration-ledger rows committed even though managed adoption has not occurred; re-inspect the hosted ledgers/identity/catalogue and rerun PLAN after every stop instead of reusing the original plan;
- before a resumed hosted APPLY, require one complete non-superuser PostgreSQL 17 run from the preserved encrypted pre-upgrade data checkpoint plus a fresh schema/ACL clone of the exact interrupted hosted state; never weaken `FORCE ROW LEVEL SECURITY` to make the provider owner dump customer rows, and copy only non-customer release metadata into the schema-only rehearsal;
- Candidate-named browser isolation accepts only the exact provider-neutral canonical current fingerprint: both the preserved clean rehearsal and the hosted interrupted checkpoint must reach the same 85-routine/eight-service-omission service/ACL matrix before browser execution is removed; historical `regprocedure` display hashes and every unexplained ACL shape fail closed;
- for LEGACY_UPGRADE only, run `supabase/release/30082026_0055_legacy_workbench_shared_context_bootstrap.sql` after the legacy Banking table bootstrap and before repository migrations; it must prove the required columns and zero duplicate open shared contexts, reject an unexpected same-named index, and create only the exact replacement unique index required before the old actor-scoped index is removed;
- treat `supabase/release/24082026_1128_legacy_upgrade_trigger_shims.sql` only as a one-time bridge for historical LIVE function deployments omitted from `public.schema_migrations`; all callable temporary shims must remain non-executable to browser/service roles, carry `CLOUDTMS_LEGACY_TRANSITION_SHIM`, and be replaced by current repeatables before contract verification and atomic adoption can pass;
- append `options=-c%20pg_show_plans.is_enabled%3Doff` to each CloudTMS PostgREST `PGRST_DB_URI`, preserving every other URI component;
- verify the live PostgreSQL memory profile and volume after any Miget resize;
- map only the audited restored logical owner to `CURRENT_USER`; never assume `SET ROLE postgres` works;
- reapply and verify audited owner default privileges after a restore;
- install and verify `cloudtms_miget_service_owner_all` for `CURRENT_USER, service_role` on every RLS-enabled public table exposed to the Worker;
- run role-specific private-helper ACL verifiers after final repeatables, without granting browser roles access to `private`;
- expose `/rest/v1` and `/rest/v1/rpc` only through the correct independently credentialed PostgREST/gateway route;
- prove a real PostgREST service-role lookup and representative RPC, not only an owner SQL query; and
- keep gateways as repository-controlled source under `infra/miget`.

The permanent read-only ChatGPT connector is **CloudTMS Miget Operations**. It is an audit route, not an APPLY route. The credential-protected browser SQL/table viewer is the repository-documented pgAdmin service. Neither replaces the protected database-release workflow.

## Worker and application deployment

The plan must name every affected Worker and prove its repository, branch, working directory/root, deploy command, managed build-token selection, bindings and secrets by name only.

Secondary Workers use dedicated `deploy/cloudflare/<worker>` branches. A prepared branch is not proof of a Workers Builds connection. Verify the actual connection and the resulting deployment identity. Preserve the Candidate deployment order: normal backend, private Worker, synthetic Worker, public broker last.

For an existing LIVE Worker, preserve the target's current provider, session, download, stationery and other target-owned variable/secret values. Never copy TEST values into LIVE. Compare variable and secret names before and after deployment, use Wrangler's `--keep-vars` safeguard when the repository does not declare every retained LIVE variable, and stop if a required LIVE binding or secret name is missing. New capabilities must deploy disabled-first until their separate Worker topology, bindings, credentials and activation authority have been proved; installing current source is not permission to enable sends, providers, payments, Candidate routing, Google switching or background drains.

After deployment, require bounded evidence for:

- Worker version/deployment identity and effective route;
- PostgREST and gateway health;
- database identity and installed ledgers;
- login and a representative read-only application route;
- scheduled-trigger presence where required;
- absence of legacy Supabase runtime hostnames; and
- no secret values in logs or reports.

## APPLY is a separate instruction

After reviewing a locked PLAN, the user must explicitly name the exact target and plan and authorise APPLY in the current task. The protected workflow additionally requires the exact commit-bound phrase:

```text
APPLY <TEST|LIVE> <NEW|ADOPT|UPGRADE|LEGACY_UPGRADE> <40-character commit SHA>
```

An APPLY for one database, environment or stage never authorises another. If the commit, target, database identity, plan evidence or required configuration changes, the plan expires and must be rebuilt.

## Required post-APPLY proof

Codex must not call the operation complete until it proves, without exposing sensitive data:

- the workflow succeeded at the exact approved commit;
- migration and repeatable ledgers match Git;
- the installed contract and security verifiers pass;
- RLS, grants, owner/default privileges and private-helper ACLs are correct;
- PostgREST has the plan collector disabled and is healthy;
- gateways and all affected Workers run the intended source;
- every Worker that fetches a public `*.workers.dev` Miget gateway retains `global_fetch_strictly_public`; prove a harmless gateway-backed database read because health/ready alone can pass while Cloudflare rejects the real hop with error `1042`;
- login, representative table/RPC routes and required crons work;
- observe each required cron on the final active Worker version with `outcome=ok` and no exceptions; for LIVE this includes the legacy `arthur-rai-broker` five-minute Sheets outbox retry as well as the main one-minute/five-minute schedules;
- configuration and activation states match the reviewed target plan;
- backup/recovery evidence is still valid; and
- no traffic for the migrated target is routed to Supabase.

## Stop conditions

Stop before APPLY if the target is ambiguous, the plan is stale, a cost is unapproved, a backup cannot be verified, the database is not blank for NEW, an installed migration hash differs, an unexpected owner/grantee appears, the final contract differs, a required verifier fails, a Worker connection is unproved, a secret would need to be exposed, or the requested work would broaden into another database/environment.
