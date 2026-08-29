# CloudTMS database release system — implementation report

## Outcome

A fail-closed, data-free database release system has been implemented in an isolated worktree. It supports:

- `NEW`: construct a genuinely blank Supabase database from the signed current baseline;
- `ADOPT`: bring an existing database under release control only after an exact read-only contract match; and
- `UPGRADE`: apply only pending immutable migrations and changed recursive repeatable closures to an already managed database.

Ordinary push/PR CI is now source verification only. Database mutation is available only through the manual, protected, two-phase `Database Release` workflow. No hosted TEST or LIVE database was changed by this implementation and no Worker was deployed.

## Authority captured

The schema-only baseline contains no customer, candidate, timesheet, finance, payment, Auth, Vault, sequence-value or other business data. It captures current application authority including:

- 38 application enum types;
- 181 application tables plus application sequences and schemas;
- 1,015 CloudTMS application functions, split into bounded baseline pages;
- 28 application views;
- 264 application triggers;
- defaults, constraints, indexes, ownership, RLS flags, effective object ACLs and safe application-owned default privileges; and
- 34 retained dropped-column physical slots required to preserve current PostgreSQL composite-row/attribute-number compatibility.

The final read-only TEST comparison also includes Supabase/platform-supplied public/private routines. It proved exact parity between current TEST and the disposable rebuild for:

- 220 public/private application relations, including physical column positions and RLS flags; and
- 1,234 public/private routines, including identity, execution/security properties and normalized definitions.

During final verification, TEST gained `timesheet_break_entry_mode_enum` and two columns from the independently published migration `22082026_1551_timesheet_break_entry_mode.sql`. The release source detected the drift, incorporated the exact committed migration blob, updated the baseline and contract, and then re-proved exact parity.

## Safety controls

- `NEW` refuses every non-empty `public`/`private` application schema.
- `ADOPT` cannot blind-baseline a populated database; the complete data-free contract must already match.
- The target environment and hosted Supabase project reference are checked before database access.
- `APPLY` requires an exact environment/mode/40-character-commit approval phrase.
- LIVE and TEST use separate protected GitHub Environments; LIVE is intended to require a human reviewer.
- Published migration hashes are immutable. Lock updates append new migration authority and refuse changed or missing historical migrations.
- Repeatables are hashed across their complete recursive `\ir` include closure; unchanged closures are skipped.
- Changed Banking Pay repeatables run the existing rollback-only catalogue rehearsal before installation.
- The Candidate/MyTMS protected-boundary lock remains unchanged and the three security verifiers run after every release.
- Final success requires exact contract equality; a partial application cannot be labelled verified.
- Release identity, commit, expected/installed hashes, mode, status and evidence are stored in private, browser-denied control tables.
- Policy X, payment economics, provider behaviour and application business data are outside this change.

## Durable workflow for future changes

1. Read `docs/DATABASE_RELEASE_BIBLE.md` and run `npm run db:check`.
2. Create a new one-time change with `npm run db:new:migration -- --name=<name>`, or replacement RPC/view authority with `npm run db:new:repeatable -- --name=<name>`.
3. Implement and review the narrow change. Never edit an older migration.
4. For a new migration, run `npm run db:lock:update` after the file is complete; the command cannot bless a changed historical migration.
5. Prove the change in a disposable database, refresh the approved contract from that proven database, and run the complete test/verifier/clean-NEW gates.
6. Publish source through normal repository review. A push never alters a database.
7. Run the manual protected workflow with `PLAN`; review the exact commit and target. Then run the same release with `APPLY` and the exact approval phrase.

These rules are mandatory in repository `AGENTS.md`, which directs every future task to the Database Release Bible.

## Verification completed

- Full backend suite: 739/739 passed.
- Release, migration-ledger and Banking Pay catalogue tests: 13/13 passed.
- Source integrity: 185 migrations and 298 recursive repeatable roots passed.
- Exact protected Candidate/MyTMS boundary hashes passed.
- Exact Candidate/MyTMS/general security verification SQL passed on the disposable rebuild.
- Blank disposable Supabase `NEW/PLAN`: passed.
- Blank disposable Supabase `NEW/APPLY`: verified.
- Repeating `NEW` on the populated rebuild: refused as required.
- Managed no-change `UPGRADE/PLAN` and `UPGRADE/APPLY`: verified.
- Final repository contract hash: `56ba6142493fdda31b4ac8e5c851d7f9782300ff30ac4bf6bb423f62340e2474`.
- Final TEST/disposable relation and routine hashes: exact matches.

## Repository state and external-state confirmation

- Work was built and verified on branch `codex/db-release-system-20260822` in a separate worktree before integration with the latest `test` authority. The final published commit and workflow evidence are recorded in the task handoff.
- Hosted TEST was queried read-only for catalogue comparison only.
- LIVE was neither inspected nor changed.
- No application data, feature flag, invitation, communication, payment, provider or Banking Pay state was mutated.
- No secret was written to source or included in this report.

## First use for LIVE

LIVE has intentionally not been assumed to match TEST. Its first operation must be `LIVE / ADOPT / PLAN`, which is read-only. If the contract differs—as is plausible for a database not upgraded for months—the system will stop and identify the differing contract sections. A separate reviewed legacy alignment release must then be created from the actual LIVE catalogue. It is deliberately unsafe to invent or automate that unknown first alignment without inspecting LIVE. Once aligned and adopted, all later changes use normal `UPGRADE` releases.
