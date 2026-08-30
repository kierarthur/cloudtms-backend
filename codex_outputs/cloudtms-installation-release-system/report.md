# CloudTMS Miget Installation, UPGRADE and NEW-Database Package

## Delivery status — 30 August 2026

This package replaces the Supabase-oriented archive dated 23 August 2026. It documents the current repository-controlled Miget/PostgREST release model and contains no secret values.

## Use

Give Codex exactly one of these read-only planning commands:

```text
CLOUDTMS PLAN UPGRADE <exact target>
CLOUDTMS PLAN NEW DATABASE <agency>
```

The package does not itself authorise APPLY, deployment, paid resource creation, secret changes, feature activation or resource reallocation.

## Accurate current scope

- Database definitions are controlled by migrations, repeatables, locked hashes, the current contract and protected GitHub PLAN/APPLY workflow.
- Existing managed databases use `UPGRADE`; the first deliberate promotion of historical LIVE uses `LEGACY_UPGRADE`; a proved-blank database uses `NEW`; exact pre-control databases may use `ADOPT`.
- Each agency keeps isolated PostgreSQL credentials, PostgREST JWT and Worker/gateway configuration while services may share the purchased Miget Resource.
- Repository directories/variables retaining “Supabase” in their names are compatibility names only and are not runtime authority.
- Miget is the sole current database provider. No release or runtime path may contact Supabase; historical names remain only where renaming would break repository compatibility.
- LEGACY_UPGRADE is resumable rather than globally atomic. After any fail-closed stop, Codex must inspect the fresh Miget state, rerun PLAN, and complete both the original-data non-superuser rehearsal and the exact interrupted-state schema/ACL rehearsal before one corrected retry.
- Routine-security fingerprints use explicit PostgreSQL catalogue schemas and names, never `regprocedure` display text that varies with `search_path`.
- The package does not claim that nonexistent generic `install:*` commands automate the full estate. Codex follows the manual and the implemented protected database workflow, then separately verifies PostgREST, gateways, Workers and application behaviour.

## Contents

1. `CLOUDTMS_INSTALLATION_AND_RELEASE_MANUAL.md` — exact Codex request language, PLAN/APPLY boundary and end-to-end Miget operational sequence.
2. `DATABASE_RELEASE_BIBLE.md` — repository's current authoritative database modes, workflow and fail-closed rules.
3. `DATABASE_CONFIGURATION_BIBLE.md` — provider-neutral configuration preservation/onboarding rules.
4. `report.md` — this package record.

Always compare the target with the current approved Git commit. This ZIP is a durable operating guide, not a frozen substitute for current repository state.
