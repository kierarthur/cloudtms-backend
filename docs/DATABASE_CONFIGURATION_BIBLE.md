# CloudTMS Database Configuration Bible

## Controlling outcome

Database structure and application configuration are related but different authorities. A structurally successful Miget NEW or UPGRADE release is not operationally complete until the target's reviewed configuration is present and verified.

Configuration handling must preserve agency isolation and existing target choices. It must never copy TEST operational data or secrets into another database.

## Configuration classes

Classify every configuration item during the read-only plan:

1. **Repository/system owned** — stable catalogues, safe defaults and semantic settings controlled by migrations/repeatables. Update only through reviewed Git authority.
2. **Environment dependent** — public origins, environment labels, Worker routes, generated secrets and provider endpoints. Transform or generate for the exact target; never copy TEST values.
3. **Agency/tenant owned** — client, hospital, mapping, rate, branding, finance-window and similar business configuration. Preserve on UPGRADE; collect explicitly for NEW.
4. **User owned** — preferences, saved views/reports, signatures and user-specific delivery settings. Preserve existing values; do not replace them from TEST.
5. **Protected/external** — passwords, JWTs, HMAC material, provider credentials, database URLs, webhook secrets and other protected values. Record names and presence only. Provision through approved secret stores, never Git or a plan.
6. **Operational data** — candidates, clients, timesheets, invoices, banking/payment/provider state, sessions, audits and histories. Never treat this as configuration and never copy it from TEST for NEW.

## UPGRADE rules

An UPGRADE preserves business rows and target-owned configuration.

- Schema migrations may add required columns/tables/defaults, but must be idempotent and reviewed.
- Replacement definitions come from the current repeatable authority.
- Existing agency and user values remain unless the reviewed plan explicitly identifies a safe repository-owned transition.
- Missing environment-dependent values are generated or transformed for that target.
- Secret values are never read back into the plan.
- Feature, send, provider, payment and autonomous-work states remain as reviewed for the target; a schema upgrade alone never activates them.

Current LIVE's first deliberate schema promotion is `LEGACY_UPGRADE`. That changes database definitions only as described by the locked plan; it does not authorise copying TEST configuration over LIVE.

## NEW rules

A NEW database starts blank and independently credentialed. The protected `NEW` release installs structure, repeatables, release metadata, safe database-local authority and verification. The onboarding stage then supplies the reviewed configuration required by that agency.

Before declaring the new agency operational, the plan must prove:

- required universal catalogues/defaults are present from repository authority;
- environment values identify the new agency/environment, not TEST;
- tenant configuration has been explicitly supplied or marked not applicable;
- initial users and their permissions were explicitly reviewed;
- protected values exist in approved secret stores without being exposed;
- PostgREST, gateway and Worker credentials are isolated from other agencies;
- send/provider/payment/autonomous features remain disabled until separately authorised; and
- login and representative read-only workflows pass.

## Recording future TEST changes

The database does not teach the upgrader by being edited directly. Git is the record.

When a TEST change adds or changes a configuration-bearing table, column, default, function, view, trigger, RLS policy, grant or extension:

1. start with `npm run db:check`;
2. create a new immutable migration or complete repeatable authority as appropriate;
3. add or update focused tests and security verification;
4. append the new migration hash with `npm run db:lock:update`;
5. refresh/verify the data-free database contract when its covered catalogue changes;
6. publish the exact reviewed Git commit;
7. use the protected TEST PLAN/APPLY workflow; and
8. prove the target ledgers and installed definition hashes match that commit.

Manual database edits that are not represented in Git are drift and must not become the source for a later upgrade.

## Planning evidence

For `CLOUDTMS PLAN UPGRADE <exact target>` or `CLOUDTMS PLAN NEW DATABASE <agency>`, Codex must return one bounded requirements list containing:

- exact target/database/environment identities;
- pending migrations and changed repeatables;
- safe configuration items to add/update/preserve;
- environment transformations;
- required tenant/user inputs;
- protected variable/secret names and presence status only;
- activation decisions;
- PostgREST/gateway/Worker configuration work;
- verification and recovery steps; and
- all blockers.

No configuration mutation occurs during PLAN.

## Verification

After an authorised APPLY/onboarding stage, verify configuration through bounded catalogue queries, row-key/count checks, safe semantic hashes and presence-only checks for protected values. Do not export full user, provider, payment or operational payloads.

Stop if required configuration is missing, a target-owned value would be overwritten without approval, a protected value would be exposed, an environment value points at TEST/another agency, or activation would occur implicitly.
