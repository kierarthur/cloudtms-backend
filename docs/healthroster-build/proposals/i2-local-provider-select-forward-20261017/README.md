# Fresh local provider base-table SELECT repair

Source-only one-target successor for `scripts/healthroster-install2-local-provider.mjs`, complete parent at `96e4ebffddf38a2123d3ccc6d94f21735fa7e422`. Apply only the one SELECTION target after independent review; do not execute or transplant the reference files. All schema/release SQL, shared libraries, profile, hosted grants, old Install1 provider, role/physical/source/vacancy/PG17/NOBYPASS checks and output freshness remain unchanged.

The unchanged local fixture creates `auth.users` and `vault.secrets` while the provider's Docker psql caller is `postgres`. It declares no table GRANT or ALTER DEFAULT PRIVILEGES. The distinct fixed schema owner is created NOSUPERUSER/NOBYPASSRLS/NOCREATEDB/NOCREATEROLE. Existing REFERENCES on `auth.users` and SELECT on the separate `vault.decrypted_secrets` view do not grant SELECT on the two base tables. The actual schema's mandatory pre-load emptiness query reads both base tables as this fixed local owner, before its first application checkpoint/LOCAL LOAD.

PostgreSQL's [privilege documentation](https://www.postgresql.org/docs/17/ddl-priv.html) distinguishes SELECT from REFERENCES and assigns object ownership to its creator; tables have no default PUBLIC privileges. The fixture and the provider's fresh database/role/container/volume requirements therefore expose a concrete source prerequisite gap. Actual R3's closed log records only `CONNECTION_CLOSED (42501)` and no failing object. The repair is justified by source; it does not claim the exact failed relation or a successful native reproduction.

Three exact source hunks, all in this one provider:

1. Add `GRANT SELECT ON auth.users,vault.secrets TO cloudtms_hr_install2_seal_owner` through the existing fixed `${owner}` binding. No PUBLIC/browser/service role grant, ALL, grant option, default privilege, schema mode or arbitrary target is added.
2. Add four safe booleans to the existing future provider identity query/record: `ownerAuthUsersSelect`, `ownerVaultSecretsSelect`, `authUsersEmpty`, `vaultSecretsEmpty`. These inspect privileges and absence only; no rows/secret contents leave SQL.
3. Require those four observations to be actual booleans true before the provider writes its original record. Existing original identity checks remain unchanged. The source/test packet is not an observed provider result; only a future actual run can produce these facts.

The original scaffold's empty application/provider checks still run. This patch does not seed tables, copy users/secrets, change hosted source or privileges, read credentials, execute a schema, accept a release or relax any guard.

Safe finite replay from a registered D: worktree:

```
node --experimental-vm-modules --test <this-folder>/test.mjs
python -B <this-folder>/grammar.py
python -B <this-folder>/verify.py --current
node --check <this-folder>/payload/healthroster-install2-local-provider.mjs
```

Do not invoke the payload as a programme. Tests evaluate the complete provider source in a VM with synthetic process/environment, fake Docker return records and an in-memory filesystem; they never invoke Docker/psql/SQL/network or the real profile/source guards. Nine suites cover full inverse, old/new grant gap, four future observation checks, original identity/freshness/source guards and closed SQLSTATE failure. PG17.7 parsing proves the one exact two-table SELECT delta, four retained grant statements, five grant-surface mutations refused, thirteen unchanged fixture statements/no fixture grants/default ACL and the exact new identity SELECT. This is syntax/source evidence, not PostgreSQL privilege execution or PG17.11 native proof.

Five complete actual source parents, the unchanged safe811-byte R3 log and a full before/after inverse are captured. `verify.py --current` requires saved/index/HEAD to equal each exact pre-mount parent and will correctly hold after adoption; plain `verify.py` retains the historical inverse proof. All R3 rollback/cleanup/resource evidence remains root-owned and unchanged. No native/Docker/auth/API/remote execution, tracked write, commit/push or root/Report mutation occurred while preparing this packet.
