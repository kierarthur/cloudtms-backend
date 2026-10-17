# Local mechanical provider TCP/admin environment correction — source only

The original45 and exact-database21 packets remain immutable. Root's two local
pre-schema HOLDs are not promoted to passes: the first stopped at Config.Cmd,
the second reached the provider fixture's local guard. Root reports both own
containers/volumes removed, no schema compilation/output/contract generated.
This packet performs no Docker, process, database, auth or source admission.

The complete original provider fixture requires `inet_server_addr() IS NOT NULL`.
The new provider's Unix-socket invocation was incompatible with that existing
guard. Exactly one runtime target now changes, through three literal hunks:

1. Require the root-supplied ephemeral local admin POSTGRES_PASSWORD before any
   Docker metadata/SQL, keeping the existing separate owner PGPASSWORD rule.
2. Use `docker exec -i --env PGPASSWORD <exact-owned-container> psql -h 127.0.0.1
   -U postgres ...`. The flag carries the variable name only, never its value.
3. Supply that own Docker CLI child with `env:{...process.env,
   PGPASSWORD:process.env.POSTGRES_PASSWORD}`. The parent node process's owner
   PGPASSWORD stays unchanged; the role CREATE continues to use the owner value
   through SQL stdin. No value enters argv, receipt, console or a new file.

The complete fixed profile/schema are unchanged exact current parents. Docker
owned-image/container/volume/loopback/resource guards, local banking_modal_v2_test,
owner/PG17/fresh source/output rules, canonical schema algorithm and all capture
bounds remain unchanged. No fixture guard is relaxed and no extra SQL is added.

The complete review-only COMMANDS.ps1.txt adds exactly the missing literal
`postgres` command element after the image. This makes Config.Cmd match the
unchanged container guard. Its entire predecessor is retained, with one exact
inverse. Frozen45/21 command documents are historical and not overwritten.
Root's actual driver correction/journals and real preflight remain root-owned.

Safe non-writing review:

```text
node --test test.mjs
python verify.py --verify
```

The tests execute the actual new provider only in explicit memory stand-ins.
They check exact TCP/name-only env argv, distinct admin/owner canaries, parent
owner preservation, neither value in emitted data/argv, early missing-admin
refusal, closed SQLSTATE and exact full raw inverses. Synthetic responses never
qualify a container, credential, fixture, schema or contract. Verification checks
only current/historical/index/saved public parents, frozen inventory and syntax.
No helper/command execution during peer review; build.py/freeze.py are author-only.
