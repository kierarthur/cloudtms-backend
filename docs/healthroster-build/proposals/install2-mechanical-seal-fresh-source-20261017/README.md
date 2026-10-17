# Fresh Install2 mechanical contract seal — source proposal only

This packet adds three files. It changes no existing source, immutable migration,
contract, registry, release manifest, protected tooling or Install1 receipt. It
does not execute Docker, PostgreSQL, a sourcePreflight, an API, auth, or a local
business test. Root alone may adopt/review/run it under the existing final clean
PG17 contract-generation exception after the candidate is complete.

The complete original Install1 provider/schema/verifier and every `LOCAL_*`
receipt plus `OWNER_PAUSE_CHECKPOINT.md` are retained byte-for-byte in `parents/`.
`PARENTS.json` binds their saved/index/HEAD bytes and their real capture commit.
`FORWARD.json` supplies sixteen literal raw inverse hunks for the two derived
complete files, plus the independent new-profile hash. `SELECTION.json` names
only three additive, presently absent targets. The protected release scripts
and old 13 October rehearsal entrypoints are not targets.

## Fixed fresh-only identity

| Item | Value |
|---|---|
| Physical repository | `D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026` |
| Branch | `codex/healthroster-install2-14102026` |
| Task label | `hr-i2-seal-20261017` |
| Container | `hr-i2-seal-pg17-20261017` |
| Volume | `hr-i2-seal-data-20261017` |
| Existing image only | `postgres:17.11-bookworm` (`--pull=never`) |
| Published endpoint | `127.0.0.1:55448` only |
| Empty database | `healthroster_install2_seal` |
| Release owner | `cloudtms_hr_install2_seal_owner` |
| Output | `.codex-tmp/healthroster-install2-mechanical-seal-20261017/{PROVIDER,SCHEMA}.json` |
| Canonical temp parent | the same output directory's `tmp/`, on D: |

Both CLIs accept exactly `NEW --candidate=<actual40lowercaseHEAD>`. There is no
CHANGED, retained-checkpoint, record-existing, replay-from, arbitrary target or
arbitrary SQL mode. The candidate must be the actual clean tracked HEAD on this
branch, with D: physical root/Git/common directories; untracked executable
source under scripts/supabase refuses. Retained unrelated ignored work remains.
Root must already have backed up the reviewed candidate. The source checks do
not invent remote-push proof.

Root first inventories Docker/storage/active work as in `COMMANDS.ps1.txt`.
The complete named volume and container must be new and task-labelled, with no
bind mount, no second mount, no host network or privileged mode, and exactly the
fixed loopback publication. Actual Docker image ID must equal the inspected
local image ID. Neither Docker projection requests `.Env` nor prints raw inspect.
The old removed Install1 container is not a usable checkpoint.

The root process supplies ephemeral **local-only** authentication: an admin
`POSTGRES_PASSWORD` injected by environment-variable name into the new container,
and a process-local `PGPASSWORD` for the new nonsuperuser release owner. These
are not application/hosted credentials. Values never enter argv, proof JSON or
console. The fixed `CLOUDTMS_DATABASE_URL` is passwordless and has no query/hash.
The provider creates the owner with the supplied local password through psql
stdin; Docker exec uses the owned container's local Unix socket as postgres,
not the application owner or another container. No value is generated, read or
used while packaging/testing this proposal. Root must not echo/dump environment
or Docker Env when running it. No secret file is introduced.

## Mechanical sequence and boundaries

1. After root's source review/adoption, back up the complete candidate. Check
   actual D: paths, source/Git state, active tasks, disk and Docker inventory.
   Require the two new resource names and fixed output directory to be vacant.
2. Construct only this one new task container/volume from the existing image.
   Preserve PostgreSQL17 and default Docker/PG resources; no tuning of work_mem,
   parallelism, heap, shared buffers, timeouts or global settings is added.
3. Run the new provider with `NEW --candidate=<actual source commit>`. It refuses
   an existing database/role/output, preserves the exact original provider
   prerequisite source and grants, sets the owner NOSUPERUSER/NOBYPASSRLS/
   NOCREATEDB/NOCREATEROLE, and preserves service_role NOBYPASSRLS. It proves the
   empty application/auth/vault scaffold and records only safe local metadata.
4. Run the new schema CLI with the same candidate. It requires the actual same
   container ID/image/profile/provider record, fresh output, PG17/current owner,
   non-superuser/non-bypass, actual JIT/collector off, empty application schemas,
   empty auth.users/vault.secrets and no prior release-control relation.
5. The complete existing canonical algorithm runs: `verifyIntegrity`, NEW
   baseline/bootstrap prefix, current recursive inventory in sqlDateKey order,
   final-definer selection/verification, every pending migration/closure, both
   reader preparation/activation phases, and all three final ACL closures. It
   refuses remaining pending units or source drift. The bootstrap receipt stays
   APPLYING; no VERIFIED/release success is manufactured. This is schema
   generation, not protected verification or local acceptance. Canonical
   compiler/empty-reader preparation calls are retained; it is not a claim that
   only CREATE statements execute.
6. From the same exact source and fixed local profile, run canonical
   `npm run db:contract:seal-local` (no replay argument), then
   `npm run db:contract:verify`. Review every generated data-free contract change.
   Root commits/pushes the reviewed complete contract under its release workflow.
   The seal changes current-contract.json after the schema source checkpoint;
   that intended generated delta is reviewed separately, never hidden in the
   pre-load clean-source proof. Do not run the old Install1 first-use/security
   test programme on this local database.
7. Close the own schema session, clean only the task's container/volume and any
   proved task temp leftovers immediately. Preserve safe receipts and generated
   contract. This is not a hosted rehearsal, Install2, native/API/financial/
   race/security acceptance, F5, feature activation, Worker/Office publication or
   final critic verdict. Those existing requirements are unchanged.

## Original API, memory and temp behavior

The provider retains blocking Docker exec and a 2,000,000-byte capture bound;
errors expose only a fixed code/anchored SQLSTATE. The schema retains the same
`openManagedPsqlSession`: one owned psql connection and writer mutex, default
1,800,000 ms per command, 8 MiB output, 32 MiB input, 10-second close, and existing
own-child failure handling. The library's already-supported process-local
command-timeout override is neither set nor raised here. This is not the small
native fixture holder's 30/45/70/120/180-second envelope or full rehearsal's
no-deadline policy. No new buffer/memory limit is introduced.

The canonical owner mapper still copies the complete source authority to its
original uniquely named temp trees and removes its own trees on process exit.
Only the process-local TEMP/TMP parent is fixed to the new task's D: `tmp/`.
Canonical mapped source and recursive include behavior remain unchanged. Abrupt
termination can leave data-free mapped-source temp directories; cleanup needs
the exact task parent and proof that no own process still needs them. The generic
contract exporter retains its original bounded psql/JSON behavior (256 MiB
stdout cap), with no new exporter/contract hashing algorithm. Existing PG/image
defaults are not asserted as observed current values; root's actual inspection
must record them. No image is built or pulled, no task network/build cache is
created, and no application data/dump is copied.

## Safe non-writing review

From this packet in the author's registered D: worktree:

```text
node --test test.mjs
python verify.py --verify
```

The tests execute only actual new JavaScript bodies in controlled memory
stand-ins; source/root/container/native-query responses are explicitly synthetic.
They cover closed modes/targets, namespace/output/provider collisions, password
non-persistence, owner/JIT/collector/empty-schema refusals, uppercase inventory
and current final-owner/reader/final-ACL choreography, writer/final-source failures,
own-session close and exact unchanged canonical source blocks. They do not run
the helper CLI, inspect Docker, connect to PG or claim native proof.

`verify.py --verify` checks the complete frozen inventory, all raw inverses,
additive vacancies, exact current saved/index/HEAD/historical parents, and Node
syntax only. Do not run `build.py` or `freeze.py` as a review command. Actual
current source can advance; any parent drift is a truthful rebind requirement,
not permission to overwrite that source or reuse a stale mechanical receipt.
