# Proposed QT whole-row binary SHA256 detector — source only

This is a six-file successor to the exact current R6 QT adapter, not an installed change or a QT acceptance result. The proposal hashes the whole audit row inside a TimeZone-bearing record:

```sql
pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone'))))
```

Both snapshots still read every `public.audit_events` row, with the original `LIMIT 1500001` used only to detect and refuse a population above 1,500,000. Each stores the original `(event_sequence,row_digest)` pair. Every digest now must be exactly 32 bytes. Counts and bidirectional `EXCEPT ALL`, the existing unique identity index guard, local `enable_hashagg=off` and restoration of its prior setting remain intact. No sampling, row predicate, duplicate removal, monetary interpretation or public success field is introduced.

`FORWARD.json` selects only `qt-assertions.sql`, `input-builder.mjs`, `result-validator.mjs`, `PROGRAMMES.json`, `OFFLINE_INPUTS.json`, and their adapter `MANIFEST.json`. Its complete parent commit is `1c60802423f854ea7664051ba4b82d2d0e3e9ca8`; capture also checked the actual later root HEAD. All 52 adapter parent images, including 46 unchanged companions, and three complete shared source owners match saved/index/HEAD/history. The original source guard, setup, protected release, holder, reader, all twelve restoration maps, background attribution, WeakMap qualifier, actual controls and limits remain byte-identical.

The fixed encoder qualifier runs inside the existing admission and equality DOs. It checks the exact 16 ordered attribute names/types/identity, six builtin send/output bindings, the named audit composite's `pg_type.typsend -> pg_catalog.record_send(record)` path, direct record/SHA256/current_setting signatures, executable privileges, internal symbols, owner, strictness/volatility/parallel metadata, UTF8 server/client encoding and PostgreSQL 17.11. The original heap/RLS/owner/policy shape is retained from the accepted Cost qualifier. Unknown/NULL qualification refuses explicitly. This is a technical representation prerequisite, not new business authority or a general builtin/purity policy.

The SQL has the same eight top-level statements and four complete DOs; the full native programme remains 56 statements, the main input 69 and fresh verification four. Response indices/shapes, CTAS 0..1,500,000 rowCount handling and all other native envelope checks remain unchanged. Only the exact two CTAS hashes and three changed DO hashes are regenerated. No actual source-admission, buildInput, buildProgram, SQL, EXPLAIN, provider or native call was made by this packet.

Safe non-writing replays from the registered Money D: worktree:

```powershell
node --test <packet>/typed-model.test.mjs <packet>/source.test.mjs
python -B <packet>/pg17-proof.py
python -B <packet>/catalogue-source-proof.py
node <packet>/verify.mjs
```

Use the installed Python with the existing approved parser path recorded inside `pg17-proof.py`; it reports parser PostgreSQL 17.7 honestly. Do not run author-only `prepare.mjs` or `source-basis.py` after freeze. Verification writes no results and performs no admission. Its current-parent check is intentionally pre-adoption: after root changes the six actual files, an old-parent current check must refuse. `MANIFEST.json` inventories every saved packet file except itself, including nested adapter manifests and upstream sources.

The synthetic typed encoder is a reference-framing test, not a PostgreSQL byte oracle. The source proof establishes the representation relationship described in `DESIGN.md`, subject to the stated cryptographic collision assumption. Actual changed-query plan, all-row cost, resource use, native result ABI, whole-table completion and QT acceptance remain **NOT_PROVED**. Root owns independent peer, actual committed adoption/admission and any bounded native trial. No original failure, old receipt or completed sample is promoted.

Root reported separate 1,000-row JSON_TEXT and bare-record SHA256 measurements at source `a65b97f59c51e972c6a4379127b46bfc6fdbeae9`: 124 ms/1,901,402 bytes versus 134 ms/32,000 bytes. These are parent-reported observations pending their own receipt backing, not evidence authored or revalidated here. They are different nonrepresentative snapshots/cache states, include metadata/boundaries, show no speedup and do not measure this context-fenced expression. They establish no cause, equivalence, whole-table readiness or QT result.
