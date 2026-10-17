# QT audit sequence-order candidate — source only

The current two QT assertions each hash every complete `public.audit_events`
row. The old aggregate orders by the complete row's MD5 text. This candidate
orders the same row hashes by the genuine unique, non-null bigint
`a.event_sequence`, allowing the existing native B-tree order to be considered
instead of sorting the wide row expression. Planner choice and runtime remain
unproved. There is no new materialization, resource setting, timeout, row filter,
column projection, DISTINCT, map exclusion, alternate financial oracle or JS
aggregation.

Root's fresh fixed read-only native index receipt is copied unchanged in
`NATIVE_INDEX_EVIDENCE.json`. It proves the exact single-key unique, immediate,
live, valid, ready, nonpartial, nonexpression `int8_ops` B-tree index.
`NATIVE_COLUMN_EVIDENCE.json` separately extracts the already captured NOT NULL
bigint GENERATED ALWAYS column records, with the full original capture hash.
The original source migration is retained whole. Identity alone is not treated
as a uniqueness proof.

Before either digest or fixture insert, the candidate checks the exact existing
index and column in `pg_catalog`. Missing/changed identity, key, uniqueness,
immediacy, validity, readiness, liveness, ordering, partial/expression shape or
DDL refuses `QT_AUDIT_EVENT_SEQUENCE_INDEX_UNAVAILABLE`. This is intrinsic
admission for this deterministic order, with distinct SQL-range aliases. No
Banking semantic body or additional diagnostic connection is involved.

Both freshly computed local digest encodings change together. Their numeric
sequence order remains deterministic for every permitted row set and is
independent of physical scan order. Every full-row `to_jsonb(a)::text`, inner
MD5, row multiplicity, `|` delimiter, empty-string coalesce and outer MD5 remains.
The equality assertion compares these two new local digests; no previous or
stored digest is reused. This is not a claim that old and new digest bytes are
equal, or that either finite MD5 fingerprint is collision-free. The existing
full canonical restoration and original WeakMap qualifier are unchanged.

`FORWARD.json` binds exact root parent
`769a54fc1053b57c8242d2960002c900dd716791`, all four complete old/new files, 48
unchanged adapter manifest entries, the two query replacements and full guard
insertion. The four targets are QT assertions, sealed programme, sealed offline
input and adapter manifest. Only the QT programme/input fields and one response
statement SHA change; the complete constructor case, verification SQL, fixture
IDs, all assertions, expected results, original scope sets, source admissions,
shared-turn ownership, 30/45/70/120/180 caps and original failed history remain.

Non-writing review commands from this directory:

```powershell
node --test test.mjs
python pg17-proof.py
```

The tests include complete raw inverses, old/new digest-encoding distinction,
empty/permuted bags, full int63 ordering, row deletion/insertion/change, repeated
non-key content, NULL/duplicate-key domain refusal, UTF8/delimiter content and
six source mutations. Model inputs are explicit stand-ins for already-created
PG whole-row text, not an imitation of native JSONB serialization. PG17 parsing
independently proves the complete PL AST inverse (excluding source positions),
only the aggregate-order AST change, exact full-row argument lineage, no row
selection or aggregate change, the fixed metadata-only guard, no range/PL
variable collisions, eight additional valid SQL counterexamples, and complete
programme/input grammar.

Author activity was source-only: no SQL, EXPLAIN, native tool, holder, auth,
business rows, root edit or runtime execution. Root must review/adopt the exact
four files and commit before its actual source-admission/native workflow. An
index scan or faster assertion is **NOT_PROVED** until genuine root observation.
