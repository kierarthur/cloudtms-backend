# Independent Weekly Source TEST component release

This database-first branch starts from the current `origin/test` commit
`e339efc032c1b357a27dfc96051b053f02d6dafa`. It contains the two exact
database-definition changes from saved feature commit
`a7e802cdef009d911ff44509dda293f647f9d86c`, but deliberately excludes that
commit's backend runtime changes.

The component database release exists so unfinished Banking Pay Stage 2 work cannot
block an unrelated Weekly Source release. It does not remove, change or run the
three open Banking Pay verifiers. The full database release remains blocked
until those integrations are complete.

Release order is mandatory: merge and APPLY this database-only component first;
only after its receipt passes may the saved backend/Office feature commit be
rebased or reconstructed on the resulting TEST head and deployed. This avoids
the Git-connected backend reaching TEST before its database definitions.

The component gate is restricted to TEST and to the two exact repeatable files
and hashes recorded in
`supabase/release/weekly-source-invoice-evidence-component.json`. A later full
release will consume their normal repeatable-ledger entries and will still run
the complete verifier set.

Creating this release definition did not change TEST. The only hosted database
exercise so far was explicitly rollback-contained.

Local verification on 24 September 2026 passed source integrity, dependency
provenance, credential scanning, all 1,426 repository tests, and the five
component release-system tests. A protected TEST rollback rehearsal compiled
both replacement definitions, passed the scoped catalogue and ACL checks,
reported `rolledBack=true`, and proved the installed definitions were
unchanged afterward.
