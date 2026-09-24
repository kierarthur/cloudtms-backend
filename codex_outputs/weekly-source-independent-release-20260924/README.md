# Independent Weekly Source TEST component release

This branch starts from backend feature commit
`a7e802cdef009d911ff44509dda293f647f9d86c`, whose parent is the current
`origin/test` commit `e339efc032c1b357a27dfc96051b053f02d6dafa`.

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

No database change, deployment, commit or push has occurred merely by adding
this release definition.
