# PF-04 D04/D05 independent source review

Reviewed 10 October 2026 in BUILD-BE. Verdict: **PASS for the finite D05 source gate and D04 future joined acceptance document**, after the HG01 correction below. This is a saved-source review only. The reviewer did not run tests, source gates, SQL, application code, database operations or joined verification, and did not edit product source.

| Reviewed file | SHA-256 |
| --- | --- |
| `scripts/bpay-next-source-gates.mjs` | `4521c19791a30845cfb65a35e743117c8f717994887145f26e3dce2ff87c0f61` |
| `tests/bpay-next-source-gates.test.js` | `9b099fafca077c2ee8f5d59079fc2251d1e189f5a04f63f6012779ff1c4b8977` |
| `docs/bpay-next/PF-04-JOINED-ACCEPTANCE.md` | `8ec385562b1ba116d4f3cac143f7658235a0240420825a817246e09f75202dd4` |

## D05 source gate

The gate selects all 33 listed operational roots and reviewed helpers from the current final-definition inventory, rather than only changed files or historical definitions. It requires exactly one controlling routine body. The list and fixed caller-to-callee map agree with the saved PF-04 routine-call census, including the actual schema-qualified `pg_catalog` calls. Existing Source inventory, approval-basis and pure finance-math boundaries remain explicit reviewed callees; this finite guard does not claim to infer an arbitrary database call graph.

The five finite ledger names are checked after removal of exactly four reviewed point clauses: the original funding-event tuple; the Source financial-effect ID; the reconciliation financial-effect ID; and the full recovery-event/command/component/transfer/amount witness. Removing only one occurrence preserves rejection of a duplicate or altered clause. Candidate/work/date filters, aggregates and a row limit do not create new point exceptions. Direct audit readers, unknown helper delegation and dynamic execution are independently refused.

**HG01 — CLOSED.** The initial `FROM`/`JOIN` recognizer missed valid `FROM ONLY private.bpay_next_case_event` and a ledger appearing after a comma in a table list. The corrected source examines every remaining ledger-name token. It permits only the actual `%ROWTYPE` use, an `INSERT INTO` target, and a `::` cast; these are declarations/targets rather than history reads. A separate ledger occurrence in an INSERT's SELECT remains refused. The correction also covers `TABLE` spelling. The existing source-unit file now includes finite ONLY, comma-list and TABLE negative cases, preserving its previous cases. No new parser, harness, SQL protocol or monetary owner was introduced.

Quoted ordinary identifiers are normalized by the existing executable-source extraction, genuine comments are removed, and literal text does not become a helper or relation token. The current allowed row-type declarations remain recognized. No further concrete bypass or current-source false match was identified in this reviewed slice.

## D04 joined acceptance

The acceptance note agrees with PF-04's card/checklist: unrelated paid history must leave current admitted preparation inputs and money unchanged; temporary cleanup must retain financial origins and all seven job-reference edges; monitoring exposes the five qualified measures; and the NEXT minute seam has one bounded cleanup visit with no emitted message, drain or new cron.

Every application, queue/minute and joined-history clause is explicitly `NOT_RUN`. The document does not turn a source check or empty schema compile into an application proof, authorize data creation, require an old Banking tick, add a timing laboratory, or claim an Office screen is complete. It retains the separately governed phase-3 real-Worker timing check. Its service-only health/role and nullable historical-clock statements match the saved source. These are future acceptance conditions, not a claim that their joined controls have already passed.

No open finding remains for this D04/D05 source-review scope. PF-04's other product clauses and later joined TEST evidence retain their own completion authorities.
