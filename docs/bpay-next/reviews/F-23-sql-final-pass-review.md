# F-23 D09/D10 final source review

Verdict: **D09/D10 source PASS for the reviewed finite gate scope.** Fresh read-only review on 10 October 2026. No outstanding finding from this review. No gate, tests, SQL, runtime, database or fixture was executed by the reviewer. No product source was edited.

Reviewed saved gate SHA-256 `7cbdba5eccca9c83191f872f4fc60f9199e42287d8fbbb2524772c34fa10c55c` and source-unit SHA-256 `ff5a6ea6363b09f133b5f79fa30faaaa72c0101fdd7fdf21bb36bb187c6560bc`. D09/D10 logic was reread directly; the dynamic classifications were compared with the actual `06102026_0236_banking_pay_next_channel_draft_readers_v1.sql` query construction and `06102026_1206_banking_pay_payment_history_page_v1.sql` history construction. This review concerns the gate, not new business SQL or a general PL/pgSQL parser.

The three preceding findings are closed in source: non-ROWTYPE inherited keys retain the full condition/statement; unsupported quoted identifiers produce an explicit refusal marker; primitive local types come only from the actual `DECLARE` through first `BEGIN` region. The normal source units include the three corresponding refusal cases. The existing nearest-SELECT/INTO and actual FROM/JOIN alias checks accept the required `alias.*` owner shape. Unresolved aggregate types and UUID cardinality remain bounded refusals.

## Closed dynamic findings

Every dollar initializer is now counted. A repeated initializer or any non-SELECT initializer sets the variable's classification to null; later initializers cannot restore it. The surviving exactly-one SELECT initializer and its finite literal suffixes receive recursive special-construct inspection; other assignment mechanisms are refused. The earlier non-SELECT/WITH then safe SELECT finding is closed in the saved implementation. The normal units explicitly refuse the earlier-unsafe SELECT/later-safe SELECT shape.

The history producer extraction includes all eight SQL-fragment variables and `V_TEXT`, both `:=` and alternative `=` forms, and `INTO [STRICT]` for those variables. It compares their complete ordered statement lists against immutable reviewed source rather than relying on the presence of the order helper. Additional or changed producer statements lose the dynamic exemption. The reported plain fragment, order reassignment, SELECT INTO and alternative assignment findings are closed. The units explicitly cover a changed plain `v_where` SQL literal and an additional `v_text` producer.

The history reference is fixed `FOUNDATION_RECONCILED=3c91e5e6ce35b5833a5c218b77b741261eeebafa`, not the checked disk source. Direct read-only Git probes returned zero for history-object existence, ancestor verification, and the diff of both history and retained 2018 against that commit. The missing-reference and self-baseline findings are closed. Closed registry metadata still requires simple identifiers, finite key types and bounded generated SQL shapes; it is not a caller SQL escape.

The schema loop allowance requires complete ARRAY lists containing only simple literal identifiers and equal counts of all matching `FOREACH ... IN ARRAY` uses and classified lists. Variable entries, concatenated identifiers and additional unclassified array shapes cannot borrow the exemption. Its existing refusal units cover variable and concatenated entries.

## Retained F05 row projections

The four-signature 2018 exception is restricted to its exact committed file bytes, after each latest successor independently passes ordinary D09 with the unchanged F01 baseline. Only ROWTYPE hazards belonging to those four signatures receive the separate retained-file allowance. Current registry entries identify `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql` as their final physical/root. No shared F01 allowance was broadened, and new successor hazards do not borrow this retained-file exception. The inspected gate design for that historical treatment is bounded correctly.

## D10 and scope

The controlling RPC selection covers changed public repeatable definitions and changed migration definitions, requires a final repeatable for the latter, and checks the expanded final closure. Exact `NOTIFY pgrst,'reload schema';` must follow the last public definition and grant/revoke, with only transaction commit statements afterward. The supplied units cover missing notification, an early notification, the valid final notification, and substantive work after it. No new D10 finding.

Historical same-signature/statement/count allowances, frozen product SQL and the owner rule-B scope remain unchanged. Unsupported bodies, unresolved dynamic forms, aggregate types and UUID cardinality are bounded refusals. This source PASS closes the finite D09/D10 review findings; it does not certify execution results, general SQL parsing or completion of other F-23 checklist rows.
