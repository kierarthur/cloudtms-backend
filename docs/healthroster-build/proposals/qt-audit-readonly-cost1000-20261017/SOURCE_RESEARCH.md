The programme is a cost observation, not a new audit equality assertion.

[PostgreSQL REL_17_11 record_send](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/adt/rowtypes.c), lines645–758, obtains the actual tuple descriptor and processes every non-dropped attribute, including null markers, type OIDs and each type's send function. Attribute names are not represented in that binary payload. The input therefore freshly binds the exact sixteen-column descriptor and six concrete type I/O identities before considering even a diagnostic invocation. This does not establish equivalence with JSON text or MD5.

[PostgreSQL REL_17_11 jsonb_send](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/adt/jsonb.c), lines116–130, constructs JSON text plus a version byte. RECORD_BINARY/SHA256 consequently do not avoid all JSON formatting work for before_json/after_json fields. Any speed or cause remains unproved.

[PostgreSQL17 CTE materialization](https://www.postgresql.org/docs/17/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION) explains explicit MATERIALIZED calculation. Here the stored expression is only its integer byte count, and the ungrouped count/sum consumer exhausts the bounded CTE. SQL AST checks prove a literal1,000-row inner sample, one materialized expression column and no filter/order/offset/DDL/DO. Parsing proves source structure, not a native plan or performance.

The existing R6 whole-table CTAS timeout is not diagnosed by this packet. Scans, detoasting, serialization, hashing, allocator costs, cache state and transaction/collector overhead remain possible contributors. These five small sequential observations do not isolate all of them or justify scaling sampled duration to the entire audit heap.
