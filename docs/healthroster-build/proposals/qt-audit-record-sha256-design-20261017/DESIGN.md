# Bounded representation and assertion argument

## Exact admitted descriptor and SQL-call closure

The existing committed public S0 metadata is captured raw at `reference/S0_METADATA.json`, SHA256 `c1e91c6507d3624dd69e92707f6f7fd0e8614479b3f5a75c3b4fd9268f152f8c`. It supplies precisely these attributes, in this order:

| Ordinals | Attributes | Native type |
| --- | --- | --- |
| 1, 3 | id, actor_user_id | uuid |
| 2 | ts_utc | timestamptz |
| 4–8 | actor_display, actor_role_at_time, object_type, object_id_text, action | text |
| 9–10 | before_json, after_json | jsonb |
| 11–14 | reason, ip, user_agent, correlation_id | text |
| 15 | event_sequence_is_authoritative | boolean |
| 16 | event_sequence | bigint, identity ALWAYS |

The complete fixed predicate is `encoder-catalogue.sql`; `ENCODER_CONTRACT.json` carries the typed mapping. It refuses additional live attributes, missing/renamed/reordered attributes, type/identity drift, an undefined/domain replacement, an unexpected relation/policy/owner shape, and a different named-composite send hook. The six leaf mappings are UUID `uuid_send/uuid_out`, timestamptz `timestamptz_send/timestamptz_out`, text `textsend/textout`, JSONB `jsonb_send/jsonb_out`, boolean `boolsend/boolout`, and bigint `int8send/int8out`. The outer anonymous record directly invokes the qualified builtin; its first field has the admitted named audit composite type and its second is text. There is no additional user-defined field type, selector or financial routine in this encoder path.

The two metadata evaluations are exact source, not native observations. A final changed native trial must actually satisfy them. Source symbols/catalogue attributes are not a binary attestation of a custom PostgreSQL build; the bounded argument assumes the observed supported PostgreSQL 17.11 implementation matches the pinned upstream code. It is not a transitive audit of arbitrary extensions or the full existing QT controls.

## Framing and six leaves

The primary [REL_17_11 record implementation](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/adt/rowtypes.c) serializes the live descriptor with field count, type OID, signed length (including a distinct NULL sentinel) and the selected send bytes. Its nested call selects the field's `pg_type.typsend` through [getTypeBinaryOutputInfo](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/cache/lsyscache.c). Attribute names are not embedded; the repeated descriptor qualification is therefore essential. `PG17_SOURCE_PROOF.json` binds exact complete function spans and required operations, rather than filenames alone.

UUID sends all 16 bytes. Bigint and timestamptz send signed 64-bit values, retaining integer precision, timestamp microseconds and infinity encodings. Boolean sends its canonical byte. Text sends length-delimited bytes. Equal UTF8 client/server encoding takes the no-conversion path in [mbutils.c](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/mb/mbutils.c), so no conversion hook can fold different strings in this admitted case. All six send/output bodies and their upstream catalogue declarations are pinned.

[JSONB send](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/adt/jsonb.c) uses its version byte and the same JSONB formatter as JSONB output; it still serializes JSON text. Numeric formatting inside that formatter is a direct C `numeric_out` call, whose source is also captured. The proposal does not claim to eliminate JSONB serialization, infer canonical JSONB bytes using JavaScript, or treat raw JSON inputs with different key/duplicate forms as distinct stored values. SQL NULL framing remains distinct from stored JSONB null; the old composite JSON could collapse those cases, so this can cause an additional conservative refusal.

## Context and comparison implication

The old `to_jsonb(a)::text` formats timestamptz using session TimeZone. [JsonEncodeDateTime](https://raw.githubusercontent.com/postgres/postgres/REL_17_11/src/backend/utils/adt/json.c) fixes ISO output style but resolves timestamptz through the active zone. Its binary send alone lacks that context. Consequently, bare `record_send(a)` is insufficient for a claim that every old JSON-visible context change remains detected. The proposal includes the actual `current_setting('TimeZone')` text inside the hashed record, without converting timestamps or accepting a guessed UTC context. A zone-alias change that formerly produced identical output can now refuse conservatively. Fixed-source session/builtin assumptions remain necessary; this is not proof against arbitrary external timezone-file changes.

With the admitted descriptor and valid stored builtin values, equality of the complete length/type/NULL-framed preimages yields equality of every represented leaf and the zone string. Equal leaves and context reproduce the old composite JSON output. The reference model exercises all fields, SQL NULL versus JSONB null, delimiters, Unicode, nullable values, signed bigint extrema, values above 2^53, timestamp microseconds/infinities, nested JSONB text and zone-only changes. Its JSONB leaves are explicitly supplied formatter-output fixtures, not a substitute native oracle.

The stored comparisons use SHA256, not those complete preimages. **Equal digests do not logically prove equal preimages.** The conditional assertion argument assumes no relevant SHA256 collision, just as the old per-row MD5 detector depended on its collision assumption. No unconditional hash-to-hash equivalence or collision-free row equality is claimed. A deliberate constant-hash counterexample in the tests makes this limit explicit; the old MD5 and new SHA256 checks are probabilistic detectors with different collision risks, not mathematically identical functions.

Given that assumption, the unchanged keyed multiset comparison implies the original row-count/key/full-row equality check: both directions of `EXCEPT ALL` preserve occurrence counts, while the native unique non-NULL event_sequence admission independently rejects key ambiguity. No text ordering/collation is used by the bytea digest comparison, no delimiter encodes multiple rows, and input scan order is immaterial. Both snapshots retain the cap+1 safeguard and full-table interpretation below the cap. Concurrent unrelated committed audit changes still cause a conservative HOLD; no new job attribution or audit exemption is added.

## Preservation and unproved cost

The full `qt_controls` body is raw unchanged: six genuine rollback users, current role/ACL checks, settings/defaults, replay/CAS/refusal/Golden invariants, journal/audit assertions and exact fixture after-state remain. Existing outer native/read-only/rollback/fresh verification/TEST/LEGACY/current actor/A lock/marker/absence, twelve complete restoration scopes, background writers and private WeakMap brand remain in unchanged owners. Limits remain statement30s/lock2s for B, C45s, local70s, operation120s, close60s and whole180s wherever originally enforced. Work memory remains4MB; no planner setting except the existing local HashAgg-off interval changes.

The new digest is 16 bytes wider per row, so the two capped tables can carry up to 48,000,000 additional digest payload bytes, before tuple/index/executor overhead (and cap+1's transient extra row). That is a source arithmetic bound, not a measured allocation or a hard temp-file bound. Nested record allocation, JSONB formatting, SHA256 CPU, scans and sort/spill behavior may outweigh removed composite-JSON/scalar formatting. No speedup, parallel plan, no-spill result or whole-DO duration is asserted. The existing timeouts remain the deciding safety bound. This source proposal remains inert pending independent peer and root's actual changed-source admission and bounded trial.
