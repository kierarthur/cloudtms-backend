# SP-08 independent SQL source review

10 October 2026. Read-only source review; no SQL, tests, gates, fixtures, runtime or hosted calls were invoked. Full card and all six checklist rows were read. The source census and confirmed SRQ304 mismatch are recorded in `../design/SP-08-flat-origin-census.md`.

## Design verdict

**PASS** for the revised bounded append-only seam design communicated by root: keep frozen 0556 untouched; retain all original 0302 validation and COMPLETE read behavior; in the fresh validated current-xid branch insert one consumed=true marker keyed by the actual HEAD UUID, with ON CONFLICT DO NOTHING and a NOT FOUND refusal. A private insert/immutable guard requires current xid/true and denies UPDATE/DELETE/TRUNCATE. Actual HEAD plus exact existing qualified context is the seam identity, without a new caller token or protocol. Statement rollback and immutable retained rows prevent successful consumption from being rearmed by constraint-mode changes.

## Schema read

`10102026_0736_banking_pay_next_local_detail_seam.sql` is additive and has no historical backfill or money fields. Its head_id UUID foreign key references the real public.weekly_source_entitlement_heads(id) UUID primary key, confirmed in the sealed source contract. transaction_id xid8 and consumed boolean are NOT NULL. Owner mapping, ENABLE/FORCE RLS, owner-only policy and revocation from PUBLIC/anon/authenticated/service_role follow the existing private authority boundary. The retained default false is safe only with the repeatable's required insert guard rejecting every value except explicit true; no legitimate new row may be left unconsumed.

## Final saved source verdict: PASS

Fresh migration, complete repeatable, registry and revised design/closing note were reread. No open actionable finding remains in this bounded source change. The earlier publisher-mint/false→true draft is superseded; no frozen-source exception was introduced.

The repeatable contains exactly two definitions. Its guard at 4–17 accepts INSERT only when transaction_id equals pg_current_xact_id() and consumed IS TRUE, using null-safe IS DISTINCT FROM. UPDATE, DELETE and TRUNCATE all raise before mutation. Two private BEFORE attachments at 21–27 cover row mutation and statement TRUNCATE, with DROP IF EXISTS/CREATE repeatable installation. Its invoker security and pg_catalog search path introduce no authority beyond the existing owner-only table.

Fresh capture remains in the existing NEXT, real receipt/HEAD/bundle/vector and current-context authority. Lines 230–238 retain the COMPLETE branch, which never executes marker insertion or backfills historical detail. The eight-line addition at 275–282 inserts the real head/current-xid/true marker and refuses ON CONFLICT through NOT FOUND. It executes before the WORK loop, so a genuine zero-work vector also consumes. Original exact prior-authorisation/HEAD/TSFIN checks at 283–332 and all later detail checks remain unchanged. A subsequent failure propagates and rolls back the new marker with the unsuccessful statement; successful retained markers have no mutation/reset path. SET CONSTRAINTS cannot disable these immediate BEFORE guards or create a second primary-key marker.

Literal source comparison of the full 471-line former capture definition with the 479-line successor proves byte equality after removing exactly one eight-line insertion and normalizing only CRLF/LF. Parameters, security/search path, original checks, composition, bounded stored detail, replay and return are unchanged. Owner remains postgres and all four ordinary API-role EXECUTE grants remain revoked. NOTIFY is issued only after definitions and ACL in the same transaction.

Registry comparison by complete entry against reviewed base cc622c8a proves exactly one changed capture chain and one new guard chain; no chain was removed or otherwise changed. The frozen publisher's complete registry entry is identical to base and still selects 0556. The saved 0556 and generic I1 source hashes are unchanged. No new public function, public trigger, command, caller token, producer protocol, origin variant, money or history backfill was added.

The final `SP-08-single-use-seam.md` and `SP-08-closing.md` agree with consumer-only implementation and keep native/hosted acceptance NOT_RUN. The closing note attributes a separate actual local cutover-ledger receipt to root; this reviewer did not query that database. Source PASS is not an app, second-use business invocation or protected-verification PASS.

## Final source identities

| File | SHA-256 |
| --- | --- |
| 0736 additive migration | `a878daf1db905ccaa71f8d29eb02703b17d3f35703ee34dbfef874e5da3b3509` |
| 0736 two-function repeatable | `b82b11839ac0ba55d24c90cc5cfb06fb901ee04ab7a272d520ce0a02057debcf` |
| Final-definer registry | `8ef9c2a0f97cdb1304cad49941465df607f2d628bb2173c8f233a6a087409ee5` |
| Frozen 0556 publisher | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| Unchanged 2120 generic I1 | `2fee6c5413c1378610790e909642f5d6ea4966274f0d1436af96ec9f1d5f63bc` |
| Revised seam design | `f339cb9f17503734d450f6fd2ca9504c68c93d5e45d32ff1a613a5478b5a3fce` |
