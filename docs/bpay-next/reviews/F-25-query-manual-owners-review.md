# F-25 manual Query owners — independent source review

**PASS for the bounded manual-owner successor, with the two reported findings closed in the final saved bytes.** This is not a whole-F-25 completion claim. Read-only review saved 10 October 2026. No product edits, SQL/database/runtime/test/browser/fixture execution, hosted reads, or deployment were performed in this review lane.

Reviewed product: `supabase/repeatable/10102026_0009_banking_pay_next_query_manual_owners_v1.sql`, **35,114 bytes**, SHA-256 **`871353cca6b4a9c24eb93a0c122dd0efbadaced2b20e6b681c354ab80a7674f7`**. The original full bodies are `05102026_0428_weekly_source_manual_review_commands_v2.sql`, not the older1601 predecessors. Source was re-read after root's fixes; full per-function diff inspected independently.

## Findings resolved

1. **Concurrent reply preemption, closed.** The first draft performed fresh scope/source/OPEN reads before producer entry and the advisory+locked reply recheck. An exact concurrent RESOLVE could commit its receipt and RESOLVED review between the initial receipt miss and that extra OPEN check, causing NOT_OPEN instead of the original exact reply. Final0009:40–45 and167–172 now perform only pure active actor/admin/payment permission using the original Office helper with null group/client/date, then enter. All original full source/group/client/OPEN business checks remain exactly after the advisory and immutable receipt recheck. Source stale and resolved review no longer preempt that race reply. The new early check is **actor permission**, not a replacement for original scoped authority. Original scoped authority remains mandatory on both receipt replay and fresh business paths.
2. **PK-only identity bypass, closed.** The first hook returned on tuple equality before its identity check; the tuple intentionally omits PK id. Final0009:433–435 now rejects identity/scope change before the equality return:436. Prose/timestamp/actor-only genuine no-op updates still return before producer/module/owner assertion:437–444.

No unresolved blocking finding was found in this bounded file after those corrections. The result relies on the inspected current shared lifetime/metadata helper interfaces; it does not assert untested runtime concurrency or compilation.

## Original contracts retained

| Contract | Independent source result |
|---|---|
| Original declaration/config | Both public signatures return JSONB, volatile/security-definer with original `public,private,pg_catalog,pg_temp` search path. Private Final helper returns integer with original `pg_catalog,pg_temp` path. No signature/config change. |
| Original service/auth/shape/hash | Service guard, exact request-key allowlists, UUID conversion failures, reason length, original OPEN/RESOLVE semantic hash domains, actor/payment choice remain exact. Initial immutable receipt probe/collision validation/full original scoped Office authority/exact result+idempotent flag remain before added entry at0009:18–39 and140–166. |
| Pure-before-entry permission | New original-helper call checks active actor/admin and payment authoriser where PROTECTED_PAY. It has no row/advisory/FK lock or business mutation; classifier at `15092026_1534_00_weekly_source_private_classifiers_v1.sql:134` is STABLE and uses ordinary reads. Group/Client/date authority is preserved at the original position after locked replay, not weakened by null early scope. |
| Fresh admission/module-before-business | Entry at0009:44–45/171–172 calls current2357 Source try-admission then module FOR SHARE before original command advisory0009:46/173 and family/review locks. Current shared entry retains its fixed module decision for other owners and admits no unrelated nested edge. |
| Exact locked replay and collision | Original advisory+second immutable receipt lookup/collision fields and original scoped replay auth remain. Only addition is producer finish if outer or leave if nested before returning the same original reply. Collision still throws; exception rolls back any fresh operational producer. |
| Manual OPEN original business | Original source re-read/current authority/route, family advisory/open-review lock and consistency, target-family count>1 refusal, original event-sequence read, review INSERT, audit, original public reply and immutable command INSERT are unchanged. No finance counter, new family/economic choice, snapshot or source baseline added. |
| Manual RESOLVE original business | Original OPEN check after locked receipt probe, Contract weekday snapshot/week/family lock, exact review lock, accepted removal/current Final/projection source branches, protected approval ancestry/scope/Local-C1-NEXT uniqueness receipts and counts are unchanged. Original RESOLVED update, audit, public reply and immutable command INSERT are unchanged. |
| Actual true tail | Fresh OPEN finalizes after immutable manual command receipt0009:105–111, before return116; fresh RESOLVE after original receipt311–313, before return318. There is no original business mutation after finish. Each uses genuine command UUID and SHA-256 of the saved original result_json as completion witness. |
| Nested ownership | `is_outer=false` branches call only current `producer_leave` for this actual nested kind; they do not receive a new standalone command or finish the parent. Current2357 finite edge allows Local completion→MANUAL_RESOLVE and Final/CorrectFinal→MANUAL_RESOLVE. A same xid8 alone does not permit an arbitrary public request to adopt a parent. Current shared leave is BUSINESS_OPEN-only; later postclock Source integration must use its real reserved owner protocol rather than assume this already permits it. |
| Other active module owner | Entry returns null,false under stable non-NEXT decision; leave(null) is no-op. Actual manual business/receipts retain shared Source semantics; the row hook creates no NEXT event without a producer. |

The inspected full diff of each original routine contains only one new producer declaration, replacement of its single admission statement with the described entry/permission protocol, completion at replay/fresh return tails, or the private Final-owner guard. All other original business statements—including protected-query counts, source authority, audits and immutable manual command rows—are unchanged.

## Empty versus actual event completion

The initial pure exact receipt reply does not enter, stage or receive a new command. A race replay **after** entering has zero new manual row events and closes its actual outer producer through the original immutable command witness. Already-open review under a new original OPEN command also writes no review tuple, so finishes without fake event or metadata command.

Current `10102026_0002_banking_pay_next_query_metadata_owner_v1.sql:116–131` verifies the actual command kind/cause UUID/request hash/result digest, counts exact producer-owned events, and for count0 marks producer FINISHED and its private input transaction CLOCK_SEALED, returning **before** `bpay_next_receive_command_v1` at147. That private terminal marker is not an agency-clock increment; no metadata_command_id, job, publication, financial member or fake query version is created. Deferred completion assertion only verifies finished ownership; it does not seal unfinished public business.

For real manual INSERT/state transition, stage preserves each actual OLD/NEW event. Finalization verifies witness and original event ownership/digests, locks own staged metadata before receive, then produces one `QUERY_INPUT_VERSION` zero-member SEALED metadata command and exact receipts at the genuine owner tail. The existing zero-member enroller path is shared0002's responsibility; this review did not run it or claim a full review of all shared enroller successors.

## Manual row hook

0009:418–456 attaches one private AFTER ROW hook to manual reviews. It constructs exactly the saved census/protocol tuple: source group, source cycle, work event, Candidate, Client, Contract, work date, state. Identity check includes actual PK id. No reason/prose, banking data, finance counters, generic `to_jsonb(NEW)`, or today-economic authority is captured.

Identity-changing UPDATE refuses before no-op comparison. Equal tuples return before producer/module/owner assertions, so original operational-only writes do not become financial commands. A material NEXT write with no actual current-transaction producer refuses; actual producer top kind must be MANUAL_OPEN, MANUAL_RESOLVE, SOURCE_FINAL or SOURCE_CORRECT_FINAL. Stage passes the actual row ID and known Candidate, honest **NULL root** (manual table has no root column), true OLD/NEW presence and exactly bounded tuples. The original manual INSERT already references Candidate before staging's Candidate FK; ordinary manual owner has no clock until true-tail finish.

The hook performs no Source admission, module FOR SHARE, discovery of a family/root, business SELECT FOR UPDATE or agency clock. It is a material-writer detector/stager, not proof that unsupported raw callers acquired early ownership. Raw material writes are atomically rejected; original true roots must enter early. The hook cannot retrofit an already-clocked Source callback into a correct protocol.

Public owners remain postgres-owned/service_role-only execute with PUBLIC/anon/authenticated revoked0009:407–413. Private Final helper and new hook revoke service_role as well0009:415–416/452–453. Schema reload NOTIFY is the only new file-tail declaration; no runtime SQL was executed here.

## Final helper and partial Source boundary

Original service check/plain current Final/cycle/actor validation is retained before the new owner guard0009:335–352. With an inherited producer it requires BUSINESS_OPEN, genuine root SOURCE_FINAL or SOURCE_CORRECT_FINAL, and matching OPEN epoch transaction. It does not enter, receive, finish or leave a synthetic manual owner. Original successful CURRENT CAS, contained work-event/Candidate/Client/Contract/date review match, immutable opening provenance, client-manifest and different-upload proof, scoped Office authority, review lock/state transition/audit and integer count are unchanged0009:353–403.

Without an inherited owner the helper preserves original Source admission, holds module decision and **refuses NEXT** as `BPAY_NEXT_QUERY_FINAL_OWNER_NOT_INSTALLED` before this helper's contained-review FOR UPDATE at372 or any manual review mutation. The calling original Final/CorrectFinal may already have reached its own CURRENT CAS/business work; refusal rolls back that transaction. Therefore this is explicitly a fail-closed **partial unsupported Source branch**, not proof of correct early true-root Source lifetime. Those outer hooks and real Source completion certificate remain pending. A future implementation must not replace this refusal with a late owner entry or second agency clock.

## Final-definition evidence

Current `docs/bpay-next/final-definer-closures.json` independently points all three signatures to0009 as finalRoot/finalPhysical. Both public chains retain old1601 and0428 allowed physical predecessors. The private Final helper now explicitly retains its real0428 predecessor—the missing baseline physical root noted by root is present in the saved registry. The three declarations and registry evidence agree. Root reported the final-definer source gate PASS; this review inspected source/registry and did **not** execute that gate or independent SQL/tests.

## Raw provenance

| Saved source path | Raw bytes | SHA-256 |
|---|---:|---|
| `supabase/repeatable/10102026_0009_banking_pay_next_query_manual_owners_v1.sql` | 35114 | `871353cca6b4a9c24eb93a0c122dd0efbadaced2b20e6b681c354ab80a7674f7` |
| `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql` | 38249 | `1e9f16241ffa5e9249a7e8bf796693b81b633c2268cd1864147223451628e8d6` |
| `supabase/repeatable/10102026_0002_banking_pay_next_query_metadata_owner_v1.sql` | 46833 | `14c76f1cf420007608e9640c19b00a0687e63b37d43f406589145598b68848ea` |
| `supabase/repeatable/09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` | 15973 | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |
| `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` | 19936 | `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1` |
| `docs/bpay-next/final-definer-closures.json` | 717483 | `a2aa21de1a3d56b4b89580eaf6ff438405cad5363bf85f2ef7af0f86cbb1f609` |
| `docs/bpay-next/design/F-25-core-query-writer-census.md` | 23646 | `487897c5341efa1e700cf013ec8e968249b85724dc48ddc50ce37d3af726f9e4` |
| `docs/bpay-next/design/F-25-query-transaction-protocol.md` | 57512 | `d9fa8ad09c724082fcd9071587d9262a39f762982e6551a03e5735c74e1d6d20` |

All product references above are current saved source, not proof of installation. Re-read later-changed shared helpers and the bounded owner file before relying on this PASS at a later release.
