# SP-08 interface source review

10 October 2026. **Final revised two-function source PASS; no actionable finding.** This verdict binds the fresh `b82b1183…` 0736 repeatable and supersedes the earlier three-function `7deb4c7b…` snapshot. Frozen 0556 and its registry entry remain unchanged under F-23. Independent read-only review covers the full card at `05_SOURCE_AND_PROTECTED_PAY.md:305–331`, all seven current checklist rows (D01 context; D02–D06 deliverables), BD042 / scope, final seam design, registry, binding census, caller map and required closing record. No product edits, SQL, tests, fixtures, runtime, hosted or secret operations. This is not first-use or application acceptance.

## Confirmed SRQ-304 correction

The new private table has the real immutable HEAD UUID as primary key/FK, `xid8` transaction identity and non-null Boolean consumption. It is owner-only with ENABLE/FORCE RLS, an owner policy and revoked PUBLIC/anon/authenticated/service_role rights. It adds no browser API, public trigger, command, financial amount, producer protocol, origin arm or Rule B change.

The new trigger function is SECURITY INVOKER with a fixed catalog search path. Its actual row attachment handles INSERT/UPDATE/DELETE; the separate statement attachment handles TRUNCATE. INSERT requires the current xid and explicit true consumption. The locked migration's DEFAULT false is therefore refused. Every UPDATE, DELETE and TRUNCATE raises fixed 23514, including a same-value update, reset or reassignment. SQL NULL does not bypass INSERT predicates because they use `IS DISTINCT FROM`. The HEAD FK and NOT NULL/PK constraints cover the remaining identity shape.

Frozen 0556 still passes the existing private qualified context and current transaction ID only after real HEAD components exist and the unchanged inventory assertion has been made immediate. It does not define or mint a marker. There is no new publisher protocol, GUC or missing-HEAD fallback.

The detail routine first preserves the original exact receipt/origin/digest/scope/bundle/HEAD/approval/generation and complete-vector checks. The fresh branch additionally proves STAGED/PROPOSED and the original current-xid qualified context, then atomically INSERTs the actual HEAD/current-xid consumed=true row `ON CONFLICT(head_id) DO NOTHING` (0736:277–282). An existing marker produces NOT FOUND and `BPAY_NEXT_LOCAL_DETAIL_SEAM_ALREADY_CONSUMED_OR_MISSING`, without updating/rearming anything. Consumption precedes the worked-component loop, so genuine empty/zero-work vectors consume too. The INSERT's own FOUND result is checked before any later statement can replace it.

The unchanged fresh before-authority revalidation and detail association checks still follow consumption. Any later raised failure rolls back marker/detail/HEAD work in that transaction. A successful marker cannot be reset by the guard, a GUC or a change of constraint mode. Early `SET CONSTRAINTS ALL` cannot create another marker or reset the persistent consumed bit. Source inspection therefore closes the previously missing single-use mechanism; no business negative invocation was performed.

The retained COMPLETE branch remains a qualified read of actual original immutable details and never touches the marker. It does not backfill missing COMPLETE detail or reopen the fresh publication seam. This preserves legitimate immutable replay while refusing a second fresh use of the qualification seam.

## Bounded predecessor and final authority

Read-only whole-definition text comparison, normalising only line endings, confirmed:

- The 510-line replacement does not redefine the frozen publisher; saved 0556 retains its reviewed hash `64ef7e6d…`, and its complete current registry entry equals the HEAD registry entry.
- New detail definition equals the complete selected 0302 predecessor after removing exactly the added eight-line consumed-marker INSERT/refusal/comment block.

No other business, money, origin, generic I1, callback, first-authorise, receipt or financial ordering clause changes. The other function is the narrowly supporting private marker guard. Existing copied declarations/signatures, SECURITY DEFINER/search paths and private ACLs remain intact. The single transaction contains these two routines/attachments and fixed PostgREST schema notification. The current final-definer registry selects 0736 as final root/physical owner only for seven-argument detail and guard, retaining the real 0302 predecessor; the publisher entry stays unchanged. No caller/public interface change is required. The corrected scope/design/closing record expressly removes the uncommitted broader draft and adds no F-23 exception.

## Full named-row closure

| Row | Current source result |
| --- | --- |
| D01 context | Retained 2018 first-authorise tail stages/publishes only after real successful outer core under NEXT, then invokes the actual 0018 initial binder. It never invents HEAD/Final or moves binding into the reusable core; physical next edit belongs to SP-03. |
| D02 | Historical staged 1245 is the existing managed 2010 book migration, not a missing live migration. Its actual typed identity, private RLS/ACL and book-shape support are retained. |
| D03 | Current 1246 binding/book/snapshot owners are unchanged. REGISTERED, POSITION_PENDING/unbound and READY remain distinct; absent evidence is not zero. |
| D04 | Current 1301 preserves sorted real receipt members, original Candidate/component identities, binding lockset, staged-pair publication and exact REGISTERED component qualification. No fake receipt or extra Worker enqueue. |
| D05 | Existing closed 0201/0212/0224/1257 origin chain, exact twelve-key committed before-witness, original full HEAD digest and new-HEAD-only 2137 capture/2120 immutable guard remain unchanged. The 0736 marker supplies only the confirmed SRQ-304 missing mechanism. |
| D06 | Fresh `SP-08-closing.md` records real 2018/0018 bind order, current book/receipt chain, Source T-26 on the unchanged 0114 retained reader, C29 flat-origin status and C30 PRIM erratum. It separates local ledger/source/compile evidence from NOT_RUN business journeys. |

The closing record correctly withdraws “effect NULL only for ZERO”: positive realised source requires its actual effect, positive held source its actual hold, and a genuine held-only ORIGINAL_POSITION can have no effect. ZERO describes a proven maintained book basis, not absence of all historical children. No artificial effect/backfill is proposed.

The saved local cutover receipt names `banking_modal_v2_test`, the exact 0355 migration path and `ledger_count: 1`. This reviewer read that receipt, not the database. The once-only migration is not turned into a standing refusal for future Local publications. The 0114 closing is the actual Local ReceiptV3 branch, not a claim that distinct Direct/C1/query activation owners were rebuilt.

## Fresh source receipt

| File | SHA-256 |
| --- | --- |
| 0736 migration | `a878daf1db905ccaa71f8d29eb02703b17d3f35703ee34dbfef874e5da3b3509` |
| 0736 repeatable | `b82b11839ac0ba55d24c90cc5cfb06fb901ee04ab7a272d520ce0a02057debcf` |
| Single-use seam design | `f339cb9f17503734d450f6fd2ca9504c68c93d5e45d32ff1a613a5478b5a3fce` |
| Final-definer registry | `8ef9c2a0f97cdb1304cad49941465df607f2d628bb2173c8f233a6a087409ee5` |
| Required closing record | `f71d299f35917dafc32683c1cd4a7deaf8ca8f70b506551f7942e267da969a81` |
| Unchanged frozen 0556 | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| Saved local 0355 ledger receipt | `2c28065c9e4041b0d2fac552e099a92e6ffd42aec4e8bf8d2cc95a3669c1752e` |

The actual no-Final first-work/book journey, amendment payload-size/editor journey, reuse/early-constraint negative business actions, and hosted/in-app acceptance remain NOT_RUN. Local installation/compile/seal results are root-owned evidence, not independently executed by this reviewer.
