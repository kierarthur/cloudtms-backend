# SP-08 independent scope review

## Current final consumer-only review — PASS

10 October 2026, fresh source after root's READY message. **PASS for the exact final 0736 source and SP-08 scope below.** The earlier publisher-replacement review is superseded and retained in the later historical section. That review overlooked F-23's frozen 0556 boundary; the final design correctly preserves it without a source-gate exemption.

The final repeatable contains exactly **two** complete functions: the private marker guard and `private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)`. There is no publisher definition. The entire frozen `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql` is byte-identical to HEAD, and its complete registry chain object is equal to HEAD. Current publisher finalRoot/finalPhysical remains 0556.

Independent complete declaration/body comparison against original 0302 found exactly one eight-line insertion and no other difference. After the original actual receipt/HEAD/component-vector/current-xid qualified-context checks, the fresh branch INSERTs `(p_head_id, pg_current_xact_id(), true)` into the private seam marker with `ON CONFLICT(head_id) DO NOTHING`; NOT FOUND raises `23514/BPAY_NEXT_LOCAL_DETAIL_SEAM_ALREADY_CONSUMED_OR_MISSING`. It performs no update, upsert or rearm. The real HEAD UUID, qualified current xid and immutable PK are the finite seam identity. Existing later origin/currentness/segment/detail checks and all original result/owner/search-path/ACL behavior remain unchanged. Those later raised failures roll back the new insertion within the failed statement/transaction. The insertion executes for the genuine zero-work fresh branch too.

The final guard (0736:4–25) requires explicit current-xid, consumed=true INSERT; every UPDATE/DELETE/TRUNCATE is rejected. The two attachments are solely BEFORE ROW INSERT/UPDATE/DELETE and BEFORE STATEMENT TRUNCATE on the new private marker relation. They are immediate, not deferred. Successful insertion is persistent and immutable; constraint-mode changes/GUC reset cannot rearm it. A second fresh insertion conflicts on the actual HEAD, inserts no row and triggers the finite NOT FOUND refusal. Existing COMPLETE branch bypasses insertion and retains the original immutable sealed-detail read/validation, so genuine retained read replay still succeeds without a second fresh consumption. The original non-NEXT early return is unchanged.

The locked additive migration remains exactly `a878daf1db905ccaa71f8d29eb02703b17d3f35703ee34dbfef874e5da3b3509`: real HEAD PK/FK, xid8 and consumed field, ENABLE/FORCE RLS, owner-only policy and no PUBLIC/anon/authenticated/service_role grants. Its DEFAULT false is intentionally not a valid insertion under the final guard; the sole named owner passes true explicitly. No migration rewrite, fake initial HEAD, historical backfill, financial/choice deletion or direct API exposure is introduced.

Fresh parsed registry against HEAD: **one existing chain update** (0302 detail → final 0736) plus **one new guard chain**, no removals and no other top-level metadata change. All original detail predecessors/allowed physical source are retained; frozen publisher chain is restored exactly. Its inherited description still says “mint/consume”, but the executable final scope/design accurately specifies consumer-only immutable insertion and no separate publisher mint.

Required D01/D02/D03/D04 reuse remains as previously investigated: real 2018 initial authorisation bind, managed 2010 historical-1245 mapping and 2011 support, current 1246/1301 owners. D05 uses the exact preserved 0556 qualified handoff and now the bounded 0302 consumption successor; flat 12-key origin, unchanged stored HEAD digest, immutable new-HEAD capture, I1 ANY-HEAD rule and Rule B remain unchanged. D06 fresh `SP-08-closing.md` correctly reflects consumer-only final scope, T-26/C29/C30 and actual local 0355 ledger count one. The card's required whole Source/book/origin records have no remaining source mismatch identified here.

No SQL/runtime/tests/business/scenario/hosted operations were performed by this reviewer. Root's F-23/local checks are separate execution evidence; their in-progress result is not asserted by this source review. First-use, amendment journey, second-use/early-constraint business invocation and application behavior remain NOT_RUN.

### Final saved source hashes

| Current file | SHA-256 |
| --- | --- |
| `supabase/migrations/10102026_0736_banking_pay_next_local_detail_seam.sql` | `a878daf1db905ccaa71f8d29eb02703b17d3f35703ee34dbfef874e5da3b3509` |
| `supabase/repeatable/10102026_0736_banking_pay_next_local_detail_seam_v1.sql` | `b82b11839ac0ba55d24c90cc5cfb06fb901ee04ab7a272d520ce0a02057debcf` |
| `docs/bpay-next/final-definer-closures.json` | `8ef9c2a0f97cdb1304cad49941465df607f2d628bb2173c8f233a6a087409ee5` |
| `docs/bpay-next/design/SP-08-single-use-seam.md` | `f339cb9f17503734d450f6fd2ca9504c68c93d5e45d32ff1a613a5478b5a3fce` |
| `docs/bpay-next/design/SP-08-SCOPE.md` | `a9653a6bf653a1834caff7057835d8c537e29e135299abc4092c7e54e4661663` |
| `docs/bpay-next/design/SP-08-closing.md` | `f71d299f35917dafc32683c1cd4a7deaf8ca8f70b506551f7942e267da969a81` |
| `supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql` | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| `supabase/repeatable/05102026_0302_banking_pay_next_local_source_chosen_detail_v1.sql` | `4318b30d109a3ebbb1d7afda457a616874fc174f5e7a81061ca0978ac5f89001` |
| `docs/bpay-next/SP-08-LOCAL-CUTOVER-LEDGER.json` | `2c28065c9e4041b0d2fac552e099a92e6ffd42aec4e8bf8d2cc95a3669c1752e` |

Complete extracted final detail-definition SHA-256: `3697b0b5276f5121c71d8af97d6e76a07256ebcee64c59de3b5f4fc1fc54e5a7`. Its predecessor is `c7530d98a8a80c6f439b9cbe3afe03f7df8b0949684b6b89f3061f5b0f7d5758`; all differences are exactly the eight-line consumed=true INSERT/conflict-refusal block.

## Superseded earlier draft review — historical provenance only

> **SUPERSEDED — do not use the PASS below for current 0736 source.** Root reports F-23 correctly rejects replacement of frozen 0556. The previously reviewed repeatable SHA `7deb4c7b3c816fdab1edd70496f6ed15bd3d9acb6590ca498cd0e73a7d108f8a` and its publisher mint/consumer flip design are historical. Root is narrowing the successor to the private guard and 0302 detail owner only: a current-xid consumed=true INSERT with conflict refusal; frozen 0556 remains authoritative and unchanged. Fresh final source review is pending root's ready message. The earlier evidence below is retained as provenance, not current acceptance.

**PASS — source and exact scope.** 10 October 2026. Reviewed the full SP-08 card, six deliverable rows, BD042, `design/SP-08-single-use-seam.md`, the fresh 0736 migration/repeatable, final-definer registry and required `design/SP-08-closing.md`. No actionable source defect remains in the bounded SRQ-304 correction. This reviewer performed source reads, Git-object comparisons and hashing only; no product edits, SQL, tests, runtime, fixtures, business scenario, hosted or secret operation.

## Confirmed correction and preserved owners

The prior binding census's explicit unfinished SRQ-304 clause is now resolved at source by the real 0736 successor. A current transaction ID was insufficient to prove single use; the new HEAD-keyed persistent marker supplies the missing consumed state without a new caller token, command, approval-origin arm or financial protocol.

| Required scope | Review result |
| --- | --- |
| D01 genuine initial binding | Existing registered 2018 first-authorise success tail remains unchanged: real successful outer core → NEXT-only genuine staged pair publication → initial TSFIN bind at 3609. No fake HEAD/Final or change to the reusable core. |
| D02 book schema | Historical 1245 maps to actual managed 2010; 2010/2011 support and all existing book identities remain intact. New 0736 support is one feature-owned private marker relation, not a replacement book schema. |
| D03 book qualification | Current 1246 book/snapshot/seed/guards unchanged. REGISTERED, applied position, seeded book and READY retain their distinct native meanings. |
| D04 Source receipt | Current 1301 actual private receipt attachment and sorted staging/binding/enqueue sequence unchanged. No Worker or Office change or extra financial receipt. |
| D05 flat origin and single use | Existing 12-key codec/context/proposal, actual Local completion, immutable new-HEAD capture and generic I1 semantics preserved. Only exact marker mint/consume seams are added to the two complete current bodies. |
| D06 closing record | Fresh `design/SP-08-closing.md` expressly records current winners/2010 mapping, T-26 reader, C29 flat origin, C30 PRIM erratum, actual local ledger count one and future acceptance NOT_RUN. It does not claim a hosted ledger or executed business journey. |

## New migration and finite guards

`supabase/migrations/10102026_0736_banking_pay_next_local_detail_seam.sql:5–9` creates exactly `private.bpay_next_local_detail_seam(head_id uuid PRIMARY KEY REFERENCES public.weekly_source_entitlement_heads(id) ON DELETE RESTRICT, transaction_id xid8 NOT NULL, consumed boolean NOT NULL DEFAULT false)`. The FK binds a genuine Source HEAD and prevents deleting its retained marker relationship. Lines 10–17 preserve logical owner mapping, ENABLE/FORCE RLS, revoke PUBLIC/anon/authenticated/service_role privileges and install the owner-only policy for the actual release user. No update/backfill of existing HEADs, money tables, choices or Rule B facts is present.

`supabase/repeatable/10102026_0736_banking_pay_next_local_detail_seam_v1.sql:4–24` adds the one private trigger callback. INSERT requires actual current xid and false; UPDATE permits only the same head/xid in the current transaction and false→true. DELETE/TRUNCATE, identity reassignment, reset, repeat transition and cross-transaction consumption raise a finite 23514 refusal. Both attachments (25–30) are on the new private relation only: a BEFORE ROW INSERT/UPDATE/DELETE guard and a BEFORE STATEMENT TRUNCATE guard. No public trigger or competing baseline attachment is introduced. The invoker callback has a fixed pg_catalog search path, logical owner and fully revoked direct execution ACL; existing definer owner contracts are retained separately.

## Exact body preservation and one-use transition

Independent extraction of each complete declaration through its closing dollar delimiter found exactly one addition per body, with no removal or other difference:

- Publisher `private.weekly_source_entitlement_publish_core_v1(jsonb,text,jsonb,uuid,text,uuid,jsonb,jsonb)`: precisely three added lines (comment plus INSERT/VALUES). All original declaration, search path, service auth, pure receipt replay, qualification, Source/business locks, canonical hashing, component allocation, origin capture, invalidation, publication receipt, result/error behavior remain byte-for-byte identical outside this addition. The original immediate full inventory assertion is now at 1650; only actual `v_local_publication_qualified=true` mints the marker at 1654–1655, immediately before the original detail call at 1658–1663. This is after creation of the real complete staged HEAD and before activation/pointers. It is an ordinary INSERT with no conflict reset/rearm.
- Detail `private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)`: precisely eight added lines (comment, UPDATE/WHERE, finite NOT FOUND refusal and separator). The original input/module, actual receipt, generation, bundle, HEAD, scope, approval, complete component vector and private qualified context checks are unchanged. Fresh branch UPDATE at 2328–2330 matches actual head/current xid/unconsumed, then refuses missing/already-consumed at 2331–2333. It executes even when genuine WORK count is zero. Existing subsequent exact authorisation/prior-head/component/detail checks remain unchanged; a failure raises and rolls back the transactional consumption with the failed publication.

The fresh branch and retained COMPLETE branch are distinct: COMPLETE selection/checks at 2279–2290 bypass the new mutation and continue the original sealed immutable-detail replay. Normal publisher receipt replay is earlier than mint and unchanged. Thus a legitimate retained read replay does not manufacture a new marker or second fresh consumption. Atomic UPDATE plus the PK provides one successful false→true transition; nondeferrable BEFORE guards and stored consumed state do not depend on constraint mode or a GUC. Early `SET CONSTRAINTS ALL` cannot reset or delete that state. The original non-NEXT early-return behavior remains unchanged rather than gaining a new owner decision.

Both old physical files remain identical to current HEAD; successor installation order supersedes them. No standing cutover migration was converted into a repeatable check. No recursion, backfill, copied ancestry, nullable-to-zero financial shortcut or generic ANY-HEAD exception is added.

## Registry and retained boundaries

Parsed UTF-8 current registry against HEAD: exactly two existing chain updates (publisher and Local detail) and one new chain (private marker guard). Both existing finalRoot/finalPhysical entries become the same complete 0736 successor, retain all prior physical owners as predecessors/allowedPhysical, and make the new successor their sole final closure. The new guard has exactly this physical/final root. No chain deletion or other top-level metadata change occurred.

Direct current-byte comparisons to Git HEAD confirmed unchanged original 0556 and 0302 files, actual 2018 initial bind, 0018 initial certificate/bind/assert, managed 2010 book schema, 1246/1301 owners, flat-origin 0201/0212/0224, actual 1257 Local completion, and executable Rule B 0151 consumers. The original origin producer/digest and whole I1 inventory proof remain in the exact preserved bodies. The 0736 declaration contains only the two required full successors and one private guard; no financial writer, Source callback family, Rule B resolver or Worker/Office module was added.

## Evidence and limits

Root-provided `SP-08-LOCAL-CUTOVER-LEDGER.json` identifies `banking_modal_v2_test`, exact `05102026_0355_weekly_source_local_flat_origin_cutover_v1.sql` and ledger_count one. Reviewed the saved receipt only. `SP-08-closing.md` correctly keeps first-use, three amendment payload-size journey, negative currentness/origin/single-use/early-constraint business invocation and hosted/app behavior NOT_RUN. This source review establishes the finite guard and preserved source contracts; it is not an executed business refusal receipt or payment result. Root's local compilation/source/seal evidence belongs to its completion record.

## Fresh source SHA-256

| File | SHA-256 |
| --- | --- |
| `supabase/migrations/10102026_0736_banking_pay_next_local_detail_seam.sql` | `a878daf1db905ccaa71f8d29eb02703b17d3f35703ee34dbfef874e5da3b3509` |
| `supabase/repeatable/10102026_0736_banking_pay_next_local_detail_seam_v1.sql` | `7deb4c7b3c816fdab1edd70496f6ed15bd3d9acb6590ca498cd0e73a7d108f8a` |
| `docs/bpay-next/final-definer-closures.json` | `bec6abad348d6a8bec5ee37f7c80a386ee83fb7cfbff2eac10fff7ec9bd9771d` |
| `docs/bpay-next/design/SP-08-single-use-seam.md` | `10643dff5037c64c41faee26aacb81276c935d5245948367757adfce6cda087c` |
| `docs/bpay-next/design/SP-08-closing.md` | `b85122101f40c4f2d4e08cb42ad924ce788ee36c72d55374ee08525b658102a1` |
| `docs/bpay-next/SP-08-LOCAL-CUTOVER-LEDGER.json` | `2c28065c9e4041b0d2fac552e099a92e6ffd42aec4e8bf8d2cc95a3669c1752e` |
| `supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql` | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| `supabase/repeatable/05102026_0302_banking_pay_next_local_source_chosen_detail_v1.sql` | `4318b30d109a3ebbb1d7afda457a616874fc174f5e7a81061ca0978ac5f89001` |
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |

Exact complete extracted definition SHA-256: publisher predecessor `53a90137fb2b8086a962bfd0fc375d0ca7e0a154c6293663c3da4baae6a1f2dd` → successor `26f2eaa2b5bf5c998ede45adb8316e28a437979d941305efcaf889c21f65dce9`; detail predecessor `c7530d98a8a80c6f439b9cbe3afe03f7df8b0949684b6b89f3061f5b0f7d5758` → successor `725d04131ee2b2d2e0f4de1b9f27981fc8f08d2ee91a70765ce2e4da9db96e5c`. The bounded diffs consist only of the mint and consume insertions described above.
