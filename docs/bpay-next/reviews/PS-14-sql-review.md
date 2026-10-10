# PS-14 SQL source review

10 October 2026. **PASS — H01–H03 closed; no remaining source finding.** Fresh-read all455 lines of current0639, the exact PS-14 card/checklist and approved design, native cancellation/settlement/materialisation owners and final registry. Source review only: no SQL, tests, fixtures, catalog, runtime, app or business action executed. Root alone edits product.

## Findings

1. **H01 — pre-build ordinary Umbrella first use references a nonexistent relation.** At initial0639:255, the eligibility probe names `private.bpay_next_net_request`. The real table is `private.bpay_next_paye_net_request`;0380's similarly named constraints are not a table. Valid confirmed ordinary Umbrella0/0 reaches this branch and will fail when that statement is first planned. Correct only the table identity; preserve the original2018 eligibility predicate.

   **Closed:** current0639:266 names the actual `private.bpay_next_paye_net_request`. The narrow ordinary Umbrella0/0 eligibility is retained.

2. **H02 — active reservation evidence is computed but does not qualify positive cash.** The ORIGINAL_OBLIGATION branch at287–297 and per-leg outstanding branch at340–350 calculate `v_active` and label NONE when false, but still return positive `remaining_reservation_cash`. The approved design/card require genuine original active reservation rather than absence of an outcome. After checking lifecycle precedence, positive cash without the original scoped active WORK/Case reservation must return typed conflict/refusal, not RESERVED cash. Genuine zero/internal-zero and already executed/cancelled branches remain exempt. Do not invent a hold for certified-zero Work or zero Case results, and do not sum holds into NET.

   **Closed:** current0639:313–315 and368–369 return EVIDENCE_CONFLICT without a cash amount when positive parent or leg cash lacks scoped active holds. True zero is exempt. Exact cancellation and actual receipt/outcome classifications still precede this qualification; no hold sum or money calculation was added.

3. **H03 — completed Case cancellation without a materialised transfer loses its retained projection.** When original transfer `o` is absent, initial0639:211 selects only `p.retired_at_utc IS NULL`. Actual0460 FINAL retires the cancellation binding's accepted projection and atomically marks worker/binding/request CANCELLED. The later cancellation qualification compares nonnull `b.projection_id` to the now-null `p.id`, so that genuine no-transfer cancellation falls through to EVIDENCE_CONFLICT instead of CANCELLED0. Resolve the exact native cancellation binding's retained projection before qualification, including its retired row; retain worker/Candidate/request/binding scope and never choose latest history. Group and transfer-present cancellation already resolve the original projection by its retained ID.

   **Closed:** current0639:215–223 first resolves the exact CANCELLING/CANCELLED binding projection, including retirement, before falling back to an ordinary current projection. The following qualifier binds the actual request, worker, Candidate, projection and optional original transfer/group scope. This supports both native accepted-projection0 cancellation and retained nonnull accepted projection; REQUESTED/BLOCKED do not produce cancellation-zero.

## Correct portions

Each of the four copied2025 function definitions, individually extracted through its `end $function$`, is byte-identical to current original2025. The successor deliberately omits the old trigger DDL and uses one transaction; it preserves signatures, logical owner, security/search path and revoked private-function privileges. Existing destination execution/posting guards and completion money behavior are not rewritten.

The new private reservation helper and STATUS function are STABLE SECURITY INVOKER, with fixed `pg_catalog,private` search path and no service/browser execute grant. STATUS invokes the helper for the exact run/worker header and each emitted transfer. Its existing module/scope guard, bounded transfer keyset, header-inclusive byte budget and conservative complete flag remain. It does not call the bank-locking execution routine from the reader or add a financial write/counter, provider, new command or current pricing lookup.

Original transfer/group projection identity is used after materialisation rather than the current header pointer. Qualified original-vector mode counts parent cash once and suppresses child counted cash during partial building; original-leg mode emits paged facts with NULL header reservation cash. Actual group completeness/conservation and per-leg OWN/ONEOFF amount are checked. Reissue is separately excluded. Confirmed ordinary Umbrella's distinct pre-build0/0 and post-build0/1 are recognized without changing unrelated net_pending semantics.

Case-only works without manufactured WORK members. The helper resolves original completed NET/DRAFT state and optional completed native destination vector. Actual SETTLED intake/outcome-request identity wins before held capacity/posting finishes; returned original cash does not revive the Draft obligation. Internal receipt/request identity distinguishes internal pending/closed zero. Uncertain execution remains NULL. REQUESTED cancellation is not treated as a fence; group qualification requires the exact checked full original-leg set and native CANCELLING/CANCELLED binding/request/worker scope.

The narrowed record SELECT projections contain every field subsequently referenced. Ordinary branches initialise `ns` and `ds` with the complete empty Case descriptor before hold qualification. `t` is assigned by the scoped strict lookup before per-leg use; `n` is assigned whenever a projection is present, and the legitimate no-projection ordinary Umbrella branch returns through original-obligation mode before any destination-record access. The no-row SELECTs retain explicit field descriptors. The selected columns match the native core, Case allocation/cancel and destination schemas. Added channel/input-kind and no-original gross/entered-NET tuple checks fail closed on contradictory projection evidence; delayed materialised originals retain their own projection authority.

The final registry has one0639 authority for four copied2025 routines, the0580 STATUS successor and the new private helper, with appropriate predecessors. Existing physical sources remain unchanged. Worker/Office closed-schema mirroring and connected acceptance are separate pending review; no all-card/runtime PASS is claimed here.

## Final source receipts

| Source | SHA256 |
| --- | --- |
| `supabase/repeatable/10102026_0639_banking_pay_next_ps_c15_reservation_status_v1.sql` | `15c88f4c45051087976ed9187a741da494c69a052611c2a41892417f9665a22d` |
| `docs/bpay-next/final-definer-closures.json` | `1f9f904cdbd0dab4196688ecdf4c20e34a71d77d755221ab5e49f8ab71859758` |
| `docs/bpay-next/design/PS-14-SCOPE.md` | `d2a3e40c1b905d36a152b151f73c19541c468e322d8979db6accdaf001157b7a` |
| original2025 | `5ed7e53c884b507444bda67c7ce6061a82c6b8690a3461ffa80846aaa98c5d1b` |
| original0580 | `ab95db05d07150d131ba027951878b9833e4e5b5e1cb9f4873cfa55fddc3a1ba` |
| native0460 Case cancellation | `4417b25f96d6caefb627eae013c0c956268356104be15e2411dead56cc60bcf5` |

Actual app/Worker/queue/minute joined acceptance remains **NOT_RUN**; installation and root-run source/compile checks are separate receipts.
