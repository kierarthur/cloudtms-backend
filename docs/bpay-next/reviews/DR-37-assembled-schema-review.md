# DR-37 assembled schema, native bridge and ACL source review

12 October 2026. **SOURCE PASS for the assembled schema, native transfer/reservation bridge and ACL scope reviewed here.** No essential finding remains in that scope. This receipt does not establish full item completion, runtime acceptance, business first use, or an execution/performance result. The reviewer made no product changes, SQL calls, test-data changes or hosted calls. Root reports these draft files as unlocked/uninstalled at this checkpoint.

## Reviewed bytes and authority

| Saved source | SHA-256 |
|---|---|
| `supabase/migrations/12102026_1215_banking_pay_next_case_net_replacement_v1.sql` | `4D9F2A154DE191084F98FB015322A4A7B400AA358344D04C97E962E95A9509B2` |
| `supabase/repeatable/12102026_1215_banking_pay_next_case_net_replacement_v1.sql` | `611DFCDE43B790AA2E9FDFC4D801316FE28C98D14FD70E5960D9C481C80FCFC3` |
| `docs/bpay-next/final-definer-closures.json` | `38C34D58DED1E818D3594051E1C28553EA2908F722D0588DB1831B6AA118B859` |

Authority: approved plan card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:716–738` and `addendum/R2_EXACT_CONNECTIONS.md:27–46`, particularly the accepted pointer/highwater distinction, retained three refusal codes, bounded commit fence, original NET cleanup and later SIMPLE_CANCEL handoff. Earlier lifecycle/cancellation findings and their closure remain in `DR-37-intake-lifecycle-review.md` and `DR37-cancel-review.md`; this receipt checks the actual permanent assembled files after the final static DDL and qualified-point-read changes.

## Schema and lifecycle binding

- Migration lines 7–26 keep the pending request separate from the highwater and accepted request number. The audited worker CHECK retains the original NULL/REVIEW alternative and adds only the exact READY PAYE refusal state with no pending request and a failed later highwater; its partial index matches that native current-refusal predicate. Lines 29–34 refuse an unfinished historical Case NET execution instead of fabricating a replacement receipt.
- Lines 35–55 preserve the original no-Case request shape and audit the exact original five-stage state constraint before replacing it. The final static DROP names `bpay_next_case_allocation_state_net_stage_check`; the preceding lookup checks both its native table/column membership and original expression. The original expression agrees with `04102026_0380_banking_pay_next_case_net_binding.sql:32–33`. Root's fresh catalogue confirmation of the constraint name is attributed to Root, not to a reviewer database query.
- The replacement header and staged members use native UUID identities, BIGINT counters/component ordinal and `private.penny_amount` fields. Counter order, all-or-none keyset tuples, finite dates/timestamps, pre-job pairing, stage/finished/refusal pairing and terminal full-count/source equalities remain closed (migration lines 57–153). Request numbering permits gaps without treating every highwater as accepted.
- The Worker, command member, job/module, captured state, state/channel, instruction and period identity FKs use existing native composite keys; `LIKE` staged-result copying is supplemented with explicit PK, uniqueness and native FKs (lines 163–183), rather than assuming `LIKE` copies those constraints. The supporting keys are present in the original `04102026_0300_banking_pay_next_case_instruction_core.sql` definitions, including Worker/run/Candidate at line 217, instruction identity at line 272, job identity at line 303 and state identities at lines 335–336.
- Immutable refusal and cancellation handoff/member records bind exact replacement/Worker/Candidate/command identities (migration lines 200–237). Lifecycle guards qualify the native points and job ownership. Deferred terminal validation reads the current replacement by its exact ID and requires the genuine terminal job/command and appropriate accepted/pending pointer outcome (repeatable lines 1674–1702). CANCELLED replacement cleanup preserves the pending handoff until the genuine later SIMPLE_CANCEL lease consumes it; cleanup is not an early Worker cancellation.

## Full native bridge parent comparison

Whole function bodies were compared with the current registry predecessors. Apart from the stated predicates and qualifying the same native point reads as `alias.*`, their original replay, permission, module, lock, frozen membership, monetary and receipt branches remain present.

| Current assembled owner | Exact predecessor | Narrow change reviewed |
|---|---|---|
| `private.bpay_next_begin_simple_transfer_v1(uuid,uuid)` — repeatable line 2554 | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:1524` | PAYE consumes the accepted projection at any positive accepted number, requires request equality and pending NULL. Umbrella retains its original request 0/projection 1 transition and frozen input restrictions. |
| `private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` — line 2681 | `10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql:5` | Selects the accepted unretired projection, requires pending NULL, and applies request equality only to PAYE. The original destination/Case delegates and terminal member replay precede eligibility; frozen gross/NET member conservation is unchanged. |
| `private.bpay_next_begin_case_transfer_v1(uuid,uuid)` — line 2896 | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql:3347` | Adds pending NULL to the existing accepted-positive/equality admission; captured economic preparation/selection state, complete NET projection and native receipt checks remain. |
| `private.bpay_next_original_reservation_v1(uuid,uuid)` — line 2990 | `10102026_0639_banking_pay_next_ps_c15_reservation_status_v1.sql:171` | Reads the native pending ID and reports pending NET when that pointer exists. Existing transfer/CSV, cancellation/reissue, no-Case and Umbrella reservation authority is retained. |

The bridge does not create an input pricing owner or use an unaccepted highwater as a financial projection. Current header/cache source checks in `DR-37-readiness-bridge-investigation.md` remain applicable: replacement metadata invalidates the reader version, while accepted money follows the original contribution; READY current refusals and historical REVIEW refusals are separate bounded native point branches.

## Final static DDL and exposure

Migration lines 238–267 now contain six explicit owner/RLS/policy/revoke blocks. Each private table has owner `postgres`, enabled and forced RLS, owner-only `CURRENT_USER` policy, and ALL revoked from PUBLIC, anon, authenticated and service_role. No dynamic owner/policy loop or public table was added. Repeatable lines 359–387 contain six explicit TRUNCATE guard attachments; the duplicate identical `DROP TRIGGER IF EXISTS` lines are harmless redundant DDL, not a lifecycle or exposure defect.

The assembled repeatable has 40 distinct function definitions, each with its exact owner and closed four-role revoke (lines 4671–4752). Existing predecessor language, volatility, security and search-path configurations are preserved for replaced owners. Its six SECURITY DEFINER functions are the existing private cancel claim/apply and simple transfer begin/build owners plus the existing public financial job/intake owners; the new private helpers are SECURITY INVOKER. Only those two existing public functions regain service_role EXECUTE (lines 4749 and 4752). The existing worksheet view remains `security_invoker=true` (line 3495) and receives the same owner/revoke treatment (lines 4753–4754). Source inspection finds no new public SECURITY DEFINER membership or browser EXECUTE/table grant; fresh catalogue parity is still a separate Root check.

The previously identified missing assembled EOF notification is **CLOSED**: `notify pgrst,'reload schema'` is now at actual line 4755 before COMMIT. The 88 newly qualified `SELECT ... INTO` point reads retain their table, predicates and lock behavior; they do not broaden the queries or relax a source gate. No blanket dynamic-SQL/history waiver is justified by this review.

Business/first-use, queue/cancellation journeys, concurrency, full staged-allocation behavior and performance remain **NOT_RUN by this reviewer**. Other scoped source receipts and Root's required release checks govern those remaining claims.
