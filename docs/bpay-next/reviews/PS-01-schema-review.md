# PS-01 schema source review

10 October 2026. **PASS — initial cancellation finding closed in the fresh paired successor.** Reviewed full new 0533 migration and paired repeatable, PS-01 scope/BD031 and future joined acceptance, exact 1324/0300/2011 schema predecessors and current 0145/0390 guards. This is a source-only D01/D05 review; no SQL, tests, fixtures, runtime, network or secret operations were executed. Root remains the product writer. Installation is not claimed.

## Resolved initial finding — cancellation could clear the retained bridge

In the initial reviewed batch, `bpay_next_transfer_umbrella_bridge_required` exempted `status='CANCELLED'`. Its shape CHECK accepted all six added fields NULL, and the composite FK used MATCH SIMPLE, which skipped that all-NULL key. Existing `bpay_next_transfer_guard_v1()` in 0145 allowed a BANK transfer's MEMBERS_READY→CANCELLED transition and manually froze only the previously existing fields. It did not compare the new bridge fields in that branch.

Consequently this exact source-permitted transition loses the recorded bridge:

```sql
-- Existing original UMBRELLA transfer, currently MEMBERS_READY and fully bound:
-- preserve every old identity/cash/member field;
-- set status=CANCELLED and all six new umbrella bridge fields=NULL.
```

Both new CHECKs then pass, the new FK is skipped, and the old MEMBERS_READY guard accepts the transition. Projection immutability does not prevent deleting the transfer's reference values. This contradicts the saved joined check that a completed ready bridge cannot be cleared/changed, and the card's retained stored transfer bridge.

Keep the all-NULL allowance for genuine cancellation before assembly. Add the minimum finite support that refuses a change/clear of an already captured bridge: for example, a scoped successor of the existing 0145 transfer guard that compares only the new bridge tuple once `old.umbrella_bridge_kind` is non-NULL, retaining every original state/identity clause. No new money owner, Case writer or protocol is necessary. A CHECK on the NEW status alone cannot distinguish cancelled-before-assembly from clearing a bridge during later cancellation. The exact FK pins the complete non-NULL tuple, but does not close this lifetime hole by itself.

Root was notified immediately with this exact finding before any approval/installation claim. The final closure below records its saved correction.

## Other schema findings

The migration's predecessor assertion selects the named constraint on the exact private table and uses `IS DISTINCT FROM` against the full audited `pg_get_constraintdef` string; missing or different predecessors fail closed before mutation. The new UMBRELLA clause adds stored payment/deduction TARGET INC and requires NULL entered NET. PAYE_MANUAL/PAYE_IMPORT and CASE_PAYOUT cash clauses remain the original expressions. The existing 1324 ex+VAT=inc/nonnegative cash and 2011 WORK-only shape remain, as BD031 requires. Physical PostgreSQL expression rendering/apply success was not independently executed by this reviewer.

The new projection target-inc columns are non-NULL nonnegative penny amounts with zero defaults. Transfer bridge columns have explicit all-NULL versus all-non-NULL shape checks, closing partial-NULL FK bypasses. The full shape requires original transfer/projection identity, UMBRELLA beneficiary/discriminator, nonnegative values, ex+VAT=inc and inc+payment−deduction=cash. The unique projection key and referencing tuple match in exact order, including projection ID, run-worker, input-kind discriminator, all money fields and cash. A complete bridge cannot point to a PAYE projection or another monetary tuple.

The existing 0390 projection guard compares whole rows excluding only retirement time, so the two new projection values are immutable without a rewrite. The migration creates no new table, grant, RLS policy, public API or financial backfill; existing private table ACL/RLS remains. The transfer FK supporting partial index is restricted to captured bridge rows. The tuple binding and final captured-tuple lifetime guard are finite and appropriate.

## Paired owner and future checklist

The full paired repeatable retains the current 2018 simple-transfer body and adds the terminal ordinary Umbrella zero-Case-terms refusal and exact projection-copy update before MEMBERS_READY. Existing member sum, cursor/lease, Candidate scope, PAYE adjustment, Case/destination dispatch and completion behavior remain. Claim/begin/0410 are untouched. D02's complete independent owner review remains its assigned review object; this schema review does not replace it.

D06 accurately marks all actual joined/installed/business/Worker/queue/minute clauses NOT_RUN and records PS-C01, ordinary 100/20/120 with zero Case totals, Candidate-leg member sums, preserved PAYE/CASE_PAYOUT, rejection of balanced fake Case terms, and later CM-13a/CM-14 Case ownership. Its “cannot clear/change ready bridge” clause exposed the initial finding and is now supported by the saved finite guard. No fixture runner, scenario programme or Payment Summary screen was added.

## Fresh correction closure

Read the complete newly added `private.bpay_next_transfer_guard_v1()` in the same 0533 repeatable. Immediately after the original DELETE refusal, an explicit `old.umbrella_bridge_kind is not null` gate compares all six added fields with `IS DISTINCT FROM` and refuses any change/clear with `23514 / BPAY_NEXT_TRANSFER_UMBRELLA_BRIDGE_IMMUTABLE`. This runs before every early-state return, so it closes both MEMBERS_READY→CANCELLED clearing and clearing after initial BUILDING capture. A NULL uncaptured old tuple does not trigger the new check. Existing all-NULL cancellation shape and all original state/identity rules remain; no new trigger was added and no previously disallowed transition was enabled.

Source comparison after removing exactly this finite added block shows the complete original 0145 function text unchanged (line endings normalized only). Signature, DEFINER/search_path, owner and private revokes are preserved. The actual final-definer registry now names the 0533 root/physical file for this signature with 0145 as predecessor. BD031 scope explicitly records why this support supersedes the earlier no-0145-rewrite design wording. Migration bytes remain unchanged. No actionable source finding remains for D01/D05 or the saved D06 checklist; actual compile/apply/business acceptance belongs to separate authorized evidence.

## Fresh reviewed source receipt (SHA-256)

| File | SHA-256 |
|---|---|
| Migration `10102026_0533_banking_pay_next_ps_c01_umbrella_cash_bridge_v1.sql` | `23d0375090ef26692d19051eed62b3033b2218b6cb4622430ce2c6421a822964` |
| Repeatable `10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql` | `a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314` |
| `PS-01-JOINED-ACCEPTANCE.md` | `a4ad570d596f5bceccf6376130369fb21b9c99366dc473fe2a054c79af44f2cf` |
| `design/PS-01-SCOPE.md` | `118de836a8b4fa9ebaee59288bdc237f430a3abb1be6e4fe4b07c826530d9f44` |
| Final-definer registry | `83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc` |
| Existing 0145 guard | `2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c` |
| Existing 2018 begin/build | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |

Only this review document was written by the reviewer.
