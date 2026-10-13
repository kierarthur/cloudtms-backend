# PS-01 transfer bridge source review

Reviewed 10 October 2026 against the full PS-01 card (06:1327–1352), current checklist D01/D02/D05/D06 and shared D03/D04 evidence, the saved scope, the F05 final BUILD owner, current guards and final-definer registry. **PASS for the saved source subset; no remaining concrete source defect found.** This is a read-only review. No SQL, database, business function, test, fixture, hosted call or runtime was invoked. Installation and joined acceptance are not established by this verdict.

## Exact successor scope

- The complete `private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` CREATE definition is identical to final `09102026_2018` after removing exactly the new 790-character terminal Umbrella block (line endings normalized solely for this comparison). All original dispatch, input/epoch/scope checks, lease, replay, cursor, member iteration, counter/sum checks, PAYE adjustment, transfer/job completion and reply operands remain unchanged.
- The new terminal block refuses payment and deduction Case TARGET INC **individually** unless each is zero. Balanced nonzero Case terms cannot pass. It copies the immutable projection discriminator and all five stored bridge values before the original MEMBERS_READY update, in the same transaction. Ordinary Umbrella WORK qualification remains the original F05 check; destination-group and Case-transfer owners are still dispatched first.
- `private.bpay_next_transfer_guard_v1()` is identical to final `04102026_0145` after removing exactly the new 615-character captured-tuple check. That check refuses any change or clearing of all six fields once OLD has a captured bridge, including MEMBERS_READY→CANCELLED. The original trigger attachment is reused; no additional trigger, calculator or business owner was introduced.
- CLAIM remains final `04102026_0130` with no successor or source change. The registry changes exactly the BUILD and minimal transfer-guard chains. The 0130 editor sequence remains PS-01→DY-09→DR-38→DR-22→CM-13a.

## Schema and preserved financial ownership

The migration asserts the named predecessor expression before replacing it. The PAYE_MANUAL/PAYE_IMPORT and CASE_PAYOUT operands match the existing 0300 clauses. Two dedicated projection columns carry payment-Case TARGET INC and deduction-Case TARGET INC; the new Umbrella branch enforces NET NULL and inclusive + payment − deduction = cash. Existing ex + VAT = inclusive and the separate F06 ordinary-WORK-only constraint remain effective. CM-13a/CM-14 still own future Case population; this source does not claim to produce their 84-cash example.

The five transfer copies use the existing finite exact-penny domain. Explicit IS NULL/IS NOT NULL predicates require either the entirely absent tuple or the complete nonnegative Umbrella tuple; they close the MATCH SIMPLE partial-NULL escape. A complete original Umbrella transfer requires the bridge outside the finite BUILDING/CANCELLED schema exemptions. The nine-column FK binds projection ID, run-worker, input kind, all five values and cash to the exact projection unique key. Existing transfer identity guards pin projection/run-worker/candidate; the 0390 projection whole-row guard includes the new columns automatically. The added captured-tuple guard prevents the all-null cancellation escape even though CANCELLED is exempt from initial bridge presence.

Original member scope, one-per-worker transfer identity, bounded actual member accumulation and final sum checks remain intact. The core MEMBERS_READY CHECK enforces the stored member sum against cash; 0145 enforces member append-only and BUILDING-only insertion. This is not a claim that 0145 recomputes an aggregate SQL SUM. Reissues retain original cash lineage and do not acquire an invented earning bridge.

The schema additions inherit the existing private table ownership, RLS and revoked access. Both new final function definitions preserve SECURITY DEFINER, the same fixed search_path, logical postgres owner and revocation from PUBLIC/anon/authenticated/service_role. There are no browser grants or public trigger changes.

## Closed review issue and remaining acceptance

**PS01-01 CLOSED:** An FK alone pinned only a nonnull bridge; clearing all fields while changing MEMBERS_READY to CANCELLED could have passed the all-null shape and cancellation exemption. The saved six-field OLD/NEW comparison now closes this concrete escape without changing original state transitions. BD031 explicitly records this narrow supporting guard successor.

The joined note is finite and explicitly marks all actual app/installed/business/Worker/queue/minute checks NOT_RUN. It retains the 100/20/120/0/120 ordinary bridge, mismatched/partial-null/foreign tuple and immutable-ready refusals, separate Candidate legs, preserved PAYE/CASE_PAYOUT/reissue behavior and deferred ordinary/moved Case bridge ownership. There is no manufactured joined receipt or claim that enum/schema installation proves financial acceptance. Existing original Umbrella rows without a bridge can cause the immediate new constraint validation to refuse installation; the migration provides no speculative business-data backfill.

## Reviewed SHA-256

| Source | SHA-256 |
| --- | --- |
| migration `10102026_0533_banking_pay_next_ps_c01_umbrella_cash_bridge_v1.sql` | `23d0375090ef26692d19051eed62b3033b2218b6cb4622430ce2c6421a822964` |
| repeatable `10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql` | `a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314` |
| predecessor `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| guard predecessor `04102026_0145_banking_pay_next_transfer_guards_v1.sql` | `2006007829398c9d849ddad959ea7e665f07b37a49f08dddd3e56385c8623a2c` |
| unchanged CLAIM `04102026_0130_banking_pay_next_simple_transfer_members_v1.sql` | `763176b54482954adf8c07e7415a86e005baae71848d03bba3245ee8c385907a` |
| `PS-01-SCOPE.md` | `118de836a8b4fa9ebaee59288bdc237f430a3abb1be6e4fe4b07c826530d9f44` |
| `PS-01-JOINED-ACCEPTANCE.md` | `a4ad570d596f5bceccf6376130369fb21b9c99366dc473fe2a054c79af44f2cf` |
| `final-definer-closures.json` | `83d8a27dc37d7193feaf651ca026b612344995d508281bbd839ec14b17d1d3cc` |
