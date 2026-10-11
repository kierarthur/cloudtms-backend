# DR-11 allocation and owed DR-08 hook review

11 October 2026. Independent read-only source review of root-authored `supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql`, comparing its Case prepare and expiry definitions against the current0559/0746 parents. No product writes, SQL execution, database access, tests, fixtures, hosted actions or commits. Application acceptance **NOT_RUN**. Policy X: no drift.

**PASS for the two writer hooks and retained DR11 allocation behavior. No essential defect found in this reviewed scope.** D14 migration/D18 assertion structural ownership is separately reviewed under the early DR37 prerequisite; this verdict does not close full DR37, runtime contention, or the whole DR11 item.

## Exact retained-body comparison

Source text comparison of each complete function, with line endings normalized, confirms equality to its parent after removing **one** new assertion call and reverting only the **six** first admission locks from NOWAIT to their original blocking form. No other body delta remains:

| Exact signature | Parent |0904 current entry |
| --- | --- | --- |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `11102026_0559_banking_pay_next_worker_reprepare_v1.sql`:1857 |0904:77 |
| `private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)` | `11102026_0746_banking_pay_next_expiry_projection_v1.sql`:5 |0904:593 |

The six NOWAIT locks retain original module→run→Candidate→job→worker/state or binding/worker order (Case0904:122–131; expiry611–618). They bound initial contention before the helper rather than pretending its later check can limit an earlier blocking wait. Lock refusal aborts the page transaction with native55P03; it creates no REVIEW, cursor spend, lease adoption or monetary effect. Original Case/component/period and sorted Work/Case financial/projection locks remain unchanged.

## Mutation admission and replay

Case terminal READY/REVIEW replay135–146 remains ahead of the assertion. Current active-job/lease/module/captured-count admission148–162 and stale checkpoint replay164–169 retain their exact predicates; an ahead cursor still refuses171–173. The assertion is called once at174 with the already-qualified Candidate, job, nonce and owner, **before** the no-Case completion shortcut176–183 and all subsequent floor/week/state/period/result/hold changes. It sits outside the nested business `check_violation` handling; foreign fence55P03 is not converted into an economic review outcome.

Expiry original deadline/request/root-review/count scope619–626, terminal EXPIRED replay627–630, current lease/order admission631–635 and checkpoint replay/ahead refusal636–640 remain unchanged. One assertion at641 precedes the READY→RELEASING worker changes, original Work/Case release, native ORIGIN requeue/wait continuation, financial revision/weekly exclusion and final/projection checkpoint publication. The original72h fence and exact expiry winner are retained; no due selector, expiry receipt, cleanup formula or fake expiry classification is introduced here.

## DR11 economic census

The existing plan `11102026_0204_banking_pay_next_case_allocation_plan_v1.sql` and immutable row kernel `04102026_0290_banking_pay_next_case_allocation_row_v1.sql` are unchanged.0204:306–314 derives funded time else created time, preserving stored-credit origin age;372 uses the full age/Case/component tuple without a kind tier.383–394 calls0290 once per recovery and carries E/H.0290:72–113 retains protected-debt threshold/floor, Overpayment headroom-only, signed H, exact capacity and reason precedence.

0904 still captures only selected instructions before allocation, consumes DERIVE340 and ALLOCATE454, seeks the native full tuple450–452, and commits immutable results/typed reservations with maintained totals and the same E/H/cursor513–523. Accepted Case LEAF rule/revision and captured Candidate floor remain authority; actual Case/component/period balances stay live under the existing money locks. Payroll gross527 still excludes NET repayment reservations. Current retained parent/plan/kernel evidence is detailed in `design/DR-11-investigation.md`; no alternative allocator, tier, signed-negative capture rule, Worker action or screen is required by DR11.

## Source receipt and limits

0904 SHA-256 `FA3C73A83075F67246E62ADEA7BA1467C019BADA601D3DB880EC51B48CDD1A9E`; parent0559 `514FB6A069DFE860D06C76D98302554BF3ED93E8E027CDBAC460BF8CEA7DA9A6`; parent0746 `F1E6505C4F0CE89BE8DA8090DBC30467EF0EC50518536EB797F11DAB37D152E6`;0204 `2EEAAFB8F14444715C30E13DC898F9B35614DE1F53920344913F2986E09D6B0C`;0290 `400E854BFD5619F7BD004E9E922D4681A673216EE001DD1A7CBD81ECF6D20AC6`.

The registry still named0559/0746 when this snapshot was read; root must register0904 per signature and preserve plan0204/row0290 ownership with its normal source/contract closure. Compile, installed/API behavior, page-size equality, concurrent contention and later app examples are **not executed or certified by this review**. The mandatory first-section shared chain/completion note remains the card's documentation delivery.

## Installed0904 / alias-only0910 final source amendment

**SOURCE PASS reaffirmed for the reviewed writer hooks, alias successor, finite source-gate amendment and final-owner registry.** No essential defect found. Root reports0904 was loaded before its source-gate result completed and is now immutable; this reviewer did not execute or independently certify that installation.

Compared the whole expiry function in `11102026_0910_banking_pay_next_fence_expiry_row_projection_v1.sql` with the0904 expiry parent. After normalizing line endings and reversing exactly five `select dr11_row.*` projections plus their matching single native table aliases, the complete function text is equal to its0904 parent (21,349 characters after reversal). The five qualified row reads are Run, Candidate control, job, expiry binding and optional Worker (`0910:25–29`). They retain the same strictness, predicates and NOWAIT locks. No other function-body difference remains. Original scope/replay admission and the one assertion before mutation are unchanged (`0910:30–52`); all money, checkpoint, original-membership projection, native ORIGIN wait, final invalidation and original72h fences retain the reviewed parent behavior.

The source gate checks the exact immutable0904 SHA-256 before granting its historical construct map (`scripts/bpay-next-source-gates.mjs:1981–1985`). The amendment lists precisely five keys, each containing the expiry routine signature, its named ROWTYPE target/table, and the exact initial `SELECT * ... FOR UPDATE NOWAIT` statement, with one counted occurrence each. Only that exact physical0904 file receives this augmented map (`2028`).0910 is checked with the normal baseline; a different statement, target, signature, occurrence count or installed0904 byte sequence has no new allowance. The gate still applies SQL write-boundary/numeric checks to every source (`2018–2019`), frozen authority to changed sources (`2028`), and the original forbidden history-reader/dynamic-execution checks (`1315–1322`). Its DR11 callee additions enumerate only the assertion from the two exact writer signatures and `pg_catalog.clock_timestamp` from the helper (`1304–1312`). No blanket file/body/table, dynamic SQL or history exemption is added.

The current final-definer registry now points expiry to0910, Case prepare and the assertion helper to0904. The allocation plan remains0204 and the immutable row kernel remains0290. This closes the historical registry-pending statement above without replacing another financial owner.

Fresh reviewed SHA-256:

| Saved source | SHA-256 |
|---|---|
| `supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql` | `FA3C73A83075F67246E62ADEA7BA1467C019BADA601D3DB880EC51B48CDD1A9E` |
| `supabase/repeatable/11102026_0910_banking_pay_next_fence_expiry_row_projection_v1.sql` | `14803CD993EF6BB7E3C11835E73AD2219520CE5F1DCF55B0DABDB74088D9C654` |
| `scripts/bpay-next-source-gates.mjs` | `B74DCE5D3344D0CFE3C570D1CA074F2963AF5C67AD4C8AED8C5E982C97BB10E9` |
| `docs/bpay-next/final-definer-closures.json` | `56C8F726194C1F4AB353430C3E632A23275967FF221D44CCA7976947A7398BAB` |

Source review only: no source-gate run, compile, SQL/database/business execution, tests, fixtures, browser, hosted action or application acceptance. Those checks remain **NOT_RUN** by this reviewer; no runtime or whole-item PASS is asserted.
