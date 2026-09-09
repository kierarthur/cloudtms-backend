# H12 source-less cancellation pre-change evidence V1

Status: **TEST/EVIDENCE ONLY — CURRENT SOURCE UNCHANGED**  
Source head: `34d073fef04c365fc0c557f881b749ce88a7e7d7`  
Runtime authority: Miget TEST; it was not changed or claimed by this work.  
The `supabase` directory name below is historical repository naming, not the current hosted database authority.

## Plain-English result

Two facts are now independently frozen before any correction is written:

1. **C016 is a real current cancellation veto.** If an unpaid frozen manual adjustment lacks enough information to decide what should happen to it in a future pay run, both current cancellation routes stop before cancelling anything. The user has decided that this missing future-source information must not stop a confirmed-unpaid whole-Candidate or whole-Draft cancellation. That correction is not implemented here.
2. **C020 is not a normal unpaid-cancellation scenario.** The current unpaid-work builder can create only `PRE_BANK_CANCEL` or `NO_MONEY_UNWIND`. Its processor blocks anything else. The only other schema-permitted type is the paid/settled reversal type, which has a separate owner and must not be guessed into an unpaid route. C020 is therefore a defensive legacy/corruption guard, not a reason to change a valid confirmed-unpaid cancellation.

Only whole-Candidate and whole-Draft/run cancellation are supported. Nothing here adds item-, component- or Timesheet-level cancellation.

## C016 — exact current behavior

The shared detector is `public._pay_detect_manual_adjustments_for_carry_forward`. It treats a manual-like item with no recognised source as safe for an automatic carry-forward only when all of these frozen facts exist:

- a non-null and non-zero amount including VAT;
- an amount excluding VAT;
- a VAT amount;
- a nonblank description;
- the Candidate identity;
- PAYE or Umbrella channel;
- and, for Umbrella, the Umbrella/payee context.

When every fact exists, the detector returns `SOURCE_LESS_CARRY_FORWARD_SAFE`. The current PRE_BANK and NO_MONEY owners then create/reuse the carry-forward, record the correction item and continue their existing cancellation writes.

When a fact is absent, the detector returns `SOURCE_LESS_AMBIGUOUS` with one of eight exact reasons:

- `MISSING_AMOUNT_INC_VAT`;
- `ZERO_AMOUNT`;
- `MISSING_AMOUNT_EX_VAT`;
- `MISSING_AMOUNT_VAT`;
- `MISSING_DESCRIPTION`;
- `MISSING_CANDIDATE_CONTEXT`;
- `UNSUPPORTED_OR_MISSING_PAY_CHANNEL`;
- `MISSING_UMBRELLA_PAYEE_CONTEXT`.

Both current apply owners check this result and return `SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS` before carry-forward creation, correction-ledger insertion or item cancellation:

- `public.pay_pre_bank_cancel_apply_work_item` in `supabase/repeatable/04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql`;
- `public.pay_no_money_unwind_apply_work_item` in `supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql`.

This is the red pre-change fact. It does not mean money moved and it does not make it safe to invent a source or carry-forward.

## C016 — accepted future boundary, not an implementation

The future policy-neutral correction must:

- complete the existing confirmed-unpaid cancellation;
- atomically preserve every available frozen Candidate, payment, item, amount, sign, PAYE gross/net, tax, ex-VAT, VAT, channel and payee fact in the existing correction evidence;
- keep missing facts explicitly missing;
- mark the item for source-restoration investigation;
- never create an automatic carry-forward from ambiguous evidence;
- never choose a future financial outcome;
- project one idempotent Banking alert separately from financial completion.

The current computed alert `MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS` is not a durable post-cancellation alert. Its Candidate scope includes a pending carry-forward only while the source batch is not `CANCELLED`, and its identity is grouped as `GROUPED:ACTIVE`. Once cancellation succeeds, this source disappears. There is no current `MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED` projection.

The existing correction row and alert acknowledgement identities can support a later exact design, but this guard deliberately does not implement it. No current UI or RPC safely resolves the missing source. That later source-treatment choice remains a separate policy decision; it cannot be allowed to veto the cancellation.

## C020 — corrected scenario

`public.pay_payment_correction_expand_work` is the canonical unpaid-work builder. Its exact mapping is:

- a `NO_MONEY_UNWIND` request creates `NO_MONEY_UNWIND` work;
- every other request admitted to this unpaid builder creates `PRE_BANK_CANCEL` work.

It cannot emit a third work kind. The table does permit `SETTLED_REVERSAL`, but that is paid/settled recovery territory. `public.pay_settled_payment_reversal_apply_work_item` explicitly rejects a new reversal and directs the established amendment/recovery process instead.

`public.pay_payment_correction_process_chunk` handles the two unpaid kinds explicitly and returns `BLOCKED_BY_UNSUPPORTED_SOURCE` for anything else. In plain English, that means the unpaid processor has received a legacy, corrupt or wrong-engine row. It must fail closed without moving money or being silently relabelled.

Therefore C020:

- cannot arise from canonical confirmed-unpaid generation;
- does not block a correctly formed whole-Candidate or whole-Draft unpaid cancellation;
- is unrelated to C016's incomplete future-source evidence;
- must not receive an automatic source adapter as part of the C016 correction.

## Guard coverage

The focused guard checks:

- exact source-file hashes at the frozen head;
- the current veto's position before all cancellation writes in both apply owners;
- complete-fact PAYE and Umbrella source-less safe cases;
- all eight ambiguous-fact reasons;
- the alert's non-CANCELLED source filter and grouped active identity;
- canonical unpaid work-kind generation;
- the defensive unsupported-kind branch;
- the separate paid/settled owner;
- mutations attempting to bless the current C016 veto, invent a carry-forward, auto-route C020 or claim C020 blocks canonical unpaid cancellation.

## Change boundary

Files added by this evidence step only:

- `tests/fixtures/07092026_2030_h12_source_less_cancellation_prechange_v1.json`;
- `tests/07092026_2031_h12_source_less_cancellation_prechange.test.mjs`;
- this report.

No SQL owner, Worker, frontend, release, contract, manifest, Bible, Miget database, payment, provider, settlement, remittance or mail state was changed.
