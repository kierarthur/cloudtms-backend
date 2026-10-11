# DY-08 current investigation — hours-only integrity refusal

11 October 2026. Sequence95; depends on DY04. **Read-only investigation complete; product and joined acceptance pending.** No SQL/product/checklist/spec changes, database calls, tests, fixtures, screen capture/render or hosted actions were performed. Only this note is written.

Read the full owning card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1071–1090`, all D01–D04 checklist rows at2799–2819, master shared-owner chain `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:67–70`, owner answers:52–53 and current final-definer registry. The binding outcome is an integrity refusal only: Daily is hours-only; no Daily ancillary admission, new option or payment feature. Ordinary/Weekly Additional and separate expense-claim Timesheets remain their original owners.

## Exact final source and required SQL delta

The actual final `private.bpay_next_daily_approval_evidence_v1(uuid)` is **`supabase/repeatable/11102026_0925_banking_pay_next_daily_five_standard_rate_rows_v1.sql`**, registered at `final-definer-closures.json:12414–12428`. This is DY02's completed successor of0018 and DY08's second/final named edit position. Re-read/copy0925, not the historical0018 pack. Preserve its complete current Contract/NULL-Contract identities, dated resolver, five Standard rows/all fifteen cells/NULLs, JSON contract, search path, owner and closed private ACL.

After the existing strict TF/physical identity reads at23–24, the current combined provenance IF spans25–52. Its ancillary tail47–51 contains exactly:

| Native TF field | Existing predicate to move unchanged |
| --- | --- |
| additional_units_json | `IS DISTINCT FROM '{}'::jsonb` |
| additional_pay_ex_vat | `IS DISTINCT FROM 0::numeric` |
| expenses_pay_ex_vat | `coalesce(value,0)<>0` |
| mileage_pay_ex_vat | `coalesce(value,0)<>0` |
| mileage_units | `coalesce(value,0)<>0` |
| travel_pay_ex_vat | `coalesce(value,0)<>0` |
| accommodation_pay_ex_vat | `coalesce(value,0)<>0` |
| other_pay_ex_vat | `coalesce(value,0)<>0` |

Move those checks into their own branch **before the remaining provenance predicate**, with the exact owner comment `-- Daily Timesheets are hours only (owner 8 Oct 2026); integrity guard only`, raising **SQLSTATE23514 / `BPAY_NEXT_DAILY_HOURS_ONLY_VIOLATION`**. The two IS DISTINCT predicates also reject NULL; do not replace them with COALESCE and inadvertently admit damaged data. The other six retain their existing NULL-as-zero semantics. Nonempty Additional-unit JSON is invalid even when its contained quantity is zero. No charge-side field, available-rate schedule, new reason/detail protocol or extra ancillary check is assigned by this card.

All non-ancillary provenance conditions must remain unchanged and keep `BPAY_NEXT_DAILY_APPROVAL_PROVENANCE_REQUIRED`: genuine DAILY/HOURS scope, native current/nonarchived/nonrevoked/nonadjustment identity, exact version and authorisation, SELF_REPORTED, Candidate/Client/role/channel, issue flags, AGGREGATE/policy/rate-reference shape and clocks/breaks. Preserve all later identity/date/rate-authority/channel/precision refusals too. Actual callers qualify genuine Daily; a direct private invalid non-Daily/non-HOURS invocation should retain its provenance refusal rather than acquire a misleading Daily damaged-data label. Keep any necessary scope qualification finite; do not duplicate the complete provenance expression or weaken its refusal.

**No publisher or0939 guard rewrite is required.** Current1001 publisher calls this helper for NULL-Contract `DAILY_HOURS` at94–96 and for genuine contracted Daily at220–226. Neither call catches/continues on this error. The existing native authorisation-trigger transaction therefore retains atomic refusal; do not turn it into partial approval/payment. DY04's0939 route/withdrawal/Work guards and DY05's1001 frozen settings/revision support stay intact. No Source/pay-query admission or producer is involved.

## Narrow Worker / Office propagation

The card says Worker:none / Screen:none at1084–1085. Checklist D02/D03 explicitly require typed refusal preservation and a damaged-data blocker. Reconcile these as a small change to **existing failure handling**, with no new route, command, reader, screen, control or data repair action.

- `broker/src/index.js` already has a finite missing-rate parser at93942–93978. Add adjacent exact-code recognition for the new hours-only refusal; leave that existing Daily/Contract missing-rate parser unchanged. Recognise an actual structured SQL23514 with exact message, or its already-sanitised exact code in existing failure results. Do not recognise arbitrary substring/prefix/raw database prose or map every Daily provenance error to this code. Return the same existing failure fields with the exact closed code, nonretryable damaged-data explanation and no replacement/partial success.
- Existing single native AUTHORISE's `mutationErrorResponse` at168108+ and definite-refusal catch168624+ must preserve it before unknown-outcome reconciliation. Reuse existing AUTHORISE-only bulk item94118, stored polling94335–94336, atomic-pair refusal94533–94537 and original decision-result95995 seams where the same native response traverses them. Preserve the flattened exact code/message/nonretryable flags on readback; avoid DY04's former lost-context bulk mistake. The retry wrapper's finite business-code set95216–95235 should classify this exact known refusal. Existing Source refusals,55P03/40001 retries and uncertain transport recovery remain unchanged.
- The named Office module `js/bpay-next-office.js` currently has no dedicated Daily refusal copy. Its public API can provide one finite exact-code explanation to the existing native Timesheet/Authorise error presentation, retaining actor/window qualification and original error identity. Native single Authorise catch paths are `js/main.js:315330–315332` and315930–315932; existing bulk result normalization is197137–197138. A minimal caller hook may be needed to consume the Office mapper; a map that is never reached does not satisfy D03. Preserve the original Source-specific mapper and missing-rate navigation; do not offer a Contract-rate button for damaged ancillary data.
- `bpay-next-job-step.js:431–455` and Workbench `jobError` validators already preserve bounded closed BPAY_NEXT codes for their native POSITION failure path. The new code fits that existing type. No new queue schema/error framework is required. If the code is actually presented through native Workbench failed-job copy, add only its exact explanatory case to existing `js/bpay-next-workbench.js:793–798`; this is supporting copy, not a fabricated new delivery path. Do not claim that synchronous authorisation rejection ran a POSITION job.

Suggested truthful existing-notice copy: “This Daily Timesheet contains pay items that are not allowed. Daily Timesheets are hours only. Correct the Timesheet data before paying it.” Keep the original closed server code for diagnostics; do not show an internal SQLSTATE or claim all Candidates/bulk siblings had no prior payments. No screenshot design is owned by this card; no new screen or pack work is warranted.

## Closure evidence boundary

D01 must review the complete0925 successor with exactly this split, original bytes/ACLs and new final registry owner. D02/D03 must prove the exact real AUTHORISE/result caller mapping from saved source, with existing unknown behavior retained. Root owns normal required compile/syntax/source/contract/release checks. D04 remains future joined **NOT_RUN**: genuine Daily hours succeed; damaged authorised-future-fixture ancillary data yields the dedicated23514 and no partial Timesheet payment; broken clocks/breaks retain the old provenance refusal; ordinary Weekly Additional and separate expenses are unaffected. No extra fixture, hash/race laboratory or local financial test programme is proposed.

## Inspected current raw hashes

| File | SHA-256 |
| --- | --- |
|0925 Daily evidence helper|39c1a1f69fc5c9a5152aa08ca037e295e6e08b64bbd0651177556fa6fe52c99a|
|1001 Daily frozen publisher/guards|b5f2f9ec742a11a88ede05ae0eb433725b410907827bf10300b3540824a92a72|
|0939 contractless publication/route support|4a21ab611b90ed39fed312a5d8aaa0754e72f980da9c621ee815ec1f611c6c1a|
|broker/src/index.js|fcf488bc88ee955bfe560d49cc5d61a1055c5c87eab4087487e843cac731bb65|
|broker/src/bpay-next-job-step.js|70a989b65ddf08df2b6f5f5133ac0521300708021382870730a9f58051907651|
|FE js/bpay-next-office.js|0dcf5fc982195684fc2001985bb258a6c9397b89b7c34f0888612860c0f77c15|
|FE js/bpay-next-workbench.js|f01b5540b281e9ee30b45098811f6ec7c8067ddad93f9247faa4bea760990e3e|
|FE js/main.js|a65011bef9bb78bb83e4b8e2ab7481690dd724c83ef6b3716b79ff6c11292915|
