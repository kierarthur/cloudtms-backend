# SP-20 SQL independent source review

12 October 2026. **SOURCE PASS** for saved0005 and the finite registry/gate changes below. No essential defect identified. This is independent static source review; no SQL/database/business call, test, fixture, browser, hosted, product/checklist edit or commit was performed. Root-owned local compilation/normal checks and joined acceptance are separate evidence.

## Exact parent retention

Read `12102026_0005_banking_pay_next_source_tax_classification_v1.sql` and full original→actual diffs for all four entries in `.codex-local/SP20-parents.json`. Each recorded parent file SHA freshly matches saved bytes, and each recorded original complete definition matches its exact current parent. Each authored definition has the original signature/defaults/return type/configuration; complete unchanged body regions retain original statements and lifecycle/financial behavior.

| Complete successor | Actual bounded delta |
| --- | --- |
| stage_source_current_v1(uuid,uuid,uuid),0005:4 | approved INSERT finite native kind→tax assignment439–440; all original inventory/basis/origin/schedule/amount/ID/replay remains |
| initial_tsfin_certificate_v1(uuid),0005:632 | exact sealed classification comparison723–724 mirrors stage; all genuine initial TF/detail/policy/generation/digest evidence unchanged |
| capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer),0005:732 | closed answered Source boolean866–872; existing Umbrella/book refusal qualification873/903 and exact own tax941 |
| hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer),0005:1140 | immutable line facts1254–1262; closed book type/tax check1320–1329; bound Source reimbursement branch1344–1378 |

Stage WORKED_TIME/ADDITIONAL_UNIT are TAXABLE; actual EXPENSE/SOURCE_FIXED_EXPENSE are NON_TAXABLE. Existing component kinds/UUIDs/keys/ordinals/exclusions/source amount and pay-charge separation are retained. Mileage is its existing native expense representation. No new classification/helper/producer protocol or gross recalculation. Supporting initial assertion consistently closes its former “non-Work=NULL” conflict without touching original sealed replay, adding evidence or backfilling accepted history.

## Genuine binding, immutable cut and native guards

Capture's `coalesce(...,false)` positive allowance requires SOURCE, actual common HEAD or INITIAL SOURCE+event+snapshot arm, valid native source channel and exact WORK/ADDITIONAL TAXABLE or EXPENSE/MILEAGE NON_TAXABLE. Wrong/NULL treatment and unsupported kinds cannot waive original guards. Direct PROTECTED_WORK/noHEAD and ordinary Umbrella restrictions are unchanged. The existing binding lookup still requires exact original origin/root/version/snapshot or HEAD, revision/component, one publication and actual canonical native book. Native snapshot tax now equals the line, and every original R/H/source channel/money tuple remains. An unbound genuine Source component is still unavailable.

Hold obtains Work kind and immutable approved-line kind/tax via exact rp.approved_line_id + captured revision + component key; it does not choose a current/latest line. Book qualification uses that own exact tax and a closed Source kind/tax predicate. Reimbursement bypass requires an actual captured binding and SOURCE EXPENSE/MILEAGE NON_TAXABLE. Ordinary/unbound/unknown/signed/managed shapes keep their original pathways. All completed segment attribution, position/currentness/R/H/target comparisons, zero and positive inclusion, original money-lock/cursor/count/insert logic remain.

All original module/run→Candidate→job/worker→Work/position locks, limits1–100, active execution identity, lease/cursor replay and original query pin/OR47 exclusion/checkpoint/projection paths are retained. No new public-root/header locks, release or accepted-choice rewrite is introduced; PolicyX/OR44 and economic-vs-execution fences remain.

## VAT and once-only valuation

Reimbursement inputs explicitly reject NULL amount/policy/date/source or target channel, unsupported channels, absent actual Umbrella VAT flag and non-penny amounts1350–1353. It resets its local window fields every row1354 and reproduces current1157's eligible immutable policy/date query, latest date_from/native source_window_id order and count-at-winning-date ambiguity check1355–1364. Original missing/ambiguous named refusals are preserved. Native core1324:28–38 makes policy VAT nonnull and in0..100, so the helper's optional-input coalescing does not replace authority.

Actual `public._pay_umbrella_vat_calc(numeric,numeric,boolean)` is the unchanged canonical26052026:569–599 JSON ex/vat/inc calculator.0005:1366–1370 consumes its genuine vat/inc outputs and deliberately keeps target ex equal to retained source ex. Numeric conversion and existing run-line/hold NOT NULL/conservation constraints fail closed, rather than fabricating amounts. PAYE gets source ex, zeroVAT and equal inc1372–1373. Each row keeps its exact immutable policy window ID in the original financial inserts. No ERNI or manufactured same-channel argument is used for reimbursement.

Taxable/other original lines retain the single original `bpay_next_value_source_for_target_v1` call1376–1384 unchanged; capture calculates no target money. There is no second calculator, rate/current-policy fallback or duplicated conversion.

## Boundary, ACL and gates

Exactly four private signatures advance to0005 in final-definer registry. Original predecessor chains remain; all four source ACLs restore postgres ownership and revoke PUBLIC/anon/authenticated/service_role at tail. No new table/migration/public function/grant/Worker/screen exists. Existing schema notification and complete transaction packaging are retained.

The final gate diff adds only `pg_catalog.round` (the native penny-input check) and `public._pay_umbrella_vat_calc` to the **exact hold signature** in ROUTINE_WORK_CALLEES and DR05_ROUTINE_WORK_CALLEES. No broad exemption or suppressed scan is added. The existing current-owner scan still checks these authored bodies.

No early0130/0200, ordinary tax publication/general hold treatment, Source producer, Case allocation or kernel edit occurs. BD127 explicitly retains those DR38/WB29/CM13a owners and deferred BLOCKED_ON_DR38 / AUTHORED_NOT_RUN transfer/settlement acceptance. Fixed expense mapping agrees with latest owner11Oct; no new policy conflict. This source PASS is not proof of application, contention, timing, queue or monetary journey acceptance.

## Fresh hashes

| File | SHA-256 |
| --- | --- |
| supabase/repeatable/12102026_0005_banking_pay_next_source_tax_classification_v1.sql | df49b9fc467ae9806effb19ec3b39820059fa46980a2f761f62c3f2ef8a96111 |
| .codex-local/SP20-parents.json | 09b9952a93877cbd0b722b04f6876b0efcce2fddcb8038d3d904bfbe39975293 |
| docs/bpay-next/final-definer-closures.json | e4c4b7d360d1143b63dfcb4b46bb39d6bee231246170ecf3bfdb35fa85e3eb4b |
| scripts/bpay-next-source-gates.mjs | 456d9f6dc9784d55391750a329c25c20a969956a3b5a5646afb3868df57c755e |
| docs/bpay-next/design/SP-20-tax-classification.md | 8e37b38a27fdb022d1e61bb64aac8dc5f8bfc3d57b58c2ff9cc799f3e103c04e |

Final gate amendment freshly re-read: both additions are confined to those two exact finite callee lists; **SOURCE PASS retained**. Current gate SHA456d9f6dc9784d55391750a329c25c20a969956a3b5a5646afb3868df57c755e and unchanged SQL SHAdf49b9fc467ae9806effb19ec3b39820059fa46980a2f761f62c3f2ef8a96111 were rechecked from saved bytes. No verification programme was rerun by this reviewer.

