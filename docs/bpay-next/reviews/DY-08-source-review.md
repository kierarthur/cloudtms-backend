# DY-08 independent source review

11 October 2026. **SOURCE PASS: no essential defect found in the current saved batch.** Read-only review of BD106, the full DY-08 card, all four checklist rows, the shared helper chain, complete current 0925 parent, successor 1129, existing publisher callers, backend Authorise changes and the actual frontend consumers. Product edits, database calls, tests, fixtures and rendering were not performed by this reviewer.

## Authority and scope

The controlling card is `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1071–1091`; checklist `IMPLEMENTATION_CHECKLIST.md:2635–2650` contains D01–D04. Master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:69,179` places the shared evidence helper at DY-02 → DY-08, sequence 95 after DY-04. Current local scope is `docs/bpay-next/design/DY-08-scope.md` (BD106). The original card's Worker/Screen “none” is compatible with checklist D02/D03's exact refusal preservation and existing failure copy: this batch introduces no screen, control, ancillary-payment admission or separate user flow.

## Database source

`supabase/repeatable/11102026_1129_banking_pay_next_daily_hours_only_integrity_v1.sql:25–33` contains the exact required owner comment, genuine DAILY/HOURS qualification and dedicated SQLSTATE 23514 / `BPAY_NEXT_DAILY_HOURS_ONLY_VIOLATION` before the remaining provenance branch at 34–58.

All eight predicates are unchanged from 0925: Additional units JSON and Additional amount use `IS DISTINCT FROM`, so NULL remains invalid; expense, mileage amount/units, travel, accommodation and other amounts retain their original `coalesce(...,0)<>0` behavior. No amount is converted, repaired or admitted. NULL/non-Daily/non-HOURS identity does not enter the new branch and still fails the original provenance check.

A static full-function comparison removed the new branch from 1129 and the eight original ancillary operands from 0925. The complete remaining function matched after whitespace normalization; a separate comparison matched all eight moved operands exactly after whitespace normalization. Thus existing current/version/authorisation, clock/break, Contract/context/date, policy, channel, precision, resolver and return-contract behavior is retained. Five Standard rows and all fifteen genuine available-rate cells, including NULLs, remain intact at 1129:126–137.

The unchanged current publisher `11102026_1001_banking_pay_next_daily_frozen_approval_context_v1.sql:94–96,220–226` calls the helper on the same authoritative financial row for NULL-Contract and contracted Daily paths. There is no catch/continue permitting a partial publication after this exception. The helper retains STABLE, SECURITY DEFINER, fixed search_path, postgres ownership and complete PUBLIC/anon/authenticated/service_role revocation at 1129:140–142. No new RPC, schema, trigger, producer or money owner is added. Registry `docs/bpay-next/final-definer-closures.json:12414–12429` selects the complete 1129 definition and retains the exact 0018/0925 lineage.

## Transport and existing presentation

`broker/src/index.js:93942–93961` recognizes only the exact closed sanitized code or exact SQLSTATE 23514 plus exact SQL message. A bare SQLSTATE, substring, ordinary provenance error, broken clock/break error or transport timeout does not match. The dedicated refusal is HTTP 409 with exact code, truthful plain copy and retryable:false. The original missing-rate parser remains unchanged.

The eight existing AUTHORISE seams are bulk-item flattening (94141), retained bulk summary/readback (94358–94359), atomic pair failure (94556–94560), finite business-error retry classification (95248), ordinary Authorise result failure (96019), generic mutation refusal (168136–168139), correction-pair failure (168386–168387), and final native RPC catch before existing unknown reconciliation (168648–168651). Each addition preserves the original native success/status/counter/affected-row and unknown-outcome branches. Bulk readback keeps each failed item's code and nonretryable state without converting authorized siblings into failures or claiming partial refusal as success. No new retry or data-repair action is added. The extra ManualWeek mapper seam has been removed; its original path is unchanged.

`FE/js/bpay-next-office.js:19–27` exports the small exact-code failure mapper and refuses success/ok:true objects. Actual existing consumers are `FE/js/main.js:5100–5107,5120–5123,5136–5139`, qualified against the captured request actor and the current SESSION actor. Single Authorise catches at 315343/315943 and existing bulk errors/results at 197106/197133/197148–197149 reach it. Original error metadata/cause and bulk status/counts remain preserved. The original Source qualification and missing-rate Contract navigation stay restricted to their own codes. This refusal offers no Contract-rate action or extra screen. `FE/index.html:3027,3053` advances the Office and main cache URLs.

## Evidence and limits

Current raw SHA-256:

| Source | SHA-256 |
| --- | --- |
| 0925 parent | `39c1a1f69fc5c9a5152aa08ca037e295e6e08b64bbd0651177556fa6fe52c99a` |
| 1129 successor | `8d9dfa131740391903d8aa68c6ce8a478f9a646b0f431ad43d30987a37b65911` |
| BE broker/src/index.js | `020ba5667127beb0abfafa8f57b2e78bb2801d62878858e55b608c1452c45639` |
| FE js/bpay-next-office.js | `e9030c493baa1709b6f5a20b41208a0931a7b634151e58e9cd78484ca32226ae` |
| FE js/main.js | `50289381a37c18a5c157e73e66e791c342023d9280b1cf5d86d07805e11cb3bd` |
| FE index.html | `052b1ea1f1062b9d468fd32dec59712e770404bf2221eacb086e79c403a54bb9` |
| final-definer-closures.json | `0601fecc155a9b59ac0c50c0232a41d876605876ba8fef4c3dff9855ba844301` |

Root reports source 12/12 before load, syntax/integrity 345/903, sole local PG17 compilation and sealed/coupled contract `3a0244519e91d9554be7d7499a29173a08786d78806f2ff203acbb55296c4fbe`, unchanged public membership and native verifier passes. Those checks are root evidence, not independently executed here. 1129 is now locally installed and immutable per root.

`docs/bpay-next/design/DY-08-future-joined-checklist.md` is appropriately AUTHORED / NOT_RUN, covering each damaged ancillary operand, genuine hours-only success, broken clock/break provenance, no partial publication, existing single/bulk copy and ordinary Weekly/separate expense preservation. No connected app acceptance or A4 screen acceptance is claimed. Owner A5 correction is a separate next boundary.
