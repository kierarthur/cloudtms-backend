# PS-11 server and scope source review

10 October 2026. **PASS — server/scope source only; no open findings.** Reviewed the complete PS-11 card, five checklist objects, BD037 scope, new0623 closure, final registry, existing SQL/nullable monetary contracts, actual Worker decoder invocation and new ordinary server source controls. Fresh re-read closes H01 below. No SQL, tests, runtime, fixtures, app, hosted or business actions were executed by this reviewer. Office presentation requires its separate frontend review.

## H01 closed: retain valid immutable NET request text

The initial classifier required exactly two fractional digits for applicable NET. That was too narrow for `NET_REQUEST`:0380 changes `entered_paye_net` from numeric(18,2) to `private.bpay_next_penny_amount`, which is an unconstrained numeric domain with exact penny validation, not a forced scale. The command transport accepts `65`, `65.0`, `65.00` and `0`. Current0390 `bpay_next_accept_case_net_request_v1` and0100 simple intake insert the numeric argument directly. Current2018 financial intake delegates to these true owners. History serializes the actual value with `::text`, preserving that numeric scale. A valid retained request can therefore emit `65`, `65.0` or `0`.

The fresh Worker and frontend helper mirrors now accept the existing finite nonnegative exact money-string shape: up to16 integer digits and optional1–2 fractional digits. They preserve the original string and zero rather than converting or normalizing it. Fresh NET_REQUEST controls explicitly cover `65`, `65.0` and `0`; BD037 records the actual0380 domain correction. H01 is closed. No SQL, financial owner or public body change was required.

## Correct closure and permissions

0623 is the narrow semantic successor of the current PS-07 root0614 and includes it without a duplicate physical function. Registry names one finalRoot0623 and retains original1206 as allowed finalPhysical, with both original1206 and0614 predecessors. EX-41 then CM-18 must merge from this chosen closure. Both inherited source and captured-history behavior remain unchanged, including the single transaction and schema reload.

The1206 source already returns actual immutable projection/request IDs, original transfer projection binding and complete frozen earnings/rates/deduction/recovery children. Retired accepted projections remain accessible; historical NET must use the original transfer's exact projection rather than the current worker or latest projection. No per-line NET/cash field, proportional calculation, new section, route, money owner or schema is added. Source comments accurately explain those facts without pretending to change SQL behavior.

Service-JWT and active-admin SQL checks, exact Candidate/Work/parent/context ancestry, fixed search path, UTC, audited owner/ACL and bounded live-keyset contract remain intact. The existing Worker authenticates admin, injects its own actor, reads one bounded page and preserves closed fields/errors. Its response validator now invokes the classifier after the existing exact scalar/parent/key checks for every actual row; it forwards retained data unchanged. Draft guarded readers and PAYE advice are separate and unchanged.

## Correct nullable semantics and source controls

Current WORKER/COMMAND_WORKER PAYE NULL is explicitly current NET-not-entered; Umbrella NULL is not applicable. Recorded PAYE_MANUAL/PAYE_IMPORT projection NET is required. Original0300 projection nullability explicitly permits Umbrella and CASE_PAYOUT NULL;0380 also permits CASE_PAYOUT NET_REQUEST NULL with its zero-gross shape. The decoder correctly distinguishes those intentional NULLs from a missing PAYE request and does not fabricate zero. CASE_PAYOUT cash is not a payroll NET. Reissue retains its original cash lineage without a new NET input.

RUN_LINES/RUN_WORKS/LINES/SHIFTS are intrinsically unallocated, independent of Candidate NET being known; the helper rejects added entered-NET/cash/allocation fields there. It contains no numeric conversion, summation, subtraction, division or inverse-money logic. Genuine100 earning less30 recovery remains earnings workings, never inferred70 PAYE cash.

D03 ordinary server controls cover zero/missing/not-applicable, the actual scale-preserving request text, invalid input kinds, numbers/negative values, line allocation fields and no-split source mutations with negative controls; they resolve the actual finalPhysical through the registry and preserve the52-section parity controls. These are bounded source/unit controls, not a universal SQL parser or business acceptance programme. D02 Office labels/retired exact-reference behavior are a separate frontend review. D04 explicitly keeps the joined65/two-Timesheet, NULL/zero, earnings-basis and whole-transfer/export/print clauses **NOT_RUN**, and preserves AUTH-PS for later Summary layout.

## Reviewed SHA256

| Source | SHA256 |
| --- | --- |
| `broker/src/bpay-next-history.js` | `a402fa5f72baf341235b0c35a513bed6cb47c5df17e3b7f461918e86a7a29cb9` |
| `tests/bpay-next-payment-history-caller.test.js` | `b0c1f69431e2a5753c866da283e67669b116f0c43df888c70a31d13fb1cbe17b` |
| `supabase/repeatable/10102026_0623_banking_pay_next_ps_c12_recorded_net_history_v1.sql` | `f53ca9dae5e297c628b5e1abcf491ceec1dd3bbea05e99aa2fea61fec108573e` |
| inherited0614 closure | `abecabcaa00883d1ebd2735c4f28df313a8a6161f4dce71272a57b4bd590d340` |
| original1206 physical | `d963c739f743e1526647ef101db7b9460dad88be3e2f56c2775c199aeddc2c73` |
| `docs/bpay-next/final-definer-closures.json` | `a0cf5d67e0e46568831bb54550680bcbac083af7b7263862dba9ac9af35d1bfd` |
| `docs/bpay-next/design/PS-11-SCOPE.md` | `7b2b4b501dd6b7a6c7ab93b18fc44bf27d92a1dacd78785a5a7393ae9b3ff0ea` |
| `docs/bpay-next/PS-11-JOINED-ACCEPTANCE.md` | `636ed0323587deca96300b1d65c39ae8dd863b6680b3de0d9bb93047627d2ece` |
| nullable request schema0380 | `9b750149eb86e7ddd6cff218783cbff2674c64abf3a117f20e66579d5f3d291e` |
