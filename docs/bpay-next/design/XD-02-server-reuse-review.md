# XD-02 existing server owner reuse review

Independent saved-source review, 13 October 2026, seq148/BD180. Reviewed existing1827 private/public binder and claimed-origin guards/schema, current1536 original fourteen-argument Case-create delegation and apply owner, preparation authority, Worker fixed RPC/input/receipt/error handling and existing accepted-command wake. Current `XD-02-build.md`, journal investigation and prior XD01/02 investigation supply the narrow caller boundary. This review changes documentation only; no SQL/database/RPC/business/network call, test, runtime invocation, product/checklist/BUILD_LOG edit or Git operation was performed.

**SOURCE PASS for reuse. No server or Worker product change is required or justified by the reviewed XD02 caller integration.** No concrete mismatch was found between the existing binder, current original Case-create entry and closed Worker receipt contract. This is an admission/reuse verdict, not proof of joined adoption, legacy-use/concurrency/consent behavior, current hosted installation, immediate Case materialisation, NET allocation/payment or whole-card acceptance. Root's fresh sole-local-PG17 original-owner capture remains separate evidence.

## Exact owners and authority

The saved final registry maps both `private.bpay_next_bind_stored_credit_v1(uuid,uuid,uuid)` and its public wrapper, plus `private.bpay_next_stored_credit_legacy_guard_v1()`, to `04102026_1827_banking_pay_next_candidate_destination_group_v1.sql`. The original fourteen-argument `private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text)` and apply `(uuid,uuid,bigint)` map to `11102026_1536_banking_pay_next_case_create_intake_v1.sql`. Preparation authority remains `04102026_0575_banking_pay_next_preparation_service_v1.sql:7–22`; no competing final or new owner is proposed.

The public binder's argument order at1827:77–84 is actor, command, legacy Case; its private delegate at8–10 is command, legacy Case, actor. Both call the original preparation authority. That authority requires service JWT, non-null active admin and current NEXT owner, with share locks. Public binder is SECURITY DEFINER and service-only; private binder is INVOKER and revoked from PUBLIC/anon/authenticated/service_role. Logical ownerpostgres mapping and unchanged private helper/guard revokes remain at549–577. Origin table is private and revoked from all four non-owner roles by migration1827:136–145. Full captured bank facts never enter the public receipt.

The binder derives Candidate from the exact legacy Case; Candidate is not a caller-supplied replacement operand. The existing admin authority admits native Cases across this agency. The original Candidate-scoped masked-row qualification and retained client Candidate context are still required for the Office action; the closed server contract must not be widened with a guessed Candidate/amount/bank substitute.

## Genuine retained source and atomic replay

At1827:18–28 authority and null-ID checks precede the original command advisory lock. An existing immutable origin is read before fresh eligibility. Same command with a different legacy Case or actor raises `BPAY_NEXT_STORED_CREDIT_REPLAY_CONFLICT`; an exact replay uses its original Candidate/principal and the same fixed CREDIT/MANUAL_CREDIT/NON_TAXABLE/reason tuple through the original Case-create owner. Current channel/eligibility changes do not replace accepted origin facts. The current Case-create owner still requires active authority/current module epoch and exact original receipt input, so replay does not grant a new epoch or foreign actor.

Fresh admission at31–63 takes the existing Candidate share lock and exact legacy Case, bank and component update locks. It requires current active PAYE Candidate; original MANUAL_CREDIT_ADJUSTMENT/NON_TAXABLE/ONE_OFF_SPECIFIED_BANK_ACCOUNT; required bank; ACTIVE/PENDING with no payout batch/transfer; positive original/outstanding equality; uncleared/unwritten-off; exactly one component from a first-two ambiguity probe; matching Candidate and original UMBRELLA source; REIMBURSEMENT_GROSS_FIXED/CASE_TOTAL/TOTAL and genuine case source-family identity; no linked Timesheet/closed component; original source amount/remaining equality; exact source-basis family/tax/routing; no RESERVED/COMMITTED legacy reservation; exact COMPONENT_CREATED/MANUAL_CREDIT_ADJUSTMENT_CREATE event; bank hash correspondence and finite creation/bank timestamps.

No caller money, tax switch, beneficiary, account, bank hash/version or consent surrogate participates. The actual creator event and original bank facts are checked; this review does not invent another consent token or claim an unexecuted joined consent test passed. Existing `_bank_hash` is the retained pure input-string hash owner already reviewed for XD01.

At64–74 the original native remaining principal is accepted and the origin is inserted in the same SQL transaction, without an exception swallowing either failure. Migration1827:8–27 makes command_id the Case-create-request FK/PK, legacy Case and component identities individually unique, and command/Candidate an exact command-member FK. The private penny/finite timestamp/hash/name/account constraints retain the original source. A concurrent or later second command hitting either unique origin identity cannot retain a second accepted liability after the transaction fails. No alternate command UUID or new liability creator is needed.

## Claimed-origin fencing

The existing guard1827:88–101 rejects a claimed legacy origin by exact legacy Case identity. For non-advance UPDATE it also checks the old identity, preventing reassignment away from a claimed Case. Its four attachments at102–113 cover pay_advances UPDATE/DELETE; finance components UPDATE/DELETE; one-off bank details UPDATE/DELETE; and advance reservations INSERT/UPDATE/DELETE. The guard does not depend on current NEXT activation, so a claimed origin is not released back for legacy use by an owner rollback. It is an exact existing identity fence, not a replacement for the other installed legacy-writer fence.

Origin immutability remains attached at538–546 through the existing `bpay_next_effect_immutable_v1()` callback. This review does not broaden trigger events, add a new table/guard or waive any existing writer census. Actual future creator/legacy-use/concurrency negatives remain authored NOT_RUN; source reading does not replace them.

## Current Case-create delegation and meaning of acceptance

Binder1827 invokes the original fourteen-argument signature. Current1536:273–287 preserves that signature and directly delegates all original operands to the seventeen-argument owner at147–271, adding null note/original-liability-reference/preview only. There is no new required preview or manual input for this internal retained-source path. Terms at5–99 reject invalid/nonfinite/subpenny principal and incompatible families; CREDIT/MANUAL_CREDIT/NON_TAXABLE is valid and all repayment-rule inputs are null. The current owner therefore receives the native principal unchanged and preserves the original fixed reason.

Current1536:183–218 keeps NEXT epoch, active admin, exact Candidate and original command/input replay qualification. Fresh admission at220–269 captures the current Candidate source channel and valuation policy, receives the ordered CASE_CREATE command, records exactly one command member and the durable request, seals the command, and returns exactly `{case_id,command_id,sequence,phase,replay}`. Both IDs equal the original command; sequence is positive bigint text; phase is ACCEPTED_PENDING_CASE. It does not claim Case materialisation or payment. Original-Umbrella origin remains separately captured while the newly created Case uses the accepted current PAYE channel.

Existing apply1536:289–370 requires exact CASE_CREATE job/Candidate/current module epoch and owned live lease. It materialises the accepted Case/rule/CREDIT component/period/event, advances native financial-view revision and workbench Case read projection, completes the job/command and returns CASE_CREATED. No new payout/transfer/provider command is added to make the Office receipt visible. Bound origin may exclude the old stored row before this queued apply finishes; absence from the stored list is insufficient evidence that the Case is shown. Existing exact CASE_COMPONENTS Candidate/Case readback and the qualified affected parent acknowledgement remain the caller completion seam.

## Existing Worker contract and uncertainty

`broker/src/bpay-next-command.js:65/86` already has only BIND_STORED_CREDIT `{command_id,legacy_case_id}` and the five native receipt fields. `commandCall` at235–252 requires exact keys and UUIDs and applies this action's2048-byte bound. No actor/money/bank/route fragment is admitted. `handleBpayNextCommand` at437–508 authenticates the existing active Office admin, supplies authenticated user.id, sets attempted only before the original fixed RPC, bounds/qualifies its response and never automatically retries.

`broker/src/bpay-next-command-rpc.js:7–35` selects only the fixed public bind endpoint for this action, independently requires exactly two UUID args plus an authenticated actor UUID, sends the native argument order through named keys, forwards AbortSignal and refuses redirects. `commandResult` at341–359 requires the complete five-field shape, ACCEPTED_PENDING_CASE, positive bigint-text sequence, exact command ID and case_id=command_id. The result contains no captured full bank details or calculated money.

Current definite SQL refusals map through the existing finite conflict/not-eligible/scope/authority/module errors at399–429. Abort/unknown dependency, invalid/missing success response and malformed/oversized receipt preserve outcome_unknown once attempted; no arbitrary database diagnostic is surfaced. A later attempt's refusal does not prove an earlier unknown attempt failed; the already-reviewed caller's explicit same-byte/UUID retry and retained accepted state remain required. Nothing here authorises a replacement command or re-adoption after acceptance.

The actual route at `broker/src/index.js:199198–199224` uses the unchanged handler/RPC and, after accepted response, sends the existing finite ENROLL wake. Wake failure is caught and leaves the accepted response intact for the existing recovery scan; it does not convert durable admission into a refusal or drain work within the caller. Original CASE_CREATE enrollment/apply remains the only completion route. XD02 needs no new Worker kind, job, command, namespace or scheduler.

## Evidence checkpoint and limits

The saved current contract independently contains the three exact binder/guard definitions with full normalized hashes: private binder `04198cf56a11a3c0ecf61b12df3337d5ef92be588f6d652251c349a318a3d8c4`; public binder `4dc72788bb2f41384e0ff1055642fc695742b8657f265c5d57725147d49e581b`; guard `54ed6be1a6d91d559eec3b65d2ffe65d8f41e5779ff6851d9d6ebcc5c557b37a`. It preserves owner-only private ACLs and owner+service public ACL, expected search_path and invoker/definer posture. This is consumed saved generated-schema evidence, not a new reviewer database capture. Root's fresh original-owner catalogue must be reported separately when saved.

| Saved source | Canonical LF SHA-256 |
|---|---|
| Existing1827 repeatable | `33891038c2865684c091f7bd3df4ce188bb42e026129163216e3bc341feb198c` |
| Existing1827 migration | `af9849f6e83784b0064ee58179aabc740d396764c3639ef7132967f394c9512d` |
| Current1536 Case-create owner | `853ddce45455defed9f715a2593267794f3b224e469a9eaccde2aeca6e44a470` |
| Original0575 authority | `4c651558f56b85c29cf10f21a1d3168ce259f75a66001a24a7f53dbc76216a87` |
| Existing Worker command | `2ad2580b7fbf86fd53434e0d54ae48a28125607fe7508d6073897edbf5819bf7` |
| Existing fixed command RPC | `a1ee88cfc37deab4e20fbd109d5f4bd70740b04bdea0004e6b505c3e3a383bab` |
| Final-definer registry | `f01b2decad4d006e84e457f1b774c21d2668b61ede72c2e5336cb36f44ba52b9` |

The reuse verdict requires preserving these owners and the caller's original actor/Candidate/epoch/request/receipt containment. Whole NET/capacity/allocation and later stored-credit settlement/advice owners remain their assigned subsequent cards. Future joined checks and A4/runtime receipts are neither executed nor inferred from this SOURCE PASS.

## Root-captured reuse catalogue closure

**SOURCE PASS retained after the separately saved root catalogue was read.** `XD-02-native-reuse-catalogue.json`, saved SHA-256 `9dac95b5a487bec45bdbd9a91d9f8f8f22329a90b2f5bcbba431ccf426852034`, declares sole-local-PG17 catalogue capture with no business rows or routine execution. Its original1827 file hash matches the source checkpoint above. Independent extraction/hash comparison of all three complete saved dollar bodies agrees with captured MD5s: private binder `4f254481132cf3769091faa2b2fca660`, public binder `08a9edba7db9c5d98c17b90acd0669fb`, private guard `51a8a4c66c0a6a9e39bc6877487a5167`.

All three captured installed-definition SHA-256 pins equal the saved current-contract identities above. Captured current local owner/configuration and invoker/definer/volatile posture correspond to source, with private binder/guard owner-only execution, public binder owner+service execution, and no PUBLIC/anon/authenticated execution. This closes the supplied native-owner/source correspondence for reuse. The reviewer made no fresh PostgreSQL call and does not turn catalogue matching into executed adoption, legacy-use, replay, concurrency, payment or hosted-install acceptance.
