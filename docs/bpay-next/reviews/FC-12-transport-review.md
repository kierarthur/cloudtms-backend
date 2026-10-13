# FC-12 independent transport source review

12 October 2026. **Transport SOURCE PASS — T12-01/T12-02 closed in the final override below.** Read-only review of the current Worker/Office transport against BD147, complete FC-12 card/checklist and1536 interfaces. Root alone edits product. No product/checklist edits, business SQL/RPC, data, tests, fixtures, browser/rendering, network or hosted actions were performed. The native screen is not reviewed/built by this receipt; joined acceptance remains NOT_RUN.

## Required corrections

**T12-01 — initial defaults Preview is rejected.** `broker/src/bpay-next-preparation.js:780` unconditionally parses the `input` query key and throws when its URL value is missing, although the new schema declares `restructureInput?`. Office1126 omits null parameters, so normal CASE_RESTRUCTURE_PREVIEW `{case_id,expected_case_revision,input:null}` becomes a400 before the SQL read-only defaults arm. Preserve only this new kind's missing input as typed NULL; parse present JSON normally and retain CASE_PREVIEW's required input behavior. This is a caller-contract mismatch, not a reason to add another endpoint or wrapper.

**T12-02 — Office omits receipt target qualification.** Worker preparation1034 requires the restructure receipt to match the requested command plus its row's Case/Candidate, and requires NULL when no receipt was requested. Office1274–1279 does the analogous checks only for pause/edit. A structurally valid different-command/Case/Candidate restructure receipt, or requested NULL receipt, passes its Office row decoder. Add the same finite check using the existing `same` helper. Later newer current terms must not invalidate a genuine older original terminal receipt; retain immutable original request/receipt identities.

## Checked contract and narrow scope

The FC12 block appended to Worker `bpay-next-case-contract.js` and FE `bpay-next-case-preview.js` is text-identical after line-ending normalization. It closes every input/context/terms/token/preview/receipt key, rejects negative/unknown/noncanonical numeric and invalid date shapes, preserves actual NULL floor, requires exactly one schedule mode, Monday bounds, original Case/version/input token equality, entered-total/basis and server echo consistency, and terminal applied revision exactly expected+1. Penny/date comparisons validate native output only; the browser derives no money or schedule.

Command request/response maps use only CASE_RESTRUCTURE and exact accepted phase, Candidate/Case/original command/rule/version bindings;32KiB command,8KiB input and16KiB token bounds remain finite. SQL1536:65–128 emits matching native before/after/default/context/protection,186–210 matching native phase/issue/date/immutable receipt. CASE_COMPONENTS adds only nullable `restructure_receipt` and an optional exact request UUID. SQL2320–2328 qualifies that request and caps the final page after annotation. The existing component reader and original pause/edit fields remain retained.

The four literal RPC/runtime/queue allowlists add only the named action/job kind. Job STEP is a single terminal CASE_RESTRUCTURE operation, admits only CASE_RESTRUCTURED/BLOCKED, and validates the same closed native receipt. Existing CLAIM/lease/owner and DONE/replay paths retain their original adapter behavior. Audit source enums add only actual RESTRUCTURE_REQUEST/RESTRUCTURE_OUTCOME, mirrored in Office; no invented Case-money event, new financial formula, route, producer or reauthentication is introduced. Remaining job/page bounds and behavior are being read before final source closure.

## Source checkpoint

| Source | SHA-256 |
|---|---|
| BE `broker/src/bpay-next-case-contract.js` | `01b0cceb0790505c05d1fecff6ebc9f5036d0d8e968214d1878067ae876d1ef2` |
| BE `broker/src/bpay-next-command.js` | `cd60d1de208b26e3769d3874dfac2cbbd6caef7e09dd0b7403478589f90cb463` |
| BE `broker/src/bpay-next-preparation.js` | `918192365a7346d0ecc387d60b156291564fbbdef2b66d8e674dccd93ad5c2b1` |
| BE `broker/src/bpay-next-job-step.js` | `99241e7f25aac9cfddd2772b795ed5e068527a8b2afaae3a5a25d602577eae3b` |
| BE `broker/src/bpay-next-command-rpc.js` | `42634dc189ba2f137451e8c52d7146fd39cbc9ad39ccc06230d765ab006ddf6d` |
| BE `broker/src/bpay-next-job-rpc.js` | `a13ab48ba4dacd136be34f9a5e7589ad8407f6fda63ac12d63378bf6708b4998` |
| BE `broker/src/bpay-next-runtime-rpc.js` | `96cb0801ac4907f8c5fdf23986084a0ef071ff6a25663e031b041fe2560d64d0` |
| BE `broker/src/bpay-next-financial-queue.js` | `d96e2479ea432641c5a2054040feb076b5882268e00775d40f14340345ded7f3` |
| BE `broker/src/bpay-next-case-audit-contract.js` | `7bf3adeae214d26f435cb93cbbaf5c842f493214369e1ce30e9db949b92fd399` |
| FE `js/bpay-next-case-preview.js` | `ff56bd7d262d2cebb79dc10d93f3b7f31e432abbd249abd57bdebe883bc464d1` |
| FE `js/bpay-next-office.js` | `0381f5dffa0058e7b6503f0c8b51b999ce493273fa479735166078fca5c75116` |
| FE `js/bpay-next-case-audit-contract.js` | `3f4e6b5fdc3331eeee9be7e719d912e58ec1f75a59457ff9d7cddb3dcd079ee2` |
| SQL `12102026_1536_banking_pay_next_case_restructure_v1.sql` interfaces | `85d5842a492e39fd9cf411e287bdace6ca3f59f21c3bd715f5ba4fbd8b87122c` |
| `design/FC-12.md` | `b10e7b2d6d47670ce34497361d0cef78339b9a81a25db9da77a2b832b4186713` |

This is an interim source checkpoint; root is still editing. Existing preparation-file line-ending churn is packaging, not justification to alter unrelated behavior. Final saved hashes/closure follow the two corrections. No installed/app-tested claim follows from this review.

## Final transport override — SOURCE PASS

T12-01 is closed at Worker780: only a missing CASE_RESTRUCTURE_PREVIEW input remains typed NULL; present JSON and the original required CASE_PREVIEW input still follow their prior parse/validation path. T12-02 is closed at Office1277–1278: an annotated receipt must match the requested command and row Case/Candidate, must be present when requested and must be NULL otherwise. This is equivalent to Worker1034, preserving genuine older terminal receipts when later current terms have advanced.

The remaining job call/result paths, singleton terminal STEP, ID-only queue admission, named action error mapping,8s deadline, bounded streamed128KiB read/final response and original awaited mutation handling have now been read. No further essential transport defect was identified. Shared FC12 validator parity remains exact normalized text. Native screen/caller lifecycle source review is separate and underway; A4 and all joined business acceptance remain NOT_RUN.

Final changed hashes supersede the initial checkpoint: BE `broker/src/bpay-next-preparation.js` `8715ddba120198f9fc6326e46c4d3cc8e0c0124b294bd58fc3dd79d76be14db3`; FE `js/bpay-next-office.js` `456a48e11e6efd161fdeb3adf4cb356356dd42b1d3e95949a7da270577d11c91`. Other transport file hashes in the table remain current. Current SQL interface checkpoint is `14364da406573b3031c30e9f2f74148862109eb2c03613e0035b3897b6f0665b`; fresh native Preview/admission/receipt/page/STEP interfaces still match the reviewed closed schemas. That interface check is not a full SQL-owner source verdict.
