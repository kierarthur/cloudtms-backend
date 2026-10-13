# FC-11 Worker / Office transport source review

12 October 2026. **SOURCE PASS for the named Worker and shared Office transport seams.** T01 is closed in fresh saved bytes; the separately reported T02 journal predicate is also corrected. Read-only saved-source review; Root alone edits product. No tests, SQL execution, business/hosted calls, auth submission, queue action, fixtures or joined acceptance were performed. The independent native UI review remains with its assigned reviewer.

## Current source checkpoint

| Source | SHA-256 |
|---|---|
| BE `broker/src/bpay-next-command.js` | `c228ebf921f5464f91af06b38d0252e222d6b4bb34eee20daea4b157e4f17e68` |
| BE `broker/src/bpay-next-job-step.js` | `a9171adedfb11c2a9fc2b4e78090104426fb1ebd253eecdb8e53c6e21d5366b7` |
| FE `js/bpay-next-office.js` | `12372158e0c1fcfbd2e3d592f3328a5424cbd97203406fb21d8eaab0b8ab9cce` |

Authority: FC-11 card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1238–1266`, all owning checklist rows, BD146 `docs/bpay-next/design/FC-11.md`, the five-owner SQL review and existing EX-01 proof contract. This review concerns the named Worker and shared Office seams; the new per-component window/screens remain a separate delivery boundary.

## Closed finding T01 — native debt preview money domain

Historical finding at Office SHA `adc9b20d…7b242eb3`: `js/bpay-next-office.js:1054` qualified WRITE_OFF preview `total_amount` as generic `cash`, whose parser at 691 permits only sixteen integer digits. The Worker preview instead uses native `caseMoney` (`broker/src/bpay-next-command.js:142,292`). Existing Case input legitimately permits up to 1024 bytes of unsigned decimal money (`js/bpay-next-case-preview.js:8,13`), and the new WRITE_OFF input/receipt has a matching `writeOffMoney` field (`bpay-next-office.js:601–604`). A valid larger native debt was rejected before authentication in Office.

**Closed in the current Office hash above:** line 1054 now chooses `writeOffMoney` for WRITE_OFF only, retaining PAYE policy NULL and the existing cash domain for other protected actions. No numeric conversion or another amount owner was introduced. The source-only failure is closed.

## Source checks already closed

The BE changes are exactly two command lines and one job-result predicate in the current diff. WRITE_OFF requires SOURCE_DEBT in the existing finite preview decoder (command 285–297). It removes only the accidental equality between Case and component UUIDs (command 362–374; job-step 243–248). The command still binds the actual returned component and command UUID to the request (336–344), validates exact scope, nullable REQUESTED facts, positive successful delta, exact event command UUID and AMOUNT/ALL result invariants. The job result remains closed to CASE_WRITE_OFF's native job identity, terminal phase, finite six issues and protected-versus-remaining relation. No ordinary Position/other-job parser is weakened.

Office's new closed WRITE_OFF request and receipt schemas (241,268) match the native action/result shape. AMOUNT alone adds its positive decimal input (747); reason remains nonblank/NUL-free and UTF-8 bounded (603). Receipt qualification binds native component, scope and command separately, then verifies REQUESTED NULLs or the same terminal phase/money/event/issue invariants (773–784). It never assumes the component UUID equals its parent Case UUID. Pennies are used only for exact contract comparisons, not balance or capacity calculation.

The action uses the existing financial command route (734,1060–1065) and existing finite EX-01 reauthentication flow (735,1049–1058,1755–1847). The SOURCE_DEBT amount is presented as “Amount to write off” (1773), with the same server preview scope and original intent in `/auth/reauth/start` (1821). The helper validates original actor/parent frame/owner context before and after async work, clears credentials and aborts on invalidation, and returns a token only to the original parent (1763–1767,1792–1818,1842–1846). No browser financial STEP is added.

Worker proof recording and command consumption remain unchanged: active admin/session authentication (428–431), finite action token verification, proof UUID and consumed original command qualification (443–463), then the same financial RPC (467–477). SQL rechecks exact original native scope before consume/accept as documented in `FC-11-sql-review.md`; no client amount is certified independently.

The shared controller retains exact saved mutation bytes before dispatch, restores them under the existing actor namespace and byte limit, and deliberately retries the same request after an unknown result (1558–1584,1612–1621,1631–1643). Adding WRITE_OFF to the closed schemas does not rewrite old journal bytes or automatically retry a mutation. New protected dispatch requires the existing reauth token; only the established already-issued CSV exception remains tokenless.

## Boundary

No essential finding remains in the named shared transport slice at the current hashes. This review does not certify the new thin window's full journal/readback/actions, A4 pixels, root compile/catalogue checks, hosted runtime or whole FC-11 ITEM. All business/joined acceptance remains NOT_RUN.

## Additional native-window finding reported while tracing the caller

Historical T02 was outside the three-file shared transport verdict: `js/bpay-next-write-off.js:100–105` caught an initial typed SERVER_REFUSED/AUTH_REQUIRED response with `unknown=false` as an unknown result and retained the journal. Its `fresh()` at 87 required a terminal record, so a definitively unaccepted request had no deliberate fresh-request exit. The established shared controller distinguishes an initial definitive refusal from a response after an earlier unknown outcome (`bpay-next-office.js:1612–1621`).

The current narrow correction was inspected at `js/bpay-next-write-off.js` SHA-256 `e8d6621fc1192723f8d0505cf2f7bb2ac876782c07bbc24727dce45b2762e0f0`: initial submit stores the original bytes with `unknown=false` and calls dispatch only after persistence succeeds (93–98); dispatch captures the prior-unknown/accepted flag and persists uncertainty before calling the mutation (100–103). Only an initial typed definitive refusal with no prior uncertainty retires that journal (105). Any later refusal after uncertainty keeps the same original bytes. Restore conservatively treats an unaccepted retained request as uncertain (36). **T02's reported predicate is closed**; full native-window behavior and screens remain for the separate UI review.
