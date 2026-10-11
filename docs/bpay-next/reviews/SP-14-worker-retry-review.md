# SP-14 D04/D05 independent Worker retry source review

**Verdict: SOURCE PASS for D04 and the BD122-controlled D05 boundary. No product change is required in the reviewed Worker files.** Read on 11 October 2026 against the full owning card `05_SOURCE_AND_PROTECTED_PAY.md:484–514`, checklist ITEM/D01–D06 (`IMPLEMENTATION_CHECKLIST.md:3253–3273`) and `design/SP-14-owner-amendment.md:3–17`. Parent alone writes product. This reviewer performed static reads/comparisons/hashes and this review-note write only; no SQL/business calls, tests, fixtures, hosted operations, new tools or runtime/queue acceptance.

## Controlling scope and supersession

D04 retains the four existing transaction-rollback SQLSTATE mappings; arbitrary 55000 and uncertain transport cannot become safe known retries. Historical D05 requires queue redelivery of the original STEP nonce/epoch/cursor without a new CLAIM (card:494,504; checklist:3267). **That historical requirement is BLOCKED/superseded**, expressly at BD122 owner amendment:7, by the completed DR04 ID-only queue contract. Preserve direct exact STEP replay, native cursor recovery and fresh metadata CLAIM plus at most one STEP. Do not restore a queued token envelope. D06's old bespoke contention/hashing programme remains superseded; future real both-order busy/replay/timing/no-starvation acceptance is AUTHORED_NOT_RUN.

## D04: finite known rollback and uncertain outcome mappings

| Observed dependency result | Exact current result and source |
| --- | --- |
| Parsed `55P03`, `40001`, `40P01`, `57014` SQL rejection | `BPAY_NEXT_JOB_TRANSACTION_RETRY`, `retryable:true`, `outcome_unknown:false`, `recovery:RETRY_CALL` (`broker/src/bpay-next-job-step.js:391–393`). These represent the original transaction rollback classification; this adapter does not call CLAIM/STEP again. |
| Arbitrary `55000` message | Falls through the finite named 55000 cases to `BPAY_NEXT_JOB_NOT_ELIGIBLE`, false/false/NONE (:380–390). It is not promoted to transaction retry merely because it is a 55000 or contains a business-looking message. Existing terminal/module/lease/predecessor/claim/expiry/phase cases keep their separate named handling. |
| Attempted STEP timeout, malformed/oversized response or unrecognized transport/dependency failure | Remains uncertain, retryable, `RETRY_SAME_STEP`; an attempted CLAIM's corresponding recovery is `WAIT_FOR_LEASE_EXPIRY` (:334–364,395–397,418–424). The fixed deadline aborts and reports uncertainty, rather than declaring a rollback. |
| Recognized schema-cache/permission/input/conflict errors | Retain their exact existing finite mapping (:373–379); they are not part of the four rollback SQLSTATEs. |

The raw error envelope is parsed before mapping (:435–450), with the exact Source admission 55P03 observation affecting measurement only (:410–413,467–477). No raw SQL/provider message or token is returned in the sanitized retry result (:461–464). Request validation runs before transport; the one `Promise.race` performs one send and never redispatches (:415–458). Existing 2,048-byte request, 128 KiB response, 8-second deadline and 100-row page limits remain (`job-step.js:27–29,211,332–368`).

## D05: direct replay and native ID-only queue recovery

Direct STEP input remains closed to the exact original job/kind, nonce, owner epoch and phase-specific cursor/page tuple (`job-step.js:171–213`). The adapter copies those operands into a frozen args object without generating a new ID/token, incrementing a cursor or making another call. The fixed service-only transport serializes that same action/job/kind/args, forwards the signal and disables redirects; it contains no retry/discovery/CLAIM loop (`bpay-next-job-rpc.js:5–23`). Direct callers retaining an original uncertain STEP can therefore replay exactly that request; this source review does not certify an executed replay.

Queued recovery uses the completed DR04 behavior. The closed body is exactly native `{id,kind}`, refusing extra/non-value keys and nested token-bearing envelopes (`bpay-next-financial-queue.js:33–44`). Each delivery makes a fresh 120-second CLAIM, constructs one STEP from that newly returned nonce/epoch/native checkpoint, and invokes it once (:143–168; phase/cursor shaping :85–140). Busy/unknown outcomes retry the same platform identity (:59–68). A lost CLAIM cannot adopt an active stored nonce; terminal CLAIM is ACK. Nonterminal success must advance its native checkpoint and await publication of the same wake before ACK; an uncertain continuation send is not completion (:169–176). Position definite-failure handling retains its exact invocation-local closed evidence and requires `ACK_RECORDED` before ACK (:67–82).

Background builders emit only ID/kind and await their existing queue binding; financial dispatch uses that same consumer (`bpay-next-background.js:18–30,89–94`). Bounded READY/LEASE_EXPIRED recovery pages publish native job identities (:117–123); the Worker never reconstructs a financial cursor from a browser or queue envelope.

The current registered service remains `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` in `11102026_2041_banking_pay_next_case_pause_v1.sql` (registry:7526–7547). Source confirms active CLAIM refuses without disclosing a nonce (:1155–1162), newly claimed nonce/epoch are revalidated and durable cursor fields returned (:1295–1310), and exact owner STEP replay remains delegated (:1312–1315). Only after validating the actual owner's result (:1428–1469) does the service yield that exact nonterminal nonce/epoch to READY, clearing its own lease but retaining phase/cursors/effects (:1471–1484). Neither arbitrary active-lease adoption nor a separate unlock route was added.

## Saved-byte receipt and boundary

The four Worker files have no saved working-tree delta from current HEAD (`git diff` and scoped `git status` empty). Their existing bodies already satisfy the controlling D04/BD122-D05 scope.

| Reviewed saved source | SHA256 |
| --- | --- |
| `broker/src/bpay-next-job-step.js` | `D1A1349423FF00C7ED2713EA053E7D334C2015EEB250E8F2A33AA8AA42B07608` |
| `broker/src/bpay-next-background.js` | `0524712D1BF52209EEA3A7F8846491BAC4FC452B2EA593978C85162476049514` |
| `broker/src/bpay-next-job-rpc.js` | `65454073073B13C740DB5557A9A84A3CD6F64EF71F8493B2EF8A1FBFAE7F5B26` |
| `broker/src/bpay-next-financial-queue.js` | `19F08CBDD3BA40B9439B99CC274D7FA48B98DAED82BBD31207CA58C5C638CD68` |
| Current service `11102026_2041_banking_pay_next_case_pause_v1.sql` | `80675B1F77AF852977DA427D0CE0702BFB2A2DA6FAFCEA2A5E05B731810DE44B` |
| `design/SP-14-owner-amendment.md` | `5D25272F359B2DFB433BB2F83707B40B9B2A92CEF1DBFD750C99FA982F775065` |

No remaining essential source defect was found in this exact slice. This receipt does not pass the item, its SQL shift classification or any installed/joined financial behavior. Compile/catalogue, actual busy/rollback delivery, unknown-outcome replay, durable queued recovery, both-order contention and timing/no-starvation remain **NOT_RUN by this reviewer**.

