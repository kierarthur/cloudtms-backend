# PF-07 current investigation — sequence 111

Read-only findings on 11 October 2026. Full card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1805–1829`, every current ITEM/D01–D06 row (`IMPLEMENTATION_CHECKLIST.md:3279–3303`), master08:195/496 and current dependencies DR04/SP14/F11 were read. No product/checklist edits, SQL/business/hosted calls, fixtures, tests, new tools or deployment. Future application acceptance remains **AUTHORED_NOT_RUN**.

## Scope decision and exact remaining work

PF07 checks the already built durable command/enrollment/financial owners; the card explicitly forbids a second intake owner (:1811,1816). Its old Rules/Build/D01–D03 prose says version/action/lease/epoch/cursor envelopes and “not literal {id,kind}” (:1809,1817,1819; checklist:3289–3291). That wording conflicts with the completed DR04 ID-only contract, reaffirmed by BD122 SP14 amendment:7. Record this contradiction as **historical envelope wording superseded by approved DR04**, while preserving every behavioral requirement: durable DB authority, native order/replay, no financial message payload, one bounded step, awaited publication/ACK, unknown outcomes and lost-wake recovery. Do not reintroduce token queues, a metadata protocol, a new command/reader/table or a replacement intake.

Source census found **no required PF07 product delta in the named owners**. D01/D04 reuse the current exact intake rather than the old0270 baseline; D02/D03 reuse current queue/background and fixed metadata RPC. Remaining work is a current source/reuse receipt and completion note containing the nine-point verification below, plus the future joined checklist D05/D06 with honest NOT_RUN status. Compilation/catalogue/security receipts are parent-owned, not supplied by this investigation. PF08 owns future finite repeated-failure/needs-attention policy; PF07 must not implement it early.

## Actual current owners and reusable seams

| Named/necessary owner | Current source and signature |
| --- | --- |
| D01 public financial intake | `11102026_2041_banking_pay_next_case_pause_v1.sql:214–543`, `public.bpay_next_financial_intake_v1(uuid,text,jsonb)`; registry:7499–7522. Preserves typed existing action owners, actor/NEXT gates and original immutable accepted/replay receipts. Old0270 is provenance only, not a body to reinstall. |
| One original receipt sequence | `03102026_1404_banking_pay_next_command_intake_v1.sql:11–59`, `private.bpay_next_receive_command_v1(uuid,text)`: module SHARE, exact command-clock UPDATE lock, existing ID/kind/epoch returns original sequence, otherwise atomic clock+RECEIVED insert. |
| Ordered native enrollment | `11102026_2041_banking_pay_next_case_pause_v1.sql:545–970`, `private.bpay_next_enroll_one_v1()`: locks enrollment clock and only command N+1 (:562–570), one next sealed Candidate/job (:675–712), advances N only after complete enrolled membership. No worker financial-effect wait. Later FC09/FC10/snooze/discard branches remain their exact current owners. |
| Existing metadata enrollment RPC | `11102026_0541_banking_pay_next_enrollment_epoch_fence_v1.sql:4–55`, `public.bpay_next_enroll_service_v1()`: service-role/NEXT epoch, one enroller subtransaction; failure rolls back member/job/clock writes, persists only SQLSTATE/time on exact N+1, returns closed blocker; success clears it. No second metadata service is needed. |
| Existing native wake metadata RPC | `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql:38–88`, `public.bpay_next_job_wake_page_v1(text,integer)`: READY/BLOCKED and expired-LEASED index lanes, <=100, SKIP LOCKED, native wake-time rotation, exact ID/kind/command sequence. It never adopts or returns lease tokens. |
| D02 financial consumer | `broker/src/bpay-next-financial-queue.js:33–44,59–82,143–176`: closed native {id,kind}; fresh CLAIM, one STEP using its returned native checkpoint; terminal/done/durable-recorded failure ACK, otherwise awaited same-ID successor send before ACK. |
| D03 enrollment/recovery | `broker/src/bpay-next-background.js:18–40,48–99,106–153`: awaited ID-only sendBatch, enrollment-blocker retry, independently caught bounded READY/LEASE_EXPIRED/expiry/Workbench/due lanes. No recursive Candidate financial drain. |
| Fixed bounded RPC adapter | `broker/src/bpay-next-runtime-rpc.js:17–21,223–252,293–307,345–408`: ENROLL and WAKE_PAGE fixed targets, exact closed request/result, 1 KiB/32 KiB/8s/100 limits, parsed rollback versus transport uncertainty, no implicit retry. |
| Current financial service | `11102026_2041_banking_pay_next_case_pause_v1.sql:972–1576`, `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)`: original owners revalidate native leases/checkpoints; active claims do not expose token (:1155–1162), fresh returned token/cursor only (:1295–1310), validated own-lease yield only (:1428–1484). |
| Actual runtime integration | `broker/src/index.js:40994–41016,41050–41097,201728–201743`: fresh NEXT owner per invocation, fixed transports/actual queue binding, awaited one-message dispatch/minute recovery. Definite Position failure alone can ACK after native recording; logging otherwise returns RETRY. |

## Nine-point complete behavioral verification map

These are source observations and the exact future checks, **not executed application results**.

| Required clause (card:1818/checklist D06) | Existing source / retained future acceptance |
| --- | --- |
| 1. Monotonic accepted sequence | Receive1404:38–58 atomic native clock; rollback consumes no accepted sequence. Future concurrent accepts verify same native order. |
| 2. Original duplicate receipt | Receive1404:40–47 plus current typed intake delegates and accepted native receipts; replay never creates a new command identity. Future duplicate/lost-reply checks compare original receipt/effect. |
| 3. All N enrolled before N+1 | Enroll2041:567–570,675–712; partial PREPARE stays ENROLLING and clock does not advance. Future multi-Candidate enrollment observes complete N before N+1. |
| 4. Enrollment independent of effects | Enroll2041 inserts READY metadata job/member then advances completed enrollment, not jobDONE (:693–712; ordinary single owners :838–970). Candidate financial CLAIM keeps original earlier-job guard, separately from global enrollment order. |
| 5. Due scheduled action enrollment | Existing native action request/job and availability owners are retained; metadata discovery/CLAIM alone decide due eligibility, not browser clocks or queue fields (Wake0560:59–82; service2041:1274–1289). Future due scheduled action checks use those same owners. |
| 6. Visible recoverable failed enrollment | Enroll0541:28–40 retains exact error/time without advancing N. Runtime223–237 validates closed blocker, background:35/73–75 retries. Current private PROGRESS0814:2853/2887 exposes enrolment_failed; FE Office:1686 shows “Could not start every Candidate; continue processing or contact support.” Existing More/Continue processing calls WAKE (:1644,1695). No fake financial READY/error or new UI route. |
| 7. Metadata-only messages | Queue:33–44 rejects extra/live/lease/payload fields; background:18–30 and existing index send:41030 emit ID/kind. Native DB rows retain economic inputs/checkpoints. |
| 8. Busy not REVIEW | Job-step:391–393 maps 55P03/40001/40P01/57014 to known retryable RETRY_CALL; transport timeout remains unknown. SP14 independent worker receipt verifies same original direct STEP and approved ID-only queue recovery. No admission busy is fabricated as a successful financial REVIEW. |
| 9. No retained connection between steps | Per-invocation dependency factory and raw HTTP transports only; no DB client/transaction handle crosses sends. Queue executes one STEP then returns; continuation/minute opens its own original RPC. Future disconnected/duplicate/send-failure journey proves durable progress and no duplicate effect. |

Before/after-click behavior remains Rule B and the original accepted descriptor/expected revisions/captured Candidate/query inputs. Current SP14 shift reasons are retained at original Create, not later live rechecked; later updates affect the next Workbench. No PF07 producer/cut/resolver change is required. Case money remains live under original money locks. Browser-close progress/reopen is the built DR06 retained journal/qualified run seam, not a queue payload or new PF07 navigation store.

## Durable outcome and explicit uncertainty

ENROLL/EXPIRY/Workbench ACK occurs only after their definite native metadata outcome and required awaited sends (background:32–40,57–87). Financial terminal CLAIM/done STEP ACK is definite; a successful nonterminal page must advance and publish before ACK (queue:156–176). Unknown CLAIM/STEP/send retries native identity; active leases wait for expiry, then fresh CLAIM reads committed cursors. Direct exact STEP replay remains separately supported. READY/LEASE_EXPIRED minute discovery rotates native wake time and recovers missed notifications; it neither reprices nor adopts an active lease. Do not claim timing/fairness or provider outcomes from static source.

## Saved-source checkpoint

| File | SHA256 |
| --- | --- |
| current intake/enroller/service2041 | `80675B1F77AF852977DA427D0CE0702BFB2A2DA6FAFCEA2A5E05B731810DE44B` |
| enrollment epoch0541 | `2854FD9F5F51D9654193C59D74258620AFA8BDF020F45DFA8BFD78C3F47FC4B8` |
| queue recovery0560 | `03DAF223A8B572B37605F1DDC15FB777A3A543FF89140B8CFF40238EC9A139FA` |
| background.js | `0524712D1BF52209EEA3A7F8846491BAC4FC452B2EA593978C85162476049514` |
| financial-queue.js | `19F08CBDD3BA40B9439B99CC274D7FA48B98DAED82BBD31207CA58C5C638CD68` |
| runtime-rpc.js | `55E682EB8421314AB603C13D5913D365CFE90DCB0399A83167D01DD7A7AA232C` |
| job-step.js | `D1A1349423FF00C7ED2713EA053E7D334C2015EEB250E8F2A33AA8AA42B07608` |

No item completion, installation, runtime, timing, queue/Worker/minute journey or joined acceptance is inferred. No bespoke fixture/hash proof programme, old Banking parity work, new producer, money owner or future PF08 behavior is proposed.
