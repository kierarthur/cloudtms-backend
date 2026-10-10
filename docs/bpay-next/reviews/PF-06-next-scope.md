# PF-06 sequence 21 — current source inventory

10 October 2026. Independent read-only source investigation. No SQL, runtime, tests, fixtures, hosted reads or product changes were performed. This note records the next build scope, not installation or future in-app verification.

## Authority and exact objects

`08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:105` identifies sequence **21 as PF-06, Measurement hooks (step identities only)**; sequence 22 is PF-04. The full PF-06 card is `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1782–1803`. Dependencies are **F-05, F-06 and F-11**; design is not required. The current implementation checklist at 513–534 has exactly three rows:

| Row | Existing target | Remaining scope |
| --- | --- | --- |
| PF-06-D01 | MIGRATION: job/step clock and row-count completeness | Census first; add only genuinely missing columns to existing rows. Preserve existing counts and operation clocks. |
| PF-06-D02 | QUEUE-CRON: `broker/src/index.js` structured step measurement hooks | One bounded structured line `{job_id,kind,step,rows,bytes,ms}` at the actual existing step boundary; no sensitive values. |
| PF-06-D03 | Future joined in-app TEST checklist | Retain NOT_RUN: Draft timings; small/large-run FIFO enrollment delay; cancellation, return and PAYE update completion timings; Pay Query contention/busy step identity; joined DB/Worker/browser latency. |

The card expressly excludes new harnesses (Part C/B11), fixture/hash proof programmes and visible UI. It does not name a new measurement ledger, endpoint or protocol. BD019/BD020 Rule B remains the accepted F-25 scope; parked producer/resolver work is not a dependency to reopen.

## Confirmed database census

The sealed `supabase/release/current-contract.json` **relations** inventory confirms that `private.bpay_next_job` has `id uuid`, `job_kind text`, `phase text`, status, command/candidate identity, `applied_line_count bigint`, `attempt_count integer`, `available_at_utc timestamptz`, `queue_wake_after_utc timestamptz`, lease deadline/nonce and owner epoch. It has **no created_at_utc, started_at_utc or finished_at_utc**. No `bpay_next` relation named step, attempt or metric exists in that inventory.

Reuse rather than duplicate:

- `private.bpay_next_command`: immutable `created_at_utc timestamptz`, `sealed_at_utc timestamptz`, expected/enrolled member counts and agency sequence. Together with an actual job-created timestamp, these can expose the existing per-command/per-member enrollment delay without another enrollment mechanism.
- `private.bpay_next_cancel_request`: started/finished UTC, expected/released line counts and cursor. `private.bpay_next_case_cancel_binding` already has created/started/finished UTC, checkpoint and expected/released Work/case-hold counts.
- `private.bpay_next_preparation_expiry_worker`: created/started/finished UTC, checkpoint, expected/released Work/case-hold counts. Its request also has finished UTC and expected/enrolled/completed Candidate counts.
- Transfer/member, outcome, destination, case allocation/build and run-worker owners already retain their specific progress counts. `private.bpay_next_return_cash.created_at_utc` is return-row creation, not generic job completion. The run-worker has capture/allocation/resolution/effect counts but lacks general lifecycle clocks.

`available_at_utc` and `queue_wake_after_utc` are scheduling times; `lease_until_utc` is a deadline; `attempt_count` counts claims; `applied_line_count` is the existing business counter. None should be relabelled as immutable job creation, step elapsed time or universal rows visited. Historical missing times cannot honestly be reconstructed from those fields. Generic created/first-started/terminal-finished columns are the positively evidenced gap; wire rows/counts require the per-kind distinctions below before choosing any additional count column.

Actual writer seams already exist: the FIFO `private.bpay_next_enroll_one_v1()` in `03102026_1430_banking_pay_next_position_writer_v1.sql:106` creates jobs for the next command/member; the expiry enroller is `04102026_0510`; SERVICE dispatch is `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` in `04102026_0280:12`. Current PREPARE CLAIM is the designated `10102026_0151` Rule B successor. If lifecycle columns are added, populate them in these actual existing owners/terminal transitions, preserving same-transaction rollback, leases, replay and owner order. A Worker success log is not proof that a lost-reply transaction committed.

## Existing Worker hooks and narrow reuse

`broker/src/index.js:40990–41003` builds the shared F-11 dependencies; its existing warning logs only sanitized refusal code/recovery and is not the PF-06 measurement hook. Queue dispatch at 41080 and minute recovery at 201413 already converge on those dependencies. Preserve the registered `waitUntil` enrollment nudge, one-enroll behavior and fresh NEXT fences.

The real one-job boundary is `bpay-next-financial-queue.js:185` → `stepBpayNextFinancialJob` (`bpay-next-job-step.js:361–395`). The queue already awaits durable continuation publication before ACK; unknown outcome retries the exact admitted step. Instrument this existing call path through a small optional measurement dependency passed from index, without another RPC, new job, inline drain or change to ACK/retry/error semantics. Logging failure must not determine business success or queue disposition.

Use the validated input action/phase to identify the attempted step; a returned phase can be the next phase and must not mislabel the work just measured. Keep job UUID and finite kind/step only. Do not log Candidate/Work/Timesheet identity, money, cursor, lease nonce, request/response bodies, headers or credentials.

Actual count availability in the strict existing receipt schemas (`bpay-next-job-step.js:60–90`): TRANSFER_BUILD, CSV_SETTLEMENT, INTERNAL_SETTLEMENT, SIMPLE_CANCEL, PREPARATION_EXPIRY and POSITION_APPLY expose bounded decimal-text `rows_visited`; PREPARE exposes nullable `rows_visited`; PAYE case FINANCE_ALLOCATE has its separate counted receipt. Ordinary PAYE_NET_ENTRY, CSV_RETURN, CASH_REISSUE, CASE_CREATE, CASE_WRITE_OFF and REISSUE_CANCEL do **not** all expose a generic visited-row count. CLAIM also has no such count. Preserve unknown/not-applicable explicitly; do not invent `0`/`1`, copy a cumulative business count or infer replay from zero rows. If exact new counts prove necessary, add them at their real owner and deliberately update the closed response contract, not as incidental telemetry fields.

`limitedJson` at `bpay-next-job-step.js:292–324` already increments actual streamed encoded response bytes at 311 under the existing 128 KiB cap, then discards that count. Reuse that local counter for a bounded measurement observer. JSON reserialization is not wire-byte measurement; `Content-Length` alone is only a declaration; wrapping `bpay-next-job-rpc.js` fetch alone measures receipt of headers, not the whole bounded read/parse/validation. A monotonic duration around the existing complete dependency operation is sufficient Worker/RPC elapsed time, and must not be presented as separately measured DB execution time. Browser/memory measurements remain later checks, not a new harness now.

Pay Query's current exact private admission routine is `weekly_source_pay_query_admit_v2()` (`05102026_0044:10–18`), raising **SQLSTATE 55P03 plus exact `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY`**. Current Rule B seal calls it before capture (`10102026_0151:49`); existing financial service/PIN owners also call it. The pure gate itself (`05102026_0308`) is not a contention admission. Existing `bpay-busy.js` recognizes only BPAY_SOURCE_FAMILY_BUSY/BPAY_APPROVAL_EVIDENCE_BUSY; the financial step translator at 347 maps 55P03 to the existing transaction-retry code. For RQ-268 measurement, capture only that exact structured database refusal at the existing admitted call seam before translation, retaining the current public typed error/retry behavior. Do not claim every generic 55P03 is Pay Query contention, broaden the old recognizer, invoke the private routine directly or resurrect F-25 protocol work.

## Build and verification boundary

Proceed with the exact missing lifecycle columns and bounded Worker observation hooks after their existing dependencies. First define count/byte/duration semantics for every fixed kind and replay/refusal. Reuse the normal finite source gates and existing relevant units; no duplicate benchmark or local proof programme. PF-06-D03 remains a written future joined checklist and all app boxes remain unchecked. A schema compile or source review does not prove FIFO fairness, queue wall-time or browser/DB latency under live contention.

Source hashes at census:

| File | SHA-256 |
| --- | --- |
| `supabase/release/current-contract.json` | `dfb75197e9d914dfca13111a1b05b192b95e58f53955cbefb766e0c34915ec5b` |
| `broker/src/index.js` | `57893c36c37e2ea69c01a3cc8d11dcbf35ed655f89bc258c3a4eb690a1ad1825` |
| `broker/src/bpay-next-job-step.js` | `a103b8e4d6551ebe879392c3fbe2087ea2b8aeea41b47c8a0e1c0cefabcfa0c5` |
| `broker/src/bpay-next-job-rpc.js` | `d00d086d44baaee414ca8509fbe9c30b91015e55982d84c2035e32a3dfba7fb7` |
| `broker/src/bpay-next-financial-queue.js` | `de92b5604e8bddd24efedd1dfeab3a09d052613484e457785412a4e9e72b9c19` |
| `broker/src/bpay-next-background.js` | `77faa6d64335f9c54a5508f09df30fa02763861c9b89b3959b8f43349ad0edf8` |
