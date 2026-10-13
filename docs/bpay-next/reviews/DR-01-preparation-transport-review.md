# DR-01 preparation transport review

Verdict: SOURCE PASS for the scoped Worker and existing Office transport changes. No concrete open source finding was found. DB installation, business calls, queue/app/timing and joined acceptance are NOT_RUN by this reviewer.

Authority read: full `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:39-66`; every DR01 ITEM/D01-D11 checklist row; current design `DR-01-pay-date-and-skip-receipts.md` (BD065), current scope investigation and snooze follow-up; full current Worker module, scoped Office input/receipt/transport/controller code, existing fixed RPC and route wake, and full1621 SQL as a receipt/schema cross-check. No product edits, tests, fixtures, runtime, SQL or hosted calls were performed. This is the D05/D06/D07 transport review, not a claim that one review closes every DB or later screen deliverable.

## Exact changes and source evidence

| Contract | Current source assessment |
| --- | --- |
| Optional CREATE date | Worker `commandCall:217-222` accepts exactly `{run_id}` or `{run_id,pay_date}`. FE `requestBody:225-231` mirrors those two shapes. A supplied value must remain a valid canonical YYYY-MM-DD date; NULL/extra keys do not become omission. No caller time/as-of input or new route is introduced. |
| Authoritative CREATE receipt | Worker OUTPUTS:47 and FE RECEIPT:40 require `official_pay_date:date`. Worker259 and FE236 correlate it exactly with a supplied pay_date; omitted date has no invented client comparison. Returned run ID and all original counters/state remain strictly correlated.1621 public intake:370-375 emits the actual stored date; exact CREATE replay uses the retained actor/date before fresh date settings. |
| Definite wrong-date refusal | Worker417-418 matches only mutation CREATE + SQLSTATE22023 + exact `BPAY_NEXT_PAY_DATE_NOT_OFFICIAL`, before the generic22023 branch. It emits HTTP400, that exact error code, retryable=false and outcome_unknown=false. Other errors retain their prior semantics; unknown/lost mutations are not reclassified as definite failures. |
| Closed skip evidence | Worker139-141 /260-263 and FE148-150 /237-243 require at most100 exact `{work_id,code}` tuples, case-insensitive unique IDs, and only WORK_WEEK_NOT_YET_PAYABLE, SELECTION_ON_HOLD or SELECTION_SNOOZED. Positions must be a strictly increasing subset of the actual requested choice IDs; outsiders, duplicate IDs/case variants, reordering, unknown codes or extra tuple fields refuse the response. Native SQL UUID output is canonical lowercase; Worker request IDs are normalized before comparison. |
| Count semantics | WORK_PAGE item_count remains positive attempted input count and is checked against choices.length in Worker268-270 and FE243. The appended selection/candidate counts remain actual SQL authority. A genuine all-skipped receipt is representable without fake accepted Work; it does not make the unchanged empty-selection seal/enqueue succeed. |
| Accepted Work identity | FE controller463 filters the original choices by the validated skipped IDs before merging accepted_page_work_ids. Skipped items are not subsequently marked accepted/disabled as if they entered the manifest. Existing uniqueness and100-ID journal bound remain. |
| Exact journal/retry | FE request bytes are stored before dispatch; receipt validation precedes adoption; fresh definite refusal clears only the initial pending attempt. A later refusal after a prior lost reply retains the original pending bytes/IDs. retry() resends the same bytes. The new fields do not create a replacement run, command, page or financial intent. |
| Bounds/auth/transport | Existing Worker32KiB request,128KiB response and8s timeout remain, with actual streamed UTF-8 byte counting and closed decoding. FE retains128KiB read,64KiB actor journal and12s caller timeout. Trusted active admin is checked before fixed service RPC dispatch; SQL retains its own authority. Environment header, redirect:error, actor checks and frozen-read semantics remain unchanged. |
| Queue wake | Worker452 marks only an accepted SEAL_ENQUEUE response for ENROLL. Existing index198848-198871 sends that finite wake; CREATE/WORK_PAGE/skips/reads do not wake or drain financial work. |
| Dormant accepted-progress kinds | ACCEPTED_CANDIDATES/ACCEPTED_WORK declarations and prior guards remain unchanged. Current Office controller515 refuses their invocation. DR01 activates no removed F25 resolver, new accepted-input protocol or new background execution lane. |

## SQL/design correlation and scope

1621 appends skips in original request order, retaining the exact request and bounded skipped_json on the existing temporary selection page. Replay returns stored skips before fresh week/snooze reads. The CREATE run's existing snooze_business_date/control_sequence is reused; only WHOLE_TIMESHEET basis-qualified historical snooze can skip a Work. Segment/expense exclusions do not become whole-work skips. Work choices retain expected revision/ALL-or-SUBSET and existing Rule B selection/PIN/capture owners. The later WB40 hold code is an allowed closed transport value, not a claim that its future classifier/writer was built here.

The unchanged zero-WORK pages>0 seal/enqueue boundary is recorded in the scope follow-up. A successful all-skipped upload is feedback, not an executable empty Draft. True Case-only selection keeps its original zero-WORK-page path. No arbitrary financial success, zero Work row, page erasure or enroller rewrite was introduced to conceal that distinction.

Screen changes are expressly outside this card: WB11 owns removing the editable pay-date UI and showing the official date; WB21 owns skipped-item display and submit sequencing. The existing Office transport receives the new receipt now, but this review does not claim those later UI deliverables or the holiday-feed acceptance gap are complete. Rule B remains the accepted-input authority.

## SHA-256 snapshot

- `broker/src/bpay-next-preparation.js`: `91b3ca6654a761a2f16d7a9b2af5360b168858681d555467d6b7cd94ddc6ea89`
- `broker/src/bpay-next-preparation-rpc.js`: `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34`
- `broker/src/index.js`: `b9872851195f757a57575b90cfade390d47ffeccc1991cbd702c01dd1dc8742b`
- `js/bpay-next-office.js`: `70cff4fde898eb1fa986ac2c270248134117a183c128e77f42372ef65b5dd513`
- `supabase/repeatable/10102026_1621_banking_pay_next_preparation_pay_date_v1.sql`: `5b6481250c71522e311492801d57359a36c21b47300fd36d98e9086fcbe108da`
- `docs/bpay-next/design/DR-01-pay-date-and-skip-receipts.md`: `255309fc296ddfb9b286f9f1162d51027e7c0d0f07518a491ae0f18d7d0c4dde`
- `docs/bpay-next/design/DR-01-snooze-scope-followup.md`: `1813f86576376b548081197a09dc25726d6cd6bc3a8efa6b3a62beaa8e37c63c`
