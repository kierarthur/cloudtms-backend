# PF-06 future joined acceptance

Execution NOT_RUN. Source hooks only; no timing scenario, fixture or harness is built here. Reuse one later owner-authorized joined TEST receipt across these clauses.

- [ ] List Draft job insertion/first-start/completion UTC from existing private job rows and correlate the job_id/kind/attempted step logs. New jobs have captured creation; historical NULL is unknown. Existing applied_line_count and owner-specific counts retain their own meanings.
- [ ] Start a small run alongside a large run and measure each command/member-to-job creation and creation-to-first-start delay in existing FIFO order; no new enrollment mechanism.
- [ ] Correlate cancellation, CSV return and PAYE update job clocks/step logs to their own completed state, including replay and genuine pending/refusal outcomes. BLOCKED is resumable.
- [ ] Observe an actual Pay Query admission contention reply: exact55P03/WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY produces the labeled busy step line with the same job/kind/attempted step. Other lock retries must not be labeled PayQuery contention.
- [ ] Confirm each actual bounded job call emits exactly one structured line with job_id,kind,step,rows,bytes,ms; no payload, Candidate/Work/Timesheet identity, money, cursor, nonce, header or credential. Zero visited rows is not proof of replay.
- [ ] Separate measured database execution, whole Worker dependency/read/validation time, queue delay, browser wall time and memory in the same joined receipt. Worker ms is not isolated DB time. Rows are the validated per-call visited count or null; bytes are actual read response bytes or null when not read. A failed/logging-disabled observer must not change SQL success, queue continuation, ACK or retry.

All full Worker/queue/minute paths and application boxes remain NOT_RUN. Schema compile/source verification is not business/timing acceptance. The sole approved phase3 300Timesheet queue timing remains separate and later. No hosted installation or external action is authorized by this checklist.
