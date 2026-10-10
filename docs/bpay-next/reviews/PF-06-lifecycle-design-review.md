# PF-06 lifecycle supporting design review

10 October 2026. Read-only independent review of the proposed three-column/job-trigger supporting piece. No SQL, runtime, tests, fixtures, hosted calls or product edits. This reviews the design, not implementation or installation.

**Verdict: valid narrow supporting piece, subject to the historical-NULL and transition rules below.** The PF-06 card expressly asks for census and exact missing job clocks. Adding nullable `created_at_utc`, `started_at_utc`, `finished_at_utc` to the existing private job row plus one local BEFORE INSERT/UPDATE trigger addresses a confirmed gap without rewriting monetary/enroller/F-25 owners. It does not require a new step ledger, protocol, RPC or data backfill.

## Existing guards and callers

The sealed `supabase/release/current-contract.json` has **zero triggers on `private.bpay_next_job`**. The existing `03102026_1342` immutable guards attach to revisions/children, Work pointers, effects/case events and policy rows, not the operational job. Neither sealed trigger inventory nor source attachment search shows an existing job immutable trigger. Thus there is no current job BEFORE-trigger ordering conflict to work around. Preserve every existing attachment.

The job is intentionally mutable: fixed owners update phase/cursor/counters/lease/status; the queue recovery service (`04102026_0560:65–80`) also updates `queue_wake_after_utc` while keeping status and lease unchanged. This recovery-only UPDATE must not synthesize start/finish times. The ordinary CLAIM owner (`03102026_1430:442–445`) updates LEASED and increments attempts; expired-lease CLAIM can update LEASED→LEASED. The first start must remain pinned across these retries/reclaims.

The current job contract is owner-only (`postgres` logical owner ACL), no browser/service table grants, and currently no RLS. The new supporting columns need no new table ACL/RLS policy or public visibility. Use a private trigger function with a fixed `pg_catalog` search path, audited logical owner and revoked direct EXECUTE for PUBLIC/anon/authenticated/service_role. It can remain SECURITY INVOKER: changing NEW fields requires no auxiliary read, privilege elevation, lock, external callback or function call beyond server clock. Register its final repeatable definition under the existing F-04 source gate; attach/drop-if-exists/create in the repeatable, not immutable migration.

Existing owners use `%rowtype` and explicitly construct fixed JSON/TABLE receipts; the inspected source has no job whole-row `to_jsonb(v_job)`/`row_to_json(job)` emission or function returning the job composite. Additive columns do not justify changing any closed Worker response schema. Keep all money/lease/counter/status expressions unchanged.

## Required narrow clock semantics

1. The migration adds **nullable, no-default** columns only. Otherwise adding a default can make old rows appear newly created. Do not UPDATE historical rows. Nullable finite checks may reject infinity while allowing genuine absence; do not require started time for every DONE job.
2. On INSERT, set creation from the actual server clock, ignoring any supplied timestamp; set start only if the actual inserted status is LEASED, and finish only if the actual inserted status is DONE/FAILED. One sampled clock can be reused for these same-row insert observations. The current enrollers normally insert READY, but the trigger should not invent a first lease for another actual initial state.
3. On UPDATE, pin creation to OLD, including OLD NULL. Pin any existing start/finish to OLD. **For historical rows with OLD.created_at_utc NULL, keep all three timestamps NULL even if later discovery, reclaim or completion updates them.** Merely testing OLD.started_at_utc/finished_at_utc IS NULL would misleadingly timestamp a historical job's next observed lease/terminal update as its original first event. Alternatively a deliberate distinct “first observed after installation” field would be a different design; it is not needed here.
4. For a row whose creation was genuinely captured by this trigger, populate missing start only on entry into LEASED (`OLD.status IS DISTINCT FROM NEW.status`), and missing finish only on entry into DONE/FAILED. A recovery wake, cursor page, LEASED→LEASED reclaim or terminal replay must leave captured values unchanged. Override timestamp-only caller changes instead of treating a request-supplied value as evidence.
5. Use actual `clock_timestamp()` for the observed mutation, not transaction start, scheduling/deadline columns or supplied timestamps. The value records the durable row transition if the enclosing transaction commits; a later exception rolls it back. It is not proof of commit until the owning result is confirmed, nor a separate DB execution-duration measurement.
6. Job **status** DONE/FAILED is the proposed finish condition, not phase or a related request's status. In existing cancellation owners, a blocked request is terminal for that job and writes job `status='DONE', phase='BLOCKED'` (`04102026_0260:349–353`, `04102026_0460:228–231`); that correctly gets a job finish. A job whose actual status is BLOCKED stays resumable and gets no finish solely for that status.
7. Do not impose a new requirement that every terminal job was leased. Expiry enrollment can close an original READY PREPARE directly as DONE/EXPIRED (`04102026_0510:125–132`), before any worker/claim. It must remain valid with created/finished present and started NULL for a new job, or all historical timestamps NULL. Do not change status authority/reopen rules as part of measurement.

A pure NEW-row clock trigger adds no business lock/FK target or counter after an owner's clock boundary and does not alter authority, replay, Source admission, money helpers or FIFO selection. Do not read other tables or take admission locks in it. Concurrency is already serialized by the row mutation; preserving OLD values keeps first observations stable without a second query.

## Count and completion limits

Reusing `applied_line_count` under its existing **cumulative applied-line** meaning is correct and avoids a duplicate counter. It is not universal rows visited or a per-RPC count for returns, PAYE, CLAIM or all other kinds. Keep each existing owner's typed counters and the Worker `rows_visited` distinctions recorded in `PF-06-next-scope.md`; log unavailable/not-applicable counts honestly. This proposed supporting piece is sufficient for the missing lifecycle-clock part of D01, but must not alone be described as complete per-kind count telemetry, D02 step logging or D03 future in-app measurement.

No current-source consistency defect blocks the proposal when those rules are implemented. Review the actual new migration/repeatable before locking/installing, including no default/backfill, pinned old NULLs, exact local-only trigger mutation, idempotent attachments and owner/ACL closure. All future in-app PF-06 checks remain NOT_RUN.

## Source hashes

| Source | SHA-256 |
| --- | --- |
| `supabase/release/current-contract.json` | `dfb75197e9d914dfca13111a1b05b192b95e58f53955cbefb766e0c34915ec5b` |
| `03102026_1342_banking_pay_next_guards_v1.sql` | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| `03102026_1430_banking_pay_next_position_writer_v1.sql` | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa` |
| `04102026_0510_banking_pay_next_preparation_expiry_v1.sql` | `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa` |
