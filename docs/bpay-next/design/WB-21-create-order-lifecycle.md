# WB-21 Create membership and order lifecycle — source investigation

Status: finite source design recommendation, 10 October 2026. No product edits, execution, SQL installation, tests or runtime calls. The builder explicitly identified the small PREPARE/OPEN compatibility seams as necessary WB-21 support; this is not a restart of F-25's parked producer protocol. Existing Rule B expected revisions, four captured Candidate fields, query snapshot and query pin remain the financial authority.

The initial unchanged-enroller proposal is unsafe. A genuine click-order reservation can use the existing PREPARE command and agency clock only with the exact lifecycle compatibility below. Recording `command_clock.last_sequence` without receiving PREPARE is an input cutoff, not execution ordering: a later financial command can otherwise receive/enroll ahead of PREPARE before seal.

## Current exact constraints

| Current physical source | Exact evidence | Consequence |
|---|---|---|
| `03102026_1404_banking_pay_next_command_intake_v1.sql:18–66` | Module SHARE, command-clock UPDATE, exact existing kind/epoch replay, then receive PREPARE as RECEIVED | One existing order, one exact command ID; no second clock/kind |
| `03102026_1430_banking_pay_next_position_writer_v1.sql:127–150` | Enrollment clock UPDATE, next command UPDATE; PREPARE branch precedes generic SEALED check; run SHARE and require ENROLLING/run SEALED | RECEIVED/OPEN currently raises; it does not wait |
| `03102026_1590_banking_pay_next_selection_manifest_v1.sql:149–184` | Run UPDATE; existing run_command immediately returns replay; only absent binding performs seal checks/RECEIVED→ENROLLING | Prebinding CREATE requires the existing-binding branch to transition once after all original seal checks |
| `04102026_0510_banking_pay_next_preparation_expiry_v1.sql:66–83` | Original PREPARE must be ENROLLING/SEALED and match native selected Candidate count | Existing expiry rejects an abandoned RECEIVED/OPEN PREPARE |
| Same file `:125–137, :587–595` | Nonnull original PREPARE requires an actual original PREPARE job per Candidate; immutable worker guard couples original command/job absence | A retired no-job PREPARE needs a narrowly proved no-original-job alternative; do not fabricate jobs or set original ID NULL |
| `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql:23–34` | Progress means actual job OR enrollment-clock advance | Waiting returns null/no clock change; retiring an exact no-job predecessor can advance enrollment and return null without changing this wrapper |
| `04102026_0520_banking_pay_next_preparation_expiry_service_v1.sql:31–53, :60–91` | Existing due index/100-row discovery and deterministic exact expiry UUID | Existing discovery/transport can remain; deadline continues to be server-owned 72h |

## Storage recommendation

Use a small owner-only accepted-offer header and typed ID/revision membership rows. Do not prepopulate final `run_selection`/`work_choice` simply to stand in for upload receipts. Their real insertion order, page ordinals, page counters, ALL/SUBSET initial shape and component guard (`0570:21–66`, current carried1236 guard) must remain owned by the manifest materialisers.

An offer header binds one run/actor/module epoch/official date, one command UUID, exact bounded immutable choice descriptor, its exact request identity, original maintained scope/context and native command sequence. Membership rows contain only actual identities, expected approved revision, native Candidate and stable ordinal, selection mode/selected boolean and accepted skip code when applicable. WORK rows require actual `(work_id,candidate_id)` and `(work_id,expected_revision_id)` provenance; CASE rows require actual Case/component/Candidate IDs, without frozen balance amounts. Explicit component/segment overrides retain their actual immutable line/key and expected revision. Exclusions remain the named D14 owner. Do not retain a generic economic JSON snapshot or finance amounts here.

CREATE must retain the actual complete native keys against one consistent scoped source read; a retained numeric context plus later live MATCHING enumeration cannot reproduce a removed/replaced key. The server subsequently pages the accepted membership by stable native ordinal, at most100 emitted rows, never live amount/name ordering. Final `run_selection`, `work_choice`, selected components/cases and exact page receipts are populated once from those retained inputs by the existing named owners. Exact replay does not choose new identities.

Whole-scope retention is a real CREATE cost even when the browser descriptor is small. Do not claim bounded100 transport proves a bounded CREATE snapshot. Use the existing maintained scope/index predicates, body/descriptor bounds and a real statement limit; reject genuinely excessive/inconsistent scope before acceptance rather than return an incomplete first100 success. This is an implementation limit decision, not authority for a new background freeze engine or versioned producers.

## CREATE ordering and receipt

1. Perform original service/Office authority, closed descriptor/UUID/boolean validation and original official-date checks. Acquire the existing module/Source admission in the established order before material facts or query capture; no gate read after the agency clock tail.
2. Lock/revalidate the exact existing run and replay first. Bind one browser-generated command UUID in the immutable CREATE intent; exact CREATE retry checks the same actor/date/descriptor/command. It must not allocate a new UUID after a lost reply.
3. Retain the accepted native membership, expected revisions, permitted Candidate fields and already-existing query facts through the chosen capture-once seam. Preserve their Rule B provenance and later-revision REVIEW policy; no producer/version/resolver/witness machinery.
4. After all original business locks/capture work, call existing `receive_command_v1(command_id,'PREPARE')` once. This remains the final agency-clock tail. Insert the exact existing `run_command(run_id,command_id)` binding and offer sequence before committing. All later uploads materialise the existing receipt; they never receive a second PREPARE sequence.
5. Return the actual command UUID and agency sequence as a typed decimal string in the CREATE receipt, alongside the retained run/date/selection identity. Worker and FE correlate these exact values. Keep the existing SEAL_ENQUEUE authoritative run/command/sequence receipt and original wake; the accepted command can exist without a job before seal.

## Required small enqueue/enroller changes

**Enqueue successor:** take the existing run lock, load the binding, and validate exact command ID/kind/module epoch. If its command is RECEIVED, run all existing `1590:163–170` sealed/count/Candidate checks, including zero-work case-only shape, then perform the original expected-member-count + ENROLLING update exactly once. Existing ENROLLING/SEALED/COMPLETE exact repeats retain original replay behavior. An OPEN received command must not be reported enqueued merely because run_command exists. Do not change original job creation/PIN/CLAIM, command kinds or final selected Candidate count.

**Enroller successor:** insert only the exact PREPARE RECEIVED wait and no-job terminal-retirement alternatives before original `run FOR SHARE`. The selected command is already locked under the enrollment clock. For RECEIVED, validate exact binding/epoch, no enrolled members/jobs, and a plain MVCC run/offer read showing legitimate OPEN acceptance; return null without enrollment advance or business-row lock. Do not acquire run SHARE on this path: enqueue/expiry hold run then need the same command, so command→run locking would introduce a deadlock.

For ordinary ENROLLING PREPARE, preserve the complete original branch, run SHARE and native member/job creation unchanged. For a COMPLETE zero-member PREPARE retired by exact OPEN expiry, prove the retained original binding, real expiry request, same epoch/deadline, real run CANCELLING/CANCELLED, no original jobs/members/run workers, and zero command expected/enrolled membership; advance only the already-locked enrollment clock by exactly this next sequence and return null. This is metadata retirement of a genuinely never-enrolled receipt, not a generic FAILED reset, skip over an unsealed live command or fake successful preparation.

## OPEN expiry and guard integration

Keep the existing due discovery, deterministic expiry request UUID, real deadline and ordered PREPARATION_EXPIRY command. Under the original module/advisory/run locks, a new narrowly admitted original binding is PREPARE RECEIVED, matching epoch, run OPEN/unconfirmed and deadline due, with expected_member_count NULL, enrolled_member_count0, no original command members/jobs and no run worker. These conditions prove no preparation/hold operation started. Revalidate them under an UPDATE lock on the original command. The waiting enroller path above must not take run SHARE, which avoids run→command versus command→run inversion here.

Retire that exact original PREPARE to COMPLETE with expected_member_count0, enrolled_member_count0 and a real sealed timestamp. Keep its original UUID, agency sequence and run_command. Then receive the ordinary existing PREPARATION_EXPIRY command and retain its native expiry request, including actual selection/page/review/Candidate counts. Do not overwrite uploaded membership or set original_prepare_command_id NULL. Partial uploaded Candidates are factual metadata; their actual count is not the zero original PREPARE enrollment count.

Use the existing bounded expiry Candidate enrollment/cleanup path for uploaded partial Candidate membership. Its genuine zero-work/no-worker branch already exists: expiry binding can have run_worker_id NULL, WORK loop then emits no lines, CASE loop emits no holds, and `apply_expired_week_v1:206` returns for NULL worker. Keep real Candidate counts and original immutable choice/page facts. No financial holds are released or week amounts fabricated when none existed.

Two exact no-job compatibility checks are therefore required in `0510` successors:

- `enroll_preparation_expiry_one_v1`: if the exact retained original PREPARE is COMPLETE with expected/enrolled0 and no original jobs/members, and it is bound to this due OPEN-expiry request, preserve original_job_id NULL rather than SELECT STRICT a nonexistent PREPARE job. Any run worker on this branch is an evidence conflict. All ordinary original-job retirement branches stay unchanged.
- `preparation_expiry_guard_v1`: admit original_prepare_command_id nonnull/original_job_id NULL only for that same exact zero-member COMPLETE/no-job original receipt and real request/run/Candidate binding. This is an exception to the current command/job-null coupling, not a nullable bypass for other original PREPAREs. Existing request initial0 counters, immutable fields, real count equality and completion transitions remain.

No additional protocol table/command is needed for this lifecycle. The existing command state and exact retained expiry request prove this branch; do not add a generic witness/certificate framework. Existing `expiry_fenced_v1` count/deadline/review checks and actual partial-Candidate cleanup can remain. Zero uploaded Candidates use the existing zero-candidate EXPIRED/CANCELLED branch. Existing uploaded-but-unenrolled Candidates use actual bounded zero-work cleanup, rather than being counted complete by changing the original run counts.

Once original receipt is retired, enroller advances that one predecessor. It does not jump to the later expiry sequence; any intervening accepted commands retain normal order. The expiry command then uses existing enrollment/jobs. Exact expiry replay retains the same request/sequence and original command remains terminal. An exact CREATE replay may return the retained expired acceptance, but new page mutation/SEAL on expired run must refuse. Local accepted journals are retained until an authoritative terminal receipt, never cleared as an unsubmitted choice.

## Finite scope boundary

Required supporters are the named CREATE/intake/append/seal plus exact `enqueue_preparation_v1`, the two metadata branches of `enroll_one_v1`, and the OPEN/no-job alternatives in existing expiry acceptance/enrollment/guard. These are necessary consequences of receiving the existing PREPARE at CREATE. They do not change financial job membership/claim/PIN, F-25 Rule B semantics, original producers, command kinds, clocks, money/capacity owners, parked files or future DR/WB lane contracts. The immutable predecessor roots need new dated successors and exact registry entries; do not edit installed predecessors.

The implementation must show full original-body preservation outside these finite branches and actual declared offer schema/FKs/ACL/guards. This note is not a claim that either support or complete descriptor expansion is already built. Source/runtime acceptance remains the builder's separately authorised work.

## Read source SHA256

| Source | Raw SHA256 |
|---|---|
| `supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql` | `be3c7dd17ecea6cbff631ed0b39c9b09947e6284b93db04ecdf517e320fed3e1` |
| `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql` | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa` |
| `supabase/repeatable/03102026_1590_banking_pay_next_selection_manifest_v1.sql` | `47a0df6a06bd7816b0102c1aa5fca0bddd97ebc08a0a04d786254db3179967b3` |
| `supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql` | `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa` |
| `supabase/repeatable/04102026_0520_banking_pay_next_preparation_expiry_service_v1.sql` | `81ffc53b673cfb5c0d5fa352c4508f14ef347fd73582c8a09022f210f92c7b9d` |
| `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa` |
| `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |
| `supabase/repeatable/10102026_1621_banking_pay_next_preparation_pay_date_v1.sql` | `e8415355b5a11f2e75e0bc532f5610b920d542799b89725bba25493d1f1af641` |
| `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `supabase/migrations/04102026_0501_banking_pay_next_preparation_expiry.sql` | `c9758121675a070ae40f1be352a40247f357309ae94efca59baf6de002cac845` |
