# F-25 QuerySync branch-evidence schema review

Reviewed 10 October 2026, saved source only. No SQL, database, runtime, tests, fixtures, hosted reads or product edits were performed. Migration 0036 remains unlocked/uninstalled at this review. This does not qualify the QuerySync owner, install the protocol, or complete F-25.

Source after correction: `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql`, SHA256 `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb`.
Design after clarification: `docs/bpay-next/design/F-25-query-sync-completion-build.md`, SHA256 `e78e73c208b29aef2e12ae4531959e70d74e38846c92c6b1221ac42128a0714c`.

Follow-up verdict: **PASS for this additive schema source slice; QSB01 closed, no open concrete schema defects found**. The complete corrected 60-line migration was re-read against the current design. The insert, typed digest and owner qualification repeatables remain explicitly pending; no execution or installation verdict is inferred.

## QSB01 — closed: retain the original issue episode and omitted work identity

Original P2 finding at SHA `3e737df47f08028a42e79bf4a6d7efbae85e704ae9af915a72fe0c7635769d71`: the initial input certificate lacked captured `issue_episode`; the omission certificate lacked both `issue_episode` and `work_event_id`. The design explicitly requires these at lines110,129–130 and139, and binds them into each child row digest at177.

The original comparison table has `incident_id` and `revision_number`, but no episode (`15092026_1534_weekly_source_plan6_schema.sql:1444–1468`). `UNCHANGED_OPEN` deliberately has no recheck/resolution event. Its later assertion therefore has no immutable child value or original event from which to recover the accepted episode. Looking at `weekly_discrepancy_incidents.episode_number` later uses the mutable incident; current incident identity staging guard 0017:854–855 pins its IDs/scope but does not pin `episode_number`. NEW/CHANGED/resolved events carry an episode, but that does not solve the unchanged-open branch. The omission certificate likewise does not retain the work identity captured by the complete original selector.

Verified correction: positive NOT NULL `issue_episode` is present at current0036:18/38; omitted work has the actual UUID FK at37 and per-completion uniqueness at44. Both rows now have mandatory 32-byte `evidence_sha256` at25/41. QSB01 is closed. The pending private insertion helper must still capture these values from the genuine locked original selection and validate exact comparison/event incident/episode identities before the clock. The additions do not change the original omission predicate or introduce a business callback.

## Required remaining completion proof

Verified schema correction at0036:6–12: the header now has separate `query_input_completions_sha256` and `query_omission_completions_sha256`, required NOT NULL and 32 bytes for QUERY_SYNC, and explicitly NULL for every other allowed owner. The existing header's owner_kind is NOT NULL, so the disjunction and explicit null checks have no UNKNOWN acceptance hole. The schema now supports the design's complete child-set binding. A successor to the actual0030 typed completion digest must still include both aggregate values and recompute every typed row digest/canonical complete set. The existing 0020 `original_witnesses_sha256` keeps its original comparison/event witness meaning. No digest-writing/qualification implementation is claimed by these columns alone.

The pending repeatable must also prove current xid/epoch, producer BUSINESS_OPEN/input OPEN/root kind and stack, full original selected/omitted census, <=1000 bounded child sets, exact input outcomes/counters, no input/omitted overlap, true-tail original evidence qualification, and complete raw stages. Scalar FKs establish existence; they do not establish same incident, work, episode, original event kind, or completion owner. This schema-only review does not mark those missing runtime guards as installed.

## Checks passed

- Every referenced UUID targets a real native UUID primary key: weekly work events535–536, incidents1416–1417, comparisons1444–1445, discrepancy events1658–1659, and private root completion0020:7–8. Restrictive FKs retain the original evidence.
- `outcome`, current comparison and required evidence SHA are NOT NULL. The branch disjunction explicitly handles each nullable prior/recheck/resolved/matched field. In CHANGED, a NULL prior makes `IS NOT NULL` false; the comparison inequality cannot turn that branch into an accepted UNKNOWN. No boolean NULL escape was found.
- `matched_facts_sha256` is required only for UNCHANGED_RESOLVED and absent elsewhere. Its octet-length check combines with the branch's explicit NOT NULL. Full typed `input_issue_sha256` is mandatory for every branch. The matched-resolved business predicate must remain its original narrow fact comparison; the schema does not impose current full-fingerprint equality.
- Both captured episode fields are native integer, NOT NULL, and positive. Both row proof digests are native bytea, NOT NULL, and exactly32 bytes. The two header child digest constraints separately forbid missing QUERY_SYNC values and values on SYSTEM_HOURS_ACCEPT, preserving that owner's existing insert shape.
- Per-completion input/work and incident keys prevent duplicate input outcomes. Omission incident/event uniqueness is valid for the original one-event-per-incident owner. Both tables contain only typed original identities/digests, no request, personal/contact, prose or financial snapshots.
- Both private tables are postgres-owned, RLS-enabled, and revoke all privileges from PUBLIC/anon/authenticated/service_role. No policy or browser/service insert path is added. UPDATE/DELETE and TRUNCATE attach the existing unconditional immutable-evidence exception function (2153:483–489).
- The migration is additive and has no job, command, publication, finance counter, historical repair, data backfill or destructive business DDL. One-time table/trigger creation is suitable for an ordered locked migration; repeatable idempotence belongs to the future qualification attachment.
