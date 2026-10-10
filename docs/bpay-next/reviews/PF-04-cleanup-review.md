# PF-04 D01 terminal cleanup source review

10 October 2026. Independent source-only review of root-authored0408 migration,0406 cleanup/service wrapper and two final-definer entries, against BD026 and current final owners. No product edits, SQL, tests, runtime, fixtures or hosted operations were performed.

**Status: PASS for the finite D01 source boundary. No product correction required.** This supersedes the preliminary review's unproved PREPARE-lifetime finding. It is not a compilation/runtime or whole PF04 completion claim.

## Command lifetime resolves the preliminary concern

The preliminary concern treated a DONE PREPARE job as though its original command could normally be COMPLETE while the run remained open. Current native source does not make that transition:

- Final no-case owner `private.bpay_next_complete_no_case_worker_v1(uuid,uuid,bigint)` in03102026_1630 marks the worker READY and job DONE/READY (91–101); it does not update the PREPARE command.
- Rule-B case owner10102026_0151 similarly completes preparation by marking job DONE/READY (1156–1157), or DONE with REVIEW worker (1174–1175), without completing the PREPARE command.
- Final confirmation2018 requires the original PREPARE command SEALED (1495–1497). Final expiry admission0510 requires that original command ENROLLING/SEALED (67–70).
- Cleanup0406 requires the exact command COMPLETE (67–70). It therefore retains those live preparation jobs already. Expiry0510's later original-job strict lookup (126–127) remains available before its FK reference is created (145–152). Expiry completion marks its own PREPARATION_EXPIRY command COMPLETE (501), not the original PREPARE command.

Thus the current COMPLETE predicate closes the specific gap claimed in the preliminary review; adding a run lock after the job lock is unnecessary and would risk reversing native run-before-job order. PREPARE in the finite kind allowlist does not waive the command predicate. Retaining such jobs conservatively is consistent with BD026 while other truly COMPLETE unreferenced job kinds still give this cleanup actual deletable rows.

## Bounded cursor and lock order

- The private singleton cleanup_clock is locked with SKIP LOCKED. Concurrent cleanup calls serialize; a contended call reports busy and zero counts. One lane runs per call and switches to the other lane afterwards. Native UUID/bigint keysets advance past retained rows and wrap after a short page, so an old referenced first row cannot permanently starve later eligible rows.
- Request p_limit must be integer1–100. Each call visits at most that many returned rows in one lane. Job rows omitted by SKIP LOCKED are not claimed visited. A full final page may require the next empty visit to wrap; this is finite and does not create a false deletion count.
- SELECTION obtains/rechecks pay_run FOR UPDATE SKIP LOCKED before deleting its page, matching parent-before-child upload ownership. Only COMPLETE/CANCELLED parents qualify. PREPARING/REVIEW/DRAFT/EXECUTING/CANCELLING and contended parents are retained. The cursor still advances past retained pages for later sweeps.
- JOB locks each visited job FOR UPDATE SKIP LOCKED, then makes plain command/enrollment/reference reads. It acquires no later Candidate/run/business lock, so it does not reverse native business-before-job order. Current FK insertions need the referenced job key and remain protected by this row lock and RESTRICT constraints.
- DONE and both lease fields are checked. Command UUID, agency sequence and epoch must match the job, with COMPLETE, sealed_at_utc, known expected count equal to enrolled count, and actual singleton enrollment_clock.last_enrolled_sequence at least the command sequence. Unknown/nonterminal states and unknown kinds remain retained.

## Retained authority and truthful counters

Only private.bpay_next_selection_page and private.bpay_next_job are DELETE targets. The upload page qualifies only after terminal parent closure; original permanent selections, choices, run members, Rule-B query snapshots/pins, workers, frozen detail and financial origins stay intact. No TTL, archive, new financial command kind, guard bypass or dependent-row deletion is introduced.

All seven actual RESTRICT job references are checked: case_allocation_state.job_id; preparation_expiry_worker.job_id and.original_job_id; work_collection.last_job_id; destination_posted_leg.job_id; run_work_query_pin_v2.prepare_job_id; installed-unused effective_work_cut.prepare_job_id. Presence of any reference retains the job. Cleanup never deletes a referencing row to make the original job eligible.

PK-limited candidate rows and actual DELETE ROW_COUNT make rows_deleted truthful; rows_retained is visited minus deleted. In SELECTION, skipped/contended parent rows count retained; JOB lock-skipped rows are outside visited. These are page facts, not a claim to count all rows or prove measured statement cost. Late references to intentionally retired closed metadata retain the existing truthful refusal; no fake DONE and no broad ACK of arbitrary JOB_NOT_FOUND are added.

## Schema, RLS, ACL and registry

0408's actual types match use: UUID run/job cursors, bigint positive page cursor, paired selection cursor nullability, singleton smallint id1 and finite SELECTION/JOB lane. RLS is enabled; all table access is revoked from public/anon/authenticated/service_role. Its sole row is seeded by migration.

The private cleanup is SECURITY INVOKER and has no EXECUTE grant to those roles. The fixed public(integer) wrapper is SECURITY DEFINER, requires service_role JWT and stable NEXT module SHARE admission, then calls the private helper under the mapped logical owner. Its EXECUTE is service_role-only. Owner/search_path declarations, final revokes/grant and schema notification are present. This preserves existing audited logical-postgres release mapping; no browser authority is created.

The final-definer registry has exactly the two new cleanup signatures, each bound solely to0406 with no competing predecessor. No financial owner/body is changed by this deliverable.

## Reviewed source SHA-256

| File | SHA-256 |
| --- | --- |
| supabase/migrations/10102026_0408_banking_pay_next_terminal_cleanup_clock.sql | 8aa263910be4f9f492fd172aae4e55b9259faec54ceb8759785457ddab1f6472 |
| supabase/repeatable/10102026_0406_banking_pay_next_terminal_cleanup_v1.sql | 434704fd74342fc461eb30ce09d812ff51eea2a05e83be42af9a14a2203fe4c8 |
| docs/bpay-next/final-definer-closures.json | 06b12aca8673b4d5b113684e21c744ba92d29618de43b70e585b71042cab3309 |
| supabase/repeatable/03102026_1630_banking_pay_next_no_case_completion_v1.sql | 3816b38a9b0776e033abbc55fa010cf62523294070c09a9c8508e36ac2d63d9c |
| supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql | 2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf |
| supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql | f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa |
| supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |

Existing queue/closed-metadata acceptance remains NOT_RUN and belongs to its named transport work. No testing programme or new proof framework is proposed by this source review.
