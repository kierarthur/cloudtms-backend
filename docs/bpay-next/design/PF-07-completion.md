# PF-07 sequence 111 — ordered durable intake and native identity wakes

BUILDER DECISION BD123: reuse the current approved DR-04 owners. The PF-07 historical full-token queue envelope wording is BLOCKED/superseded by completed DR-04 BD090 and SP-14 BD122. Queue messages remain the closed native {id,kind} wake. Version, action, lease, epoch and cursor validation remain in the original database-owned CLAIM/STEP invocation, never in a financial message payload. This preserves the card's durable ordering, replay, bounded-step, recovery and awaited publication requirements. No second intake, new protocol, command, route, producer, money owner, table or queue schema is built.

Exact checklist scope: D01 current public.bpay_next_financial_intake_v1 (2041 successor of0270); D02 broker/src/bpay-next-financial-queue.js; D03 broker/src/bpay-next-background.js; D04 historical0270 receipt reconciled with2041; D05 authored future joined acceptance; D06 the nine existing-owner checks below. Required supports are original receive1404, enroller2041, enrollment0541, wake0560, financial_job2041 and the current fixed runtime adapter/integration. All were read before completion. Existing correct code is reused without a new SQL successor. Current registry and source include order are unchanged.

## Nine required checks

These checked entries are source verification, not executed business acceptance. The detailed current-signature, line and hash map is in PF-07-current-investigation.md.

- [x] Monotonic accepted sequence: original command-clock UPDATE serializes accepted receipts in receive1404; transaction rollback cannot create an accepted sequence.
- [x] Original duplicate receipt: original ID/kind/epoch receipt returns its existing agency_sequence. Current typed intake keeps its immutable action receipt and economic inputs.
- [x] All command N membership enrolled before N+1: enroller2041 locks enrollment clock and only N+1; partial PREPARE membership retains ENROLLING and advances the clock only at its exact sealed count.
- [x] Enrollment independent of financial effects: enrollment creates durable READY jobs and membership, then advances its own clock without waiting for job completion. Original Candidate earlier-job guard remains separate.
- [x] Due scheduled actions: original available_at and CLAIM eligibility remain server-owned; bounded wake discovery supplies only existing native job identity, never browser time or economic values.
- [x] Failed enrollment is visible and recoverable: enrollment0541 subtransaction rolls back partial enrollment and stores SQLSTATE/time only on exact N+1; current PROGRESS exposes enrolment_failed and the existing Continue processing WAKE recovers it. No financial error/readiness is fabricated.
- [x] Metadata-only messages: closed id/kind validator rejects additional fields, nested/live objects and token payloads. Financial data and checkpoint authority stay in durable native rows.
- [x] Busy is not REVIEW: four definite transaction-rollback states 55P03/40001/40P01/57014 are retryable; transport timeout is unknown. Unsupported55000 is not promoted to safe retry. SP-14 source review is retained.
- [x] No retained connection between steps: one invocation opens fixed HTTP RPCs, fresh CLAIM and at most one STEP. A nonterminal successor send is awaited before ACK; unknown replies/send failures retry identity and native expiry recovers the committed cursor.

Original Rule-B expected revisions, captured Candidate fields and query snapshots are unchanged. Earlier accepted commands retain the original DB order; post-click changes belong to the next Draft. No producer-evidence framework or parked F-25 resolver is used. Case balances remain live under original money locks.

## Future joined in-app TEST checks — AUTHORED_NOT_RUN

- Close the browser during preparation, reopen the same Draft and compare its retained progress and original command receipt; duplicate the same command and verify the same receipt/effect.
- Exercise lost CLAIM/STEP replies, duplicate deliveries and failed continuation publication through the actual Worker/queue/minute recovery. Verify the same durable job resumes without duplicate money, lease adoption or repricing.
- Observe complete enrollment of N before N+1 with multiple Candidates, with enrollment independent of job completion. Observe a due scheduled action through its original availability owner.
- Observe a failed enrollment as a visible blocker, then recover through the existing Continue processing action without advancing past N prematurely.
- Verify an earlier accepted Candidate change is bound before the click and a later change belongs to the next Workbench.
- Contend the Pay Query admission key:55P03 is retryable busy, never REVIEW. Unknown transport outcome retains original identity and cannot be treated as a known rollback.

No hosted installation, business RPC, test data, queue/minute journey, timing result, provider outcome or financial first use is claimed. The owner forbids a local testing programme. These checks are authored for the later single authorised TEST journey.

## Normal checks

PASS: db:check356 migrations/918repeatables; four named Worker node--check calls; existing12/12 source gates; sole PG17 changed-load0migrations/0closures; unchanged local contract seal98ea49ac3a52bc2194b6140534410a47ffe8779f9e4ef592dc2fbfa1d4d39e14 and coupling. SOURCE PASS independent receipt reviews/PF-07-current-source-review.md covers all nine source behaviors. SQL/public SECURITY DEFINER membership/grants are unchanged; no verifier membership constant change is required for this documentation-only reuse item.
