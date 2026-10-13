# WB-44 / WB-06 compatibility closure investigation

**Source conclusion: no concrete missing current-contract repair object or retry behavior was identified for the WB06/WB44 shared join.** Current reachable financial retries are covered by the existing owners; WB44 supplies the separate bounded Snooze/week/current-projection recovery lanes. This conclusion rests on exact preserved contracts, not on an assertion that old persisted obligations do not exist. The prior blanket phrase “F03 census proves no old obligations” is not supported by the F03 review.

Read-only source/Git inspection at backend HEAD `8d9f3d9cbf86ca2e53506760ec31a7734e0c65ab` (WB44 product checkpoint `a33796fd` plus receipt checkpoint). No SQL, DB, tests, runtime, hosted reads, fixtures, app, provider, payment or product edits were executed. App/queue/minute/timing acceptance remains NOT_RUN.

## Exact controlling scope

WB06 full card `02_WORKBENCH.md:247–279` requires bounded retry, Candidate isolation and due-index recovery (257,266), says to fix only a confirmed installed gap (266), and explicitly replaces legacy Workbench jobs rather than porting them (258,261,268). Already-authorised legacy work belongs only to F20 (260). D08 specifically names `public.bpay_next_job_wake_page_v1(text,integer)` and warns that merely changing FAILED due time is undiscoverable. WB44's Worker clause at 1571 retains persisted continuation/lease/idempotency and exact old-contract compatibility repair.

R2 review F08 at 132 supplies the concrete **WB19 administrative retry** correction: exact kind/module/current authority/definite error, valid claimable state and lease/cursor invariants, retained original error/attempt evidence; unknown outcomes use receipt reconciliation. The WB19 card at 770 names W2 RETRY_WORK_JOB and requires that action. This investigation does not mark WB19 built early or weaken its actual future requirement.

The Bible at 475–478 describes the old session/Candidate refresh protocol and makes the upgrade obligation precise: a queue-contract upgrade **which makes a previously valid persisted instruction unclaimable** must ship bounded canonical repair. Old DEAD refresh instructions are audit evidence, not payments or reusable new NEXT jobs. Applying that condition requires an exact changed contract and real canonical successor; it does not authorise guessed fields, general FAILED resets or legacy financial drains under NEXT.

## Why this batch does not introduce such an incompatible contract

Fresh Git-object comparisons against the frozen F01 base `a6f7423eb71e6e7cd173e705c2290e5408a1d077` reproduce all three saved roots byte-for-byte:

| Current root | SHA-256 / preservation |
| --- | --- |
| `04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` | `03daf223a8b572b37605f1ddc15fb777a3a543ff89140b8cff40238ec9a139fa`; exact F01 equality |
| `04102026_0280_banking_pay_next_financial_job_service_v1.sql` | `6fdd30c7b53b4e2d18d048aca46fbaadc7733a3cd5edefe4773f11b50288334d`; exact F01 equality |
| `03102026_1430_banking_pay_next_position_writer_v1.sql` | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa`; exact F01 equality; still final physical position CLAIM |

The new final apply body is `10102026_1346_banking_pay_next_position_projection_boundary_v1.sql`; its publication/application projection hooks preserve original immutable command/lease/epoch, NEW/REMOVED phases, cursor and terminal readback. Its private page cap becomes100, but the existing unchanged public financial service and financial Worker already accepted at most100. A private predecessor's256 allowance therefore is not a previously valid persisted **current service/queue** instruction that this batch makes invalid. The original financial adapter/consumer remain unchanged at the hashes below, with the same version1 envelopes,13 job kinds, phase-specific typed inputs and owner receipts.

WB44 introduces `private.bpay_next_workbench_task` and its distinct runtime discriminator for the first time. It does not reinterpret old financial job rows or replace the legacy session refresh schema. ENROLL/EXPIRY and ordinary financial message routing remain the existing protocols; new WORKBENCH_CLAIM/STEP messages use the new task contract. No exact old NEXT contract-to-new-rejection case was found in the reviewed changed owners. Consequently no additional compatibility repair RPC, status reset or old-input reconstruction is justified by this batch's source delta.

## Exact reachable retry and recovery map

| Situation | Current authoritative behavior |
| --- | --- |
| Lost financial wake | 0560 READY at65–73 reads the indexed due READY/BLOCKED identifiers; expired lease at75–82 uses `greatest(queue_wake_after_utc,lease_until_utc)`. At most100 real IDs; rotates wake only, never status/lease/available-at. Recovery background138–144 emits a fresh CLAIM. |
| Earlier Candidate command | Current1430 claim431–435 refuses while an earlier same-Candidate job is not DONE. Public0280 fresh classifier258–304 returns WAIT_FOR_PREDECESSOR or CLAIM_RETRY from actual current evidence, never a second owner invocation or another owner's nonce. |
| Definite STEP transaction error | Business changes roll back; the previously committed lease/job/cursor remains. job-step349–350 maps40001/40P01/55P03/57014 to a known transaction retry. No job FAILED transition or economic reset is manufactured. |
| Lost CLAIM reply | job-step353–355 retains uncertainty as WAIT_FOR_LEASE_EXPIRY. Due discovery plus the original private CLAIM acquires a fresh owner epoch/nonce after expiry. It cannot adopt the nonce from a lost response. |
| Lost STEP or continuation-send reply | Exact retained original job/kind/nonce/epoch/cursors are preserved; financial-queue203–206 awaits the send before ACK and retains the request on uncertainty. Native already-advanced/terminal cursor readback prevents duplicate financial mutation. |
| Unknown/invalid phase or obsolete authority | Public0280 keeps original permanent admission rejection at306–308; job-step338–347 reports typed phase/not-eligible failure. This is an honest refusal, not successful compatibility repair or permission to reconstruct missing authority. |
| WB44 metadata error or obsolete generation | New1429 retains ERROR/checkpoint/barrier and delayed available/wake times; fresh Claim obtains a genuine new nonce after the due time. Step rechecks module/owner/generation/phase/cursor and native revision/financial-view/metadata checkpoints; obsolete current projection generation is replaced from actual current scope, never old financial history. |
| Future week or Snooze expiry | background156–158 calls the finite new DUE_PAGE;160–164 discovers and publishes actual stored task IDs. Separate private Snooze due receipts use actual native ID/version/date, and FUTURE_WEEK uses the actual prepared item date index. Work origin continuation or actual Case projection refresh happens under the reviewed scoped barrier. No new financial command/enroller or economic expiry action. |

Fresh static status-assignment search across current NEXT repeatables found no `private.bpay_next_job.status='FAILED'` writer. `04102026_1647` does set the **command** to FAILED for an unsuccessful write-off outcome; that is a different relation and its actual job completion is preserved. `10102026_0352` observes a supplied terminal status only to stamp lifecycle timestamps; it does not set job status FAILED. Declaring the enum or observing FAILED operationally is not an actual producer. Therefore changing a speculative FAILED job's due time would still not make it claimable, and no such reset is proposed. A future genuine FAILED administrative action must satisfy WB19/R2-F08 in its own actual scope.

## What F03/F05/F06 and the legacy disposition actually prove

- `reviews/F-03-census-review.md:5–9` proves one exact installed-writer reader acknowledgement and unchanged scanner logic against the F01 source baseline. `F-03-COMPLETION.md` reconciles saved source pieces to their F10–F14 owners. Neither is a business-table census of old queued jobs, so neither proves their absence.
- `reviews/F-06-foundation-import-review.md:24,33–35` proves exact initial-authorised/Daily receipt shapes, real binding/publication identities and complete helpers. Import performs no seed, backfill, job repair or invocation. These correct source/schema capabilities cannot be described as a generic persisted-job repair.
- F05's exact successor corrections and F06 source binding support preserve genuine canonical inputs. They do not create an administrative FAILED-reset owner or permit guessed old command/receipt fields.
- F13 conservatively retains old Workbench wakes because their actual legacy drain can perform global reconcile before scope filters. F18 reports actual existing legacy scopes; it is not a NEXT job-contract rewrite. The Bible's existing legacy Candidate/dirty-apply repairs remain their exact legacy authorities. WB06 explicitly forbids porting their job families into the NEXT position lane. F20/F19 and later actual legacy adoption/disposition remain their named owners.

## Recommended row / item disposition

WB06 D08's current-source join is satisfied by actual existing compatible transaction/receipt/expired-lease retry and indexed recovery, now supplemented by WB44's required Snooze/week/current-task lanes. “Exact old-contract compatibility” should be recorded as **preserved current financial contracts; no previously valid instruction made unclaimable by this batch; no specific additional old NEXT repair case identified**. It must not be recorded as “all old obligations absent” or “a generic old-job repair has been built.”

This supports closing the shared **source** joins after the actual WB44 SQL/Worker reviews and source checks, while leaving all app/queue/timing receipts NOT_RUN. It does not complete future WB19's explicit RETRY_WORK_JOB action, enrol legacy-authorised work, certify hosted rows or silently skip a known incompatible persisted contract. If a real older contract is later identified, record its exact immutable identity, former accepted shape, current rejection and canonical owner before implementing bounded repair; no guessed financial authority.

## Current scoped Worker/source hashes

| Source | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-job-step.js` | `53e2f466929e6640de7be4823aebf4979b81510c3de38931d198897e2de2c810` |
| `broker/src/bpay-next-financial-queue.js` | `59bb0fc90f37d3e211f9bcdbf9a17ee3f1ecbfbd6e6d3b94197cf2369ba38e20` |
| `broker/src/bpay-next-background.js` | `e7037e62639d38c9311db907f8cbd7d5d3de9366564addfc416f79a353617572` |
| `broker/src/bpay-next-runtime-rpc.js` | `a6b18ee9667831bf26fd6195250f703594c912ae14172a7aff4ee9871f8771d5` |
| `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` | `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd` |
