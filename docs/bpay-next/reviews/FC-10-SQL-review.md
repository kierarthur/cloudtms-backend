# FC-10 SQL source review

11 October 2026. Independent read-only review of the actual 2041 migration/repeatable, current1939 parents, BD120, registry, native0904 fence and1509 protection guard. **SOURCE PASS for this SQL batch. No essential source finding remains open.** This does not pass Worker/Office, A4, joined behavior, timing or the FC-10 item. No SQL, test, fixture, business or hosted action was executed by this reviewer.

## Current reviewed bytes

| File | SHA256 |
| --- | --- |
| supabase/migrations/11102026_2041_banking_pay_next_case_pause_request.sql | 07CEA164D11DF092FB5FC18EDC581EF4E5EE4776A3D0D27167F07BDB531DC344 |
| supabase/repeatable/11102026_2041_banking_pay_next_case_pause_v1.sql | 80675B1F77AF852977DA427D0CE0702BFB2A2DA6FAFCEA2A5E05B731810DE44B |
| docs/bpay-next/design/FC-10-builder-design.md | 8FA9AA9123F2481102AE94C228AA78429CB2BB89477CF00BCC4FF9F308C72B60 |
| docs/bpay-next/final-definer-closures.json | C4C498E15DA29443C51341A37E0184E2F98C8BA7B035FC75E2BF195986AEEB0A |
| supabase/repeatable/11102026_1939_banking_pay_next_case_edit_v1.sql | 7B723A61096AC50853DBC609ACBBA6437B403F11AA796A6F77E39A78B8308F66 |

The actual ten definitions are five new private state/receipt/accept/apply/claim owners and five complete successors for financialIntake/enrollOne/financialJob/preparationPage/componentPage. Registry finalRoot/finalPhysical/closure entries agree with those actual definitions. The shared economic plan and prepare owners remain1939. Native early fence helper remains0904; 0910 is the retained expiry alias, not a replacement fence implementation.

## Findings and closure

One initial compile hazard was reported: the admission IF at repeatable2041:108 had an unparenthesized CASE operand. Current saved source now uses `c.status<>(case ... end)`. This closes the same PL/pgSQL parser hazard previously observed on FC09. Root reports all ten definitions compiled on the sole local PG17 instance and the lock-recorded migration was installed. That is parent-reported compile-only evidence, not reviewer execution or a business-flow result. No other source defect was found in the reviewed batch.

## Typed storage, null safety and identity

Migration2041:8–27 binds the immutable request to the exact native command/sequence and member plus Case/Candidate composite FKs, actor, module epoch, expected revision and OPEN→PAUSED or PAUSED→OPEN action/status alternatives. The expected revision is positive and less than BIGINT_MAX, so applied expected+1 is representable. Nullable reason is deliberately permitted; nonnull reason is nonblank and at most2048 bytes. The original JSON object is retained under16384 bytes. No source/capacity or captured-selection operands are introduced.

Outcome storage2041:31–55 binds request identity/action and native job tuple, with a unique job UUID and request-keyed single terminal record. APPLIED requires a nonnull positive applied revision, nonnull action-correct status and null issue; BLOCKED requires **both applied fields null** and a nonnull issue from the six closed codes. These explicit nonnull branches prevent CHECK UNKNOWN from accepting an incomplete APPLIED or BLOCKED shape. Nullable protection facts are object/byte constrained and are produced from native helpers rather than browser input.

The separate job_id FK and `(command_id,candidate_id,action)` job FK do not alone express equality between those two referenced rows. That equality is explicitly and fail-closed checked by the sole receipt owner: repeatable2041:48–56 resolves the unique exact command/Candidate/action job and requires `outcome.job_id=job.id`, exact Case/Candidate/action, DONE/DONE/null cursor, issue equality, job sequence/module and COMPLETE/FAILED command status. It also verifies APPLIED target status and expected+1 revision against the immutable request. An inconsistent native row cannot be reported as a successful receipt. New tables grant no browser/service direct access; the actual writer always inserts its own scoped p_job_id at2041:162–166. No new tuple migration or broad guard rewrite is necessary for this batch.

Terminal absence is not mistaken for success: receipt2041:58–59 refuses a DONE job or terminal command without the immutable outcome. PENDING before enrollment is legitimate and keeps all terminal fields null. Exact Case/Candidate receipt lookup2041:42–43 refuses a foreign command. Admission replay2041:95–101 compares Candidate, Case, actor, action, module epoch and complete retained original_args, and revalidates that receipt before returning original accepted identity. It does not evaluate current Case status/protection on exact replay.

Request/outcome update, delete and truncate are denied by four narrow private triggers2041:1954–1965, reusing original effect_immutable. Migration tables and all five private owners are owned by the original logical owner with public/anon/authenticated/service_role revoked. The three original service RPCs retain only service_role EXECUTE (repeatable:1931–1953). No browser table exposure or public trigger was added.

## Factual admission and authoritative ordered application

Admission2041:74–86 closes exactly four input keys, UUIDs, positive bounded string revision, required-nullable reason and request ceiling. It derives Candidate from actual native Case; no CASE_CREATE receipt is required. Module/admin/control qualification2041:87–94 precedes replay and fresh Case/component locks2041:103–104. Fresh expected/status/supported checks2041:105–109 remain factual. It uses FUTURE_ONLY state, accepts only a one-member SEALED **status intent**2041:110–121 and changes no Case status or financial amount. This matches updated BD120's explicit admission decision; the later WHOLE_CASE refusal is retained as an original immutable outcome rather than a new Worker error envelope.

State2041:15–31 identifies actual RECOVERY/DEDUCTION membership, including genuine automatic Overpayment and unfunded Advance; CLOSED/WRITTEN_OFF or credit-only composition is unsupported. It uses existing1509 FUTURE_ONLY/RECOVERY full summary, preserves the native balance-invalid issue, and separately reads at most one actual native protected address. It does not apply FC09's preactivity edit predicate to funded recovery. No guessed outstanding threshold controls eligibility.

Enrollment2041:765–782 creates one READY/NEW native CASE_PAUSE or CASE_RESUME job for the exact immutable request/action/module/sequence/member. Claim2041:185–209 keeps module→Candidate worker_control→job ordering, current module, NEW/null cursor, available_at, READY or expired LEASED, lower-sequence predecessor exclusion, exact request and a new short lease/owner epoch. Active leases are never adopted.

Apply2041:131–145 locks module, exact Candidate control, own job and immutable request; checks kind/module/sequence; performs DONE/DONE immutable receipt replay **before** fresh lease, fence and lifecycle checks. Fresh operation proves leased NEW/null cursor, own nonce/epoch, control owner and unexpired deadline. The actual0904 financial fence is called with the original own job/lease/owner **before Case/components or status mutation**. It is outside the known lifecycle exception block.

Under Case and ordered component UPDATE locks2041:147–153, apply obtains current state, expected revision, original prior status and native recovery support, then calls unchanged1509 **WHOLE_CASE** with null amount and RECOVERY purpose. Any PENDING or active HOLD protection refuses, including a protected payout in the same Loan/Advance Case. Only an unprotected valid Case updates status, case_revision and timestamp2041:154. Candidate financial_view_revision increments once before unchanged WB08 refresh2041:155–156. No Rule, component balance, principal, period, event, hold, accepted instruction, captured status or result is rewritten.

The finite six-code check_violation handler2041:157–160 rolls back the status subtransaction, then stores BLOCKED with null applied fields and original refusal facts2041:162–166. Success stores APPLIED. Outcome, job DONE/DONE, command COMPLETE/FAILED and receipt proof commit atomically2041:162–170. Any malformed identity/lease, unexpected check, infrastructure failure, response-size error or failure inserting the outcome aborts the step rather than manufacturing BLOCKED.

55P03 is deliberately not caught. Original0904:69–71 financial-fence contention remains retry ownership. Existing job-step:386–387 maps this to retryable transaction retry, and financial-queue:67–82 retries before permanent failure handling; no exception-laundering or new financial REVIEW phase appears in SQL. No client or provider execution occurs in this batch.

## Protection summary and first address

The whole summary is existing1509:259–285: complete native protected count and sum, with amount/capacity null when any PENDING amount is unknown. The first ref is a separate bounded helper read2041:25–28: actual run/worker/channel/status, its own nullable protected amount and genuine next action. The first ref's amount is **not** the Case-wide total; consumers must display the summary total and describe multiple bindings as protection in Drafts. No date/PB label or guessed identity was added.

For the ordered writer, state summary, first ref and WHOLE_CASE assertion execute under the same Candidate/control plus Case/component locks2041:134–153. Current Create takes conflicting native component SHARE locks before it inserts immutable CASE members (`11102026_1358_banking_pay_next_guarded_create_v1.sql:219–225`) and refuses existing non-DONE Candidate jobs (`:226–229`). Current hold/release money owners use the original serialization/Case/component locks. Thus a valid new accepted choice or native hold transition cannot interleave between these writer reads. On the WHOLE_CASE protection exception, PL/pgSQL local state remains available after subtransaction rollback and its native summary/first ref is frozen into the terminal outcome2041:166. Pending amount remains null; no asserted zero or invented first-ref total escapes.

The standalone read-only state is observational, and its summary and first-ref reads are separate statements. It is not an atomic accepted authority snapshot or money certification. A subsequent Refresh can observe a newer accepted/removed Draft; only the ordered writer plus immutable outcome proves application or refusal. The actual Worker/Office review must preserve this distinction and validate the same nullable closed shapes. This does not require a new token, signing scheme, store or reader kind.

## Complete retained owners, bounded readback and economic preservation

A read-only function-by-function comparison of all five shared definitions against complete1939 parents found only the scoped changes:

1. Intake adds two actions delegating to the new accept owner; original permission/module/proof/money branches remain byte-identical.
2. Enrollment adds the exact two native request/job branches; original sequence and other command handling are unchanged.
3. Financial job adds two kinds, single-step NEW/null-cursor admission, own claim/apply owner, the finite claim-race request proof and exact post-owner receipt equality. Its output includes the original immutable receipt, with the existing response budget. All unrelated lease/cursor/money branches are unchanged.
4. PreparationPage adds optional qualified pause_command_id alongside edit_command_id; native receipt lookup and row attachment; final120000-byte guard after either optional receipt is attached2041:1691–1711.
5. ComponentPage computes pause_state once for the requested native Case, carries it as nullable control metadata and pause_receipt placeholder, and preserves all current1939 component/schedule/amount/reference fields. Native limit1–100, ordinal seek and120000-byte row-page mechanics remain2041:1861–1928.

The registry does not move the1939 plan/prepare,1509 assertion,0904 fence or0559 Case projection. Existing1939 plan's immutable accepted status/rule cut and live locked balances are retained; PAUSED collection skip remains unchanged. No original creation/amendment/funding or cancellation owner was replaced. The only actual status write is2041:154 and the only new financial-view invalidation is2041:155. Native money events stay unchanged. This closes SQL scope without early FC14, FC02, release, cancellation, resolution, query producer or new allocation work.

## Review boundary

SOURCE PASS is for these exact SQL bytes and registry only. Root-reported sole-local PG17 compilation is compile-only evidence. This reviewer did not inspect a database catalogue or run SQL; installed parity/source gates/export/coupling remain separate root receipts. Worker/Office closed schemas, journal/UI behavior, A4 screenshots, queue execution, protected/refusal/application business journeys, joined acceptance and performance are **NOT_RUN by this reviewer** and are not implied by this source result.
