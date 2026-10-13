# Owner 11 October 2026 — A1/A7 investigation

Read-only source investigation of the two exact owner corrections. Root alone changes product and obtains any fresh catalogue/stored-row count. No SQL/database call, tests, fixtures, hosted/browser action, execution or checklist edit was performed here. Source findings do not certify installation or application behavior.

## A1 — accepted-choice skip CHECK must admit the current held literal

The original membership migration `supabase/migrations/10102026_1832_banking_pay_next_accepted_choice_membership_v1.sql:21–45` defines private `bpay_next_run_choice_member`. Its inline `skip_code` CHECK at `:32–36` admits exactly:

`BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE`, `BPAY_NEXT_SELECTION_SNOOZED`, `BPAY_NEXT_SELECTION_PAY_ON_HOLD`, `BPAY_NEXT_SELECTION_WORK_NOT_APPROVED`, `BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT`, `BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE`, `SOURCE_PAY_QUERY_OPEN`, `SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE`.

It omits **`BPAY_NEXT_SELECTION_ON_HOLD`**, which the current registered owner actually writes:

| Current owner / consumer | Exact source evidence |
| --- | --- |
| `private.bpay_next_append_work_skip_v1(uuid,uuid,uuid)` | `11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql:137–140` returns ON_HOLD for the native current TF hold. Accepted acknowledgements instead reuse the original stored skip at `:111–119`. |
| `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid)` | Same file `:523–527` assigns ON_HOLD; `:572–574` writes that value into `run_choice_member.skip_code`. The failed CHECK therefore aborts this real accepted-Create path. |
| Worker decoder | `broker/src/bpay-next-preparation.js:188` already admits both ON_HOLD and PAY_ON_HOLD; its original membership row validation is `:425`. |
| Office decoder | BUILD-FE `js/bpay-next-office.js:219` already admits both spellings; original membership shape/recovery validation remains at `:514,584,722`. |

The CHECK was declared without an explicit name. PostgreSQL's expected generated name is **`bpay_next_run_choice_member_skip_code_check`**. Root subsequently reports its fresh localhost catalogue confirmed that exact name and **zero stored rows with `BPAY_NEXT_SELECTION_PAY_ON_HOLD`**. This reviewer did not issue or observe the database query independently. No later saved migration replacing this CHECK was found in the original investigated snapshot.

**Owner A1 correction governs the minimum:** one dated migration replaces that exact CHECK and always admits ON_HOLD. Retain PAY_ON_HOLD in the CHECK **only if actual stored rows still carry it**, with no stored-row rewrite; root's reported zero-row local state therefore requires a CHECK without the old spelling. Preserve every other original literal, NULL permission and the separate CASE_COMPONENT shape requiring NULL skip. Remove PAY_ON_HOLD from both current Worker/Office decoders. Current0426 writers already emit ON_HOLD; immutable installed predecessors remain provenance and are not rewritten. No money calculation or capture-time hold behavior changes. Decoder-only `PAYE_EXCLUDED_BY_SAME_WEEK_GUARD` is not written by these accepted-choice owners and does not justify widening this CHECK.

The earlier inference to preserve PAY_ON_HOLD unconditionally was incorrect under this explicit owner instruction and is superseded by this paragraph. The decoder table above records the pre-correction source and explains the exact two removals required; it is not the target allowlist.

Existing source-gate insertion is `scripts/bpay-next-source-gates.mjs`: the fixed queue-schema assertion ends at `:1942`; `verifyBpayNextSourceGates` starts at `:1944`, with the queue assertion at `:1945` and final-definer verification at `:1946`. The existing final-routine inventory is available at `:1949–1958`. A small finite accepted-choice assertion can run here and check the effective replacement CHECK's exact current literals and owner-required conditional legacy branch against the registered Create/append-skip owner literals and the corrected two decoders. Verify both current writer/decoder sets omit the old spelling; historical immutable predecessor occurrences are not current writer authority. Reuse `read`, `scanManagedSql` / `routineDefinitions` if selecting the two actual bodies. Pin the one actual membership table/constraint and current migration path; do not inspect unrelated tables or admit generic CHECK/catalogue/dynamic-SQL exceptions. This is a proposed insertion only; no gate was written or run by this reviewer.

## A7 — reuse the already-built early fence in the current expiry writer

Saved scope remains early DR-37 **D14/D18 only** (`docs/bpay-next/OWNER-ASSESSMENT-CORRECTIONS-20261010.md:5`). Full DR-37 card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:716–737`, checklist D14/D18 and R2 §2:35–38 name the Candidate-wide shared-capacity fence and the expiry writer's single assertion hook. `check/R3_FENCE_DEPENDENCY_EXCEPTION.json:29–46` explains the early prerequisite; it does not authorise early replacement machinery.

Actual current final registry:

| Exact signature | Current final source |
| --- | --- |
| `private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint)` | `11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql:4–75` |
| `private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)` | `11102026_0910_banking_pay_next_fence_expiry_row_projection_v1.sql:4–236` |

D14 migration `11102026_0904_banking_pay_next_early_candidate_financial_fence.sql:4–30` already supplies the native Candidate/week PK, exact original command/job/worker/allocation-state FKs, private owner ACL/RLS and unique active Candidate partial index `:22–24`. It adds no acquisition/replacement producer. D18 checks the active Candidate fence through that indexed native predicate (`0904:24–28`); exact NET request/job/state/selection/economic identity is checked at `:30–60`. Only the same live NET lease/owner/module tuple can pass an active fence (`:63–68`); every other owner receives named `55P03 BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY` (`:69–71`). The absence of an active fence returns without changing money. Candidate-wide admission covers shared capacity across weeks.

The final DR-08 expiry writer **already consumes the required contract completely for this hook**:

- original module → Run → Candidate → job → binding/worker admission locks retain NOWAIT (`0910:22–29`);
- exact command/root/deadline/count/review binding, terminal EXPIRED replay, live ordered lease and stale/ahead checkpoint handling remain before the assertion (`:30–51`);
- the single `perform ...financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch)` is at **`:52`**, before the first mutation at `:70`, binding/job publication at `:73–74`, hold release, projection or final/count mutations;
- original Work-before-Case release and bounded native projection/ORIGIN continuation remain (`:109–200`); worker-week expiry/count/header/command completion remains `:211–230`. No foreign-fence failure is caught and converted to financial REVIEW;
- existing Worker runtime SQLSTATE mapping makes `55P03` retryable `TRANSACTION_RETRY` (`broker/src/bpay-next-runtime-rpc.js:303`). This is source mapping evidence, not a contention/runtime test.

0910 is the five-row-alias successor of immutable 0904, already independently compared in `reviews/DR-11-allocation-review.md:36–55`. It retains the original DR-08 financial, checkpoint, 72-hour accepted expiry/confirmation and projection behavior. Root subsequently reports fresh installed-definition parity for the reused current owner; that is root's separate database evidence, not execution by this reviewer. **A7 needs a reuse/evidence correction, not another expiry definer or financial change.** This does not close future DR-37 writers, D12/D17 replacement/high-water machinery, full DR-37, or F-26 (sequence280). Do not add them at this boundary.

## Source identities and boundary

| Saved source | SHA256 |
| --- | --- |
| original membership migration1832 | `E21C27EC2D19E912330C652E6096036EACDC6396A476FD363826AD0C72AD6747` |
| accepted-choice owner0426 | `AEC4C904CEE22E748EA1AB3006E8CFDBF50BCDB9DD567B1FB1B2BC9DA7391B92` |
| D14 migration0904 | `FBCD4A69AAC8E14389C371313E25E2E8839E152603F7E30522A0A129740F145B` |
| D18/writer0904 | `FA3C73A83075F67246E62ADEA7BA1467C019BADA601D3DB880EC51B48CDD1A9E` |
| current expiry0910 | `14803CD993EF6BB7E3C11835E73AD2219520CE5F1DCF55B0DABDB74088D9C654` |
| source-gate insertion context | `03A520A156621D59EE33CEC80A9A9F6C25E2E712F8924D8453878F79FCE1FC7C` |

A1 is a proved source gap pending root's narrow correction. A7's required current expiry hook is present and reusable. Stored-row counts, installed catalogue/definition parity, compilation, source-control execution and all application acceptance remain **NOT_RUN by this reviewer**.
