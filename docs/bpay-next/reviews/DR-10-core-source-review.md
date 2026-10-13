# DR-10 independent core source review

11 October 2026. Read-only source review of current1358 guarded Create, override scope, public intake/page and Worker files against full DR-10 card274–292, all D01–D11 checklist rows, current BD111 design, EX-01 full card and owner E1 / CHECK-A1 / W13 directions. **Final core source PASS after the correction below; no essential finding remains in this reviewed boundary.** No product/checklist edits, SQL/database calls, compilation, tests, business rows, fixtures, renders, hosted calls or commits were performed.

## P2 — Freeze the accepted same-week policy tuple — CLOSED

Migration1358 adds paye_guard_decision, channel_scope, paye_override_reason, paye_override_actor, reauth_proof_id and paye_guard_scope_sha256. Current private.bpay_next_accepted_choice_guard_v1 in1832:12–18 guards only accepted_choice_descriptor / accepted_choice_command_id for pay_run UPDATE. There is no1358 successor adding the policy tuple to that immutable check.

This is part of the retained accepted identity: guarded Create replay compares stored decision/reason/proof/actor at128–141, override resolver1358:39–46 trusts the stored scope, and seal1358 trusts the stored decision. The new CHECK/FKs enforce shape, not immutability; a shape-valid change would alter the captured policy/audit identity while the accepted descriptor remains frozen. Extend the existing pay_run UPDATE branch to reject changes to those six new operands whenever old.accepted_choice_descriptor is nonnull. Preserve the original pre-cut policy assignment and every other accepted/member guard branch. No new trigger/table is required. Reported directly to root.

## Source findings closed in the implementation

- Original six-argument Create embeds policy before membership; five-argument Create delegates with null policy. Original three-argument date/deadline/run owner remains the initializer. Early accepted replay compares descriptor/command/policy and consumes the **retained** stored scope, with no new query/cut. The second run check repeats that comparison.
- Original Source -> module -> sorted Candidate/Umbrella/Work/revision/Case -> command clock -> run ordering is retained. The fresh guard call at guarded Create:284 follows command clock184 and original context validation. Proof consume follows business locks and fresh native policy scope, before any accepted membership.
- VERIFY_PAYE filters Work before row_number/member numbering in both memory and legacy branches; Cases filter through chosen native channel before insertion. UMBRELLA_ONLY retains PAYE Work with PAYE_EXCLUDED_BY_SAME_WEEK_GUARD before run_selection / work_choice and filters PAYE Case membership. Thus no out-of-channel null-skip Work remains to break native ACK, no new holds occur, and original money/query/segment owners remain.
- Migration extends both actual-old-spelling retention branches with the exact PAYE skip code; ON_HOLD and optional actually retained PAY_ON_HOLD remain. No existing skip row rewrite is present.
- Seal preserves its count/completeness checks and SEALED replay first. New guard is a plain read beneath the original run lock; it takes no new command clock, proof or foreign-run business locks. NONE while guard applies and Verify with unexecuted facts refuse explicitly.
- Override resolver implements an action-only policy preview with null amount/count and PAYE_POLICY, rather than a guessed pre-Draft amount. The complete finite context is native HEADER-qualified; accepted replay requires original actor/command/decision/reason/context/epoch and retained scope. Reason1–500 UTF8 bytes, intent2048 bytes and exact fields are enforced.
- Worker preview qualification/adapter admits only this named additional protected preview. Financial command kinds are not expanded. Preparation action strips a bounded token only for PAYE_GUARD_DECISION, requires it for Verify, rejects token/reason on Umbrella only, verifies real signature/actor/action, records native proof UUID and checks consumed command identity. Configured factor policy is rechecked before first use.
- Existing verifier intentionally permits a signed expired token to reach native proof record/readback; current record1302 distinguishes unconsumed expiry from exact consumed-command replay. Thus the added path preserves expired accepted-receipt recovery without authorizing expired first use.
- Preparation decoded JSON/mutation body stays32768 bytes; GUARD alone has the explicit encoded128KiB query allowance. Unknown mutations retain attempted/timeout outcome flags and identical IDs/body. Eight-second deadline includes proof record/factor check; controller abort is checked before financial dispatch.
- Closed response validation cross-checks original run/command/date/counts and decision-to-channel mapping; GUARD validates single complete row, fact statuses, bounded100 references, counts/context and returned namespace. Public service/admin/NEXT authority and all other public dispatch branches remain.

## Boundaries still pending

Native same-week facts are supporting source, separately reviewed by the guard reviewer. Source has deterministic100-reference overflow refusal and genuine receipt/release predicates; this note does not assert joined financial/concurrency behavior or observed F18 zero. Conditional legacy facts remain native read-only; no adoption/fixture/engine call is authorized. The guard transport's large GET envelope has not been proved end-to-end by this read-only review. Office integration and full42 actual A4 screenshots are not reviewed here.

Keep current unknown-command journal semantics and known-stale Refresh refusal in the subsequent Office implementation. This source receipt does not mark DR-10 built/installed/app-tested.

## Reviewed saved-byte hashes

| File | SHA-256 |
| --- | --- |
| migration1358 paye_same_week_guard.sql | b74faed7a411d96b5df3685181efa692f5cb64b33fb014639d1182a9b2cbe247 |
| repeatable1358 guarded_create_v1.sql | f792447ea95bf34cdb8f2769eb234261bf61d7413a6e102905cceb8ed81a57fd |
| repeatable1358 paye_override_reauth_v1.sql | 1ea45b445cf57e2f2111e231e53e6e334539d76ce84281460c9b5257340855ac |
| repeatable1358 paye_guard_intake_v1.sql | 5777b06b24a9aa1dedad608926a6481665c02043652a842f3af4ab6a48e4750d |
| repeatable1358 paye_same_week_guard_v1.sql supporting source | baf39d20cd7ef0f07a15fa8b1dca8e8f595014f03ccff9ed4335f090484414d8 |
| broker/src/bpay-next-command-rpc.js | 2cca59a13f87fb4eef36d3ac2ad021568306cb698cd1c573398b3ef22115cfa8 |
| broker/src/bpay-next-command.js | 41f47040762e3f1d04a1b8844cb7bf6583c502cfeaa2159141a433295b9e55bc |
| broker/src/bpay-next-preparation.js | 79573bcea2f36c413d4e1de7c2d324c01219cd7fb423b15e2fa80188b3ca339f |
| broker/src/index.js | 2223713ca45cb9a4c4582bdbbf00a00046d498e68d3db0d2cdacdbaf69a30852 |
| design/DR-10-design.md | 28997659255e643ac979a42b0fb0140bd95c9df2142fd058977e0f1a94df4829 |

Physical SQL paths are supabase/migrations or supabase/repeatable with full11102026_1358_banking_pay_next_ prefix. These hashes precede the requested immutable-policy guard correction.

## Fresh source closure

Root repaired the existing accepted_choice_guard in guardedCreate1358:6–49. The pay_run UPDATE condition now freezes all six policy audit operands alongside the original descriptor/command **only after old.accepted_choice_descriptor is nonnull**. Thus the original pre-cut policy assignment remains valid; the first accepted descriptor freezes it; later changes refuse. Other member/exclusion/actor branches, owner postgres and exact owner-only revoke are preserved. Registry advances only that exact named guard to the same1358 physical source. The P2 finding above is superseded and closed.

Reviewed the additional Worker workbench delta: exact SQLSTATE23514 / CHOICE_CONTEXT_CHANGED is decoded only for a failed native HEADER with nonnull count_selection, returning known409, retryable false, reload_required true. The finite failure-code list adds only that code; other read kinds and unknown/mutation behavior are unchanged. This is the owner A4 explicit Refresh seam, not an automatic retry.

Static source comparison confirms the entire original other-action reauth scope tail remains unchanged after the added policy branch. Other intake dispatch differs from0814 only by explicit pg_catalog qualification, apart from the reviewed Create/policy authority/constructor and GUARD additions. No original financial formula owner is replaced.

Final core hashes supersede the early table where different:

| File | SHA-256 |
| --- | --- |
| repeatable1358 guarded_create_v1.sql, including repaired accepted_choice_guard | 6d13cd0a482be586e49aaeb2fb67bca82613aa092657cbcc208e6f012acbc0a6 |
| broker/src/bpay-next-workbench.js | f497b65f0ea0c8285c5daa251695a373febcb5e9119b7630570c0f62b6dc6437 |
| docs/bpay-next/final-definer-closures.json | 102fd50dc2a9db54fb9c023fd55d32de9248e232567ac05ae1f754eca17536bd |
| supporting repeatable1358 paye_same_week_guard_v1.sql, separately owned guard review | 5982feb781685d84929f15403dcace89e7ad02bc22131fc96d823615986a2a23 |

The three core repeatables and original four Worker files retain the other hashes listed above. Guard-fact source has changed under its independent reviewer; the supporting hash records current bytes and does not supersede that review. Final SOURCE PASS applies to this core membership/replay/proof/clock/lock/closed-schema boundary only. Office, A4 and joined runtime acceptance remain pending / NOT_RUN.

## Final preview decoder / preserved-byte amendment

Fresh read of the small post-review Worker correction: readBpayNextReauthJson now accepts optional action=null. Only the preview caller passes call.action. A failed native response is promoted to the known409 CHOICE_CONTEXT_CHANGED refusal only for PAYE_SAME_WEEK_OVERRIDE plus exact SQLSTATE23514 and exact BPAY_NEXT_CHOICE_CONTEXT_CHANGED. Other actions, proof-record callers and financial dispatch retain the original default behavior; successful payload validation is unchanged. The fault defaults preserve retryable=false / outcome_unknown=false. This makes the owner A4 explicit Refresh refusal reachable from policy preview without widening the protected command/action schemas or converting a financial unknown into a known result.

Root restored original unchanged Worker line endings. Fresh raw-byte hashes supersede earlier Worker hashes:

| File | SHA-256 |
| --- | --- |
| broker/src/bpay-next-command.js | d5c3b15437494a287ec38db7f8776a9cf708b72e77f69e5a6ca31046e9eea5b3 |
| broker/src/bpay-next-preparation.js | d8e678c400a4ea39a63cd67a75f16562116b70babbcf748435aa2b7ac2e072b7 |
| broker/src/index.js | adc099141fc57f0ab97058eea716744dc76068ce2244f24a234ae4f6b3c3745b |
| broker/src/bpay-next-command-rpc.js | 2cca59a13f87fb4eef36d3ac2ad021568306cb698cd1c573398b3ef22115cfa8 |
| broker/src/bpay-next-workbench.js | f497b65f0ea0c8285c5daa251695a373febcb5e9119b7630570c0f62b6dc6437 |

Final core SOURCE PASS remains. No tests, SQL/database calls or product edits were made by this reviewer.
