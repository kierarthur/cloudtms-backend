# DR-18 SQL and runtime enum source review

12 October 2026. Independent read-only review of BD138, the full DR-18 card/checklist scope and the saved 0645 batch. **SOURCE PASS for this SQL/runtime slice: no essential source defect found.** This is not an item, UI, installation, performance or joined application acceptance result. The reviewer executed no SQL, business RPC, test, fixture, browser or hosted action. Construction of the native confirmation/success flow remains a separate review boundary.

## Reviewed bytes and actual owners

SHA-256 of the saved files at review:

| File | SHA-256 |
| --- | --- |
| `supabase/migrations/12102026_0645_banking_pay_next_confirm_refresh_membership.sql` | `28785F633847C5FECFD937793D8CDC841299F7847975DA779B8019A89A345763` |
| `supabase/repeatable/12102026_0645_banking_pay_next_confirm_refresh_v1.sql` | `5C1E24129CF8A14C97C80D4A7027EAF7F434BFFFEC2FF58872A159FE824552A1` |
| `broker/src/bpay-next-runtime-rpc.js` | `EB9FD85C7F6A6F7A4FC0F34BACAA069826E20D4B7943A218D39DA0069F0117C9` |
| `docs/bpay-next/design/DR-18-build-scope.md` | `388DB6C5940D3F8E821AAE6B42D9F2FE280FF9A9248E5DEDE924AAA9685F2A21` |

The current final-definer registry names the four exact 0645 successors: `private.bpay_next_confirm_simple_run_v1(uuid,bigint)`, `private.bpay_next_channel_drafts_page_v1(uuid)`, `private.bpay_next_workbench_step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer)` and **`public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb)`**. The final public name is runtime_service, not service. Its existing signature and browser/service ACL boundary remain unchanged (0645:571–580).

## Complete parent preservation

Static ordered full-function comparison against 0847, 0236 and 1429 found only the intended additions/replacement branches. Confirmation retains every original input, module, header/review revision, discard, expiry, 72-hour real-clock, sealed command, enrollment, original PREPARE job and typed completed worker RELEASE guard (0645:17–75; parent 0847:1085–1166). It adds one task UUID declaration, the truthful all-released refusal, the task/member inserts and global barrier increment. It does not recapture or reprice money.

The 0236 channel reader retains its input, confirmed/SEALED/status qualification, PAYE-before-Umbrella ordering, result shape and one-to-two-address guard. Only its native existence predicate gains the exact completed pre-confirm RELEASE exclusion (0645:130–145). The full 1429 STEP body differs only in the two member-discovery branches, exact CONFIRM binding validation and two projection branches. The full 1429 public runtime body differs only in the old-module CONFIRM task recovery branch. Other task kinds, DUE selection, CLAIM, error handling and request parsing are retained.

## Atomic native membership and security

Confirmation still locks the original Run first. After all original guards and the READY existence check, it snapshots only READY workers' native run_work, exact captured **economic** Case instructions and genuine Work collection-linked Cases (0645:76–98). Case instruction joins bind Run, worker, Candidate, `w.captured_preparation_revision` and `w.case_selection_revision`; they do not substitute the active execution revision. UNION/Work DISTINCT plus the member primary key eliminate duplicate scope. Collection membership is the actual `work_id` point relation; its original composite Work/Candidate and Case/Candidate FKs preserve common ownership (1519 migration:38–46). No later mutable worker-status traversal determines the task membership.

Task creation, immutable UUID members, the single global revision/pending increment and the DRAFT header transition share one transaction (0645:81–107). Exact completed DRAFT replay returns before any task insert or pending increment (29–34). There is no per-Work scheduling loop, money payload, Source/ordinary channel split, Candidate freshness check or invented Candidate dirty debt at Confirm.

The migration preserves original non-CONFIRM scope predicates and adds a disjoint NULL-safe `IS TRUE` CONFIRM scope, positive review revision and unique Run/task association (migration:8–34). Member rows have a closed WORK/CASE union, non-null PK operands, matching native UUID/FK column, composite CONFIRM task FK and `IS TRUE` identity check (36–46). Owner-only forced RLS and denied PUBLIC/browser/service table grants are explicit (48–53). Existing immutable guard rejects UPDATE, DELETE and TRUNCATE (54–57; guard 1342:188–200). Actual inserts remain solely in the trusted original confirm writer; no caller supplies membership.

## Bounded projection, lease and recovery

Each phase seeks the immutable `(task_id,member_kind,native_id)` primary key with a UUID cursor and limit 1–100 (0645:163–180). Plain discovery precedes whole-page ordered Candidate, Work and Case preholds, which precede the task lock (228–242). Exact module, nonce, owner epoch, generation, step, phase/cursor, discovered checkpoint and real lease deadline are revalidated (243–249); the confirmed Run/SEALED/original review binding is then checked (250–255).

Work pages call the existing core Work projection; Case pages call the existing Case projection after their required preholds (299–312). The core call adds no late related-Case locking. The separate Case membership includes collection-origin Cases. Page-size changes preserve UUID order; a full final page requires a subsequent empty seek, so no member is silently skipped. Projection/checkpoint changes share the original subtransaction, with the inherited SQLSTATE ERROR/lease-clearing path retained (408–425).

The unchanged 1429 finish helper handles `candidate_id IS NULL`, advances the global revision and decrements pending once; its DONE early return prevents a second debit (1429:245–265). Existing Workbench readers therefore retain the global pending barrier without a Candidate continuation counter. The new WAKE owner-epoch branch changes epoch/generation/lease status on this same task while retaining native members, phase, cursor, step and counters (0645:530–540). It neither adopts an active unexpired lease nor resets progress. The original bounded wake/claim/recovery system and queue identity remain in use.

## Channel and finite transport fidelity

The completed RELEASE predicate binds both preparation revisions and completed expected/released Work and Case counts (0645:136–143). Current action admission refuses a new worker action after `confirmed_at_utc` is set (2259:2739–2753); the current request guard makes completed receipts immutable (0814:3873–3895). This distinguishes a pre-confirm removed worker from later execution/channel cancellation without a new channel store or a positive-cash inference. Zero-cash READY workers retain their factual channel address. All-released confirmation raises a known 55000 refusal before any task/header write; existing preparation transport maps that rollback to definite 409 NOT_ELIGIBLE (preparation.js:912).

The Worker diff contains exactly two finite enum extensions: phases `CONFIRM_WORK`/`CONFIRM_CASE` and task type CONFIRM with those phases/DONE (runtime-rpc.js:53–54,85–86). Closed request/result keys, byte/row limits, nonce/checkpoint validation, stale and error semantics remain unchanged (55–115). Existing public SECURITY DEFINER membership increases by zero. Root's permitted source/compile/catalogue/isolation checks and native UI review are still required; no runtime outcome is inferred from this source receipt.
