# FC-11 SQL source review — sequence 132

12 October 2026. **SOURCE PASS for the five-owner SQL slice below.** No essential source defect remains in the reviewed saved bytes. This is not whole-item or business acceptance. The reviewer performed saved-file reads and parent-body comparisons only; SQL execution, migration catalogue matching, runtime first use, business data, hosted calls and joined journeys are **NOT_RUN** by this reviewer. Root retains product ownership.

## Reviewed authority and bytes

Authority: approved plan `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1238–1266`, all FC-11 ITEM/D01–D15 checklist rows, `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md` sequence 132, and current `docs/bpay-next/design/FC-11.md` (BD146). The investigation and current-parent census are in `docs/bpay-next/design/FC-11-investigation.md`.

| Saved source | SHA-256 |
|---|---|
| `supabase/repeatable/12102026_1444_banking_pay_next_case_write_off_v2.sql` | `0a3936fbfcd31e9fe327df2af40f1bf996d78aa01a48ae753b2765e294cf0e66` |
| `supabase/migrations/12102026_1444_banking_pay_next_case_write_off_any_debt.sql` | `cf1c19e9972d9e66d595066dafecddc80e61a231e7c1f0f3e595224311f96438` |
| `docs/bpay-next/design/FC-11.md` | `7b7f2e9dc53a5b13b88e6fbb2f066e80fd41d102e66eaf6d90a3b81e91d14012` |

The current prototype contains exactly five existing private owners. The initially proposed state helper and CASE_COMPONENTS successor have been removed. Existing `11102026_2041_banking_pay_next_case_pause_v1.sql:10–31,1861–1930` already exposes component outstanding/revision and `pause_state`, whose FC-16 FUTURE_ONLY request explicitly uses RECOVERY. This reuses the purpose-specific protection summary and first native binding without an extra reader or money calculation.

## Full-current-parent preservation

Complete function bodies were compared with their actual current parents, not historical overlays. References in this table are to the reviewed 1444 repeatable.

| Existing signature | Actual parent | Reviewed delta |
|---|---|---|
| `private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text)` | `04102026_1647_banking_pay_next_work_collection_write_off_v1.sql` | Lines 54 and 65 add positive typed NULL-origin eligibility and funded/approved principal selection; replay, permission, original locks and command sealing remain. |
| `private.bpay_next_apply_write_off_v1(uuid,uuid,bigint)` | `11102026_1509_banking_pay_next_case_change_guard_v1.sql:296–388` | Lines 98,109–126,153 choose nullable origin, consistent debt principal and exact component event identity. Existing DR-37 fence, FC-16 assertion, mutation and completion remain. |
| `private.bpay_next_write_off_guard_v1()` | `04102026_1647_banking_pay_next_work_collection_write_off_v1.sql` | Lines 187–192,210–240,263–273 enforce the same nullable identity and principal across immutable request, result, event and both counter tuples. Six point-read aliases are qualified without changing predicates. |
| `private.bpay_next_reauth_scope_v1(uuid,text,jsonb,boolean)` | `11102026_1358_banking_pay_next_paye_override_reauth_v1.sql` | WRITE_OFF-only changes at 376,406,409,412: SOURCE_DEBT label, typed eligibility and exact funded/approved native preview. Other protected action branches and original proof replay are unchanged. |
| `private.bpay_next_segment_write_off_membership_v1()` | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4788–4828` | Line 601 returns for an exact request with NULL Work origin before the Work-only STRICT lookup. The non-NULL Work segment attribution body is otherwise retained. |

The original receipt, enrollment, job dispatch, owner/lease predicate, deferred completion guard, strict Work-origin helper, source/ordinary producers, allocation/transfer/settlement owners and public command intake are reused. No new public function or public table grant is introduced. Lines 631–640 retain explicit existing-private owner/ACL treatment; line 641 requests the native schema reload.

## Identity, principal and event closure

The new NULL-origin branch is limited to RECOVERY/DEDUCTION components of LOAN, ADVANCE or MANUAL_DEBT. It requires matching Case, Candidate, case kind, current rule and all approved/funded/recovered/written-off/active-recovery counters using `IS NOT DISTINCT FROM`, then positively tests the entire predicate with `IS NOT TRUE` (54,126,188,406). It does not admit an arbitrary unbound OVERPAYMENT, payout or credit. Non-NULL origins still use the unchanged strict `bpay_next_work_collection_origin_v1` contract.

Debt principal P is **funded** principal for LOAN/ADVANCE and **approved** principal for MANUAL_DEBT/original Work debt. This choice is applied consistently to captured request basis (65,190–192), apply balance and Case/component comparison (116–120), terminal result (210–214), Case/component counter proofs (263–273), terminal status (268–269) and fresh EX-01 preview (409–413). Native source constraints in `04102026_0300_banking_pay_next_case_instruction_core.sql:84–90,107–121,476–481` and the paired funding updates in `04102026_0410_banking_pay_next_case_transfer_post_v1.sql:456–485` support that distinction. An unfunded Advance has P=0 and reaches the existing NO_BALANCE result; it does not create a write-off event.

All event item locations use `coalesce(work_collection_id,case_component_id)`, including terminal replay (98), creation (153), result proof (218) and BEFORE-event proof (231). For a native Case with no Work origin, Case and component UUIDs remain distinct and the component UUID is the operation item. The existing Work identity remains its collection UUID. Positive written-off delta is the only changed economic counter: no recovered, funded, approved, credit-paid, rule or schedule reset is allowed (232–240,266–276). BLOCKED/REVIEW may not have a write-off event (222–223), and the same event cannot increment either counter a second time (244–277).

## Ordered mutation, protection and replay

Acceptance preserves original exact command-ID replay before new Candidate/Case/component admission (33–45). It captures the original operands rather than repricing a changed request. Application proves terminal replay first (92–102), then the original exact leased job/nonce/owner association (103–107), then the current DR-37 financial fence (108). Work collection/position locking remains conditional on a real Work origin (109–112), followed by the unchanged Case/component lock order (113–114).

Changed component/Case revisions remain retained REVIEW outcomes (123–128). ALL requires the native WHOLE_CASE assertion; AMOUNT requires UNRESERVED_AMOUNT with RECOVERY purpose (140–142). Only the two existing named check failures become INPUT_PENDING or PROTECTED_CAPACITY (143–148); unrelated integrity errors rethrow. Active holds and accepted pending slices are not released. The write-off event and paired W updates precede the existing financial-view revision bump and WB08 Case refresh (152–162), followed by the original immutable result/date and job/command terminalization (164–169).

EX-01 first-use scope remains native and locked when `p_lock=true` (385–394). Accepted retry compares the original actor, component revision, scope, raw amount and reason, uses the original request principal/recovered/written-off amounts (395–403), and retrieves the original consumed proof scope (572–580). The existing proof mechanism is reused; there is no new proof store or token protocol. Existing public intake still consumes proof before accepting WRITE_OFF in the same transaction (`12102026_1215_banking_pay_next_case_net_replacement_v1.sql:4514–4536`).

## Migration and existing readback

Migration 1444 changes only the existing request origin. The final saved migration point-reads the exact native constraint name `bpay_next_write_off_request_check` and requires `pg_get_constraintdef` to equal `CHECK (((work_collection_id = case_id) AND (work_collection_id = case_component_id)))` (6–10). Missing or different evidence fails closed through `IS DISTINCT FROM`. The following DROP is static (13); `work_collection_id` becomes nullable (14); the replacement CHECK retains the old equal-identity rule whenever origin is non-NULL (15–16). Existing FKs, money domains, terminal constraints and installed 1647 rows are preserved. There is no catalogue aggregate or dynamic SQL. Root reports a fresh local catalogue match for the exact predecessor name/definition; the reviewer did not query the database or perform compilation.

The existing Candidate-scoped COMMAND/WRITE_OFF history path supports native Case requests without a Work origin. Its Work join applies only when a Work/Timesheet scope is requested (`10102026_0653_banking_pay_next_ps_c16_indexed_history_v1.sql:271–273,292–293`). Its retained WRITE_OFF fields include status, original operands, native amounts, issue and `completed_at_utc` (section declaration at 115). This supplies historical terminal date and same-ID result readback without a new preparation reader or a browser financial step.

## Remaining item boundary

SQL SOURCE PASS covers the migration and five owners at the hashes above. Root still owns registry/install/catalogue evidence and the independent Worker/Office/screen closure. Worker command/job decoders must remove only the old accidental `case_id = case_component_id` restriction, preserve exact native result identity, and accept WRITE_OFF's SOURCE_DEBT label. The original six issue codes, including INPUT_PENDING, remain required. UI must retain the original command bytes through unknown outcomes, obtain terminal date/results from the retained receipt/history, and use the existing RECOVERY protection facts without browser money arithmetic. The future moved-basis comparison/FC-13 connection is recorded in the investigation and is not implemented through a guessed current-channel predicate here.

At the initial review checkpoint registry mapping was pending. The current registry now names 1444 for all five reviewed owners; the catalogue amendment below records Root's local installation evidence. No complete FC-11 ITEM, application acceptance, payment execution, timings, fixtures or hosted deployment is asserted.

## Final static migration amendment

Root replaced the uninstalled migration after the source gate rejected the earlier dynamic/catalogue-aggregate form. The earlier migration hash `4c9b0600…cbb54547` is historical. The current hash above was read in full and remains **SOURCE PASS**: it targets only the fixed relation and exact verified predecessor CHECK, preserves the same nullable-origin semantics and retains transaction rollback on a missing/different constraint. The repeatable is unchanged at `0a3936fb…294cf0e66`. This amendment does not assert a business first use or new installation result.

## Root-attributed local catalogue amendment

Read `docs/bpay-next/design/FC-11-current-owner-catalogue.json`, SHA-256 `c7994202b0a3d46476c88f2270eccbd49f93c14333842252bc1f37c7dc75e971`, and the fresh saved `final-definer-closures.json`. The registry's five exact signatures all point to `supabase/repeatable/12102026_1444_banking_pay_next_case_write_off_v2.sql`. Root's sole-local-PG17 catalogue records five source-body MD5 matches, SECURITY INVOKER, original search paths, volatility `v`, mapped private owner and owner-only function ACLs. It records General `839|76|0||338226c4e9b878c5aa92fc8e319dd7bf`, public SECURITY DEFINER membership delta zero and the original General/Candidate isolation DO results.

The reviewer freshly rehashed the installed migration/repeatable source at the unchanged hashes in this receipt. Root supplied the compilation/catalogue/isolation evidence; the reviewer did not execute SQL or query that database. SQL **SOURCE PASS remains**. Compilation and metadata parity do not demonstrate a business first use, auth flow, payment, runtime allocation, screen or joined acceptance.
