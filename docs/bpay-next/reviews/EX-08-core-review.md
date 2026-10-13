# EX-08 independent core SQL review

11 October 2026. Read-only complete source review of the new1624 migration and repeatable, approved EX08 card/checklist, BD114 implementation boundary/design receipt and actual native parents. No SQL execution, database/business data, tests, fixtures, hosted actions, product edits or item/runtime PASS. Postgres skill used only for source lock/index/schema review.

**Current source verdict: PASS for the final1624 SQL and exact Worker seam recorded below.** Earlier checkpoints and corrected findings remain as review history. This is an independent source verdict, not SQL/runtime/item/application acceptance.

## Current essential findings

1. **Protected Case traversal is not bounded at its native seed.** Repeatable100–102 invokes `bpay_next_case_protected_bindings_v1(f.id,null,1001)` for every Case belonging to the Candidate and orders derived keys before the outer1001. The finite output guard118 does not bound traversal of thousands of closed Cases with no bindings. The current FC16 helper169 also orders qualified native bindings after its CTEs; its bounded output alone is not a bounded Candidate Case seed. Qualify the existing exact active-hold/pending accepted Case identities or use an explicit bounded indexed Candidate Case seed with an honest overflow refusal before lateral traversal. Do not silently truncate, recreate FC16 money logic or evaluate all Candidate history synchronously. This remains after root's correct NULL-limit→1001 fix.
2. **Informational Work recovery is read as zero.** Future CTE66 reads `i.nominal_source_ex_vat` for portions including RECOVERY74. Current0426 sets nominal to positive capacity at148–156, then emits a RECOVERY row at255–258 when recovery is positive and positive offer is zero. That row therefore has nominal zero and a real `recovery_source_ex_vat>0`. The new source can expose zero for a real native recovery and classify it UNCHANGED. The native Work collection/Case owns that debt. Omit this informational Work row from future payable scope or explicitly return its actual recovery evidence without presenting it as a payable zero/shared-conversion basis; do not create a second recovery formula.
3. **Exact native tuple FKs should be closed while schema is editable.** Member migration currently references approved line/segment and transfer by their ID alone. Existing immutable native unique keys support `(revision_id,approved_line_id)`, `(revision_id,approved_segment_id)` and transfer `(transfer_id,candidate_id)`. Use those exact native identity FKs rather than permit an unrelated line/segment revision or another Candidate's transfer in a retained scope. Also close null fields for each member kind. This is narrowly required provenance support, not a new relation/protocol. Do not FK retained Case revision to mutable current component_revision, which would block valid later changes; retained component ID plus captured numeric revision is appropriate.
4. **Protected Case key completeness requires preserving the actual binding.** Current94 constructs one key per run+component, while FC16 returns binding_kind/native_id and protected_amount. DISTINCT is over the full row, so two native bindings with different protected amounts but the same run/component can survive as duplicate member keys and later collide in the member PK. Preserve the actual native binding identity in the key (or explicitly prove/validate one such binding) rather than silently discard it. Do not invent a pooled amount. This is a source shape concern requiring a narrow decision; no runtime duplicate is claimed.

Root had already identified coarse protected Work amount duplication; it is not a new finding from this review. Protected transfer rows currently label channel/Umbrella from the frozen run worker. For mixed ONEOFF destination legs, keep those facts labelled as run-channel facts and use actual frozen transfer/destination identity for the creditor; never imply current or worker target Umbrella is the original creditor if native leg authority differs.

## Corrections verified in saved source

- Quote insertion now converts literal JSON null to SQL NULL (261). Initial body252 inserted JSON null into a schema CHECK allowing only SQL NULL/object, which would reject protected or needs-decision scopes. The defect is closed.
- Protected Work source amount is now honest NULL (84), removing whole-worker gross repeated per Work.
- Reimbursement and NOT_APPLICABLE paths now retain source principal/gross and quote NULL (123–125), rather than invent VAT0/target payment economics.
- FC16 helper receives finite1001 instead of NULL (100); Candidate source indexes were added. The remaining seed/derived-sort concern is separate.
- Preview remains STABLE/write-free, with closed cursor/20-row/64KiB result bound (148–179), and a context comparison after native scope. Root reported an accidental Apply lock insertion into Preview and removed it; no such lock is present in the reviewed saved preview.
- Fresh Apply discovers scope, preholds native Work → Case → components and revalidates context/scope before Candidate mutation (231–238).
- Same target channel/different Umbrella is admitted; exact no-op refused (229–230). No bank patch is accepted (204–208).

## Source behavior and security found sound within the stated boundary

Only two public service entrypoints are granted, both independently check service role, active native admin and NEXT. Private context/scope helpers are revoked from browser/service roles and invoked only by their definer owners. New tables are private, RLS+FORCE RLS, owner-only policy and no public/anon/authenticated/service table grants. Existing immutable trigger is applied for UPDATE/DELETE and TRUNCATE. Native request/cursor/page/context/result ceilings are explicit; no dynamic RPC/financial command/task/protocol or parked2153 dependency is added.

Operation UUID is the header PK. Request replay compares exact Candidate/actor/epoch/raw request including reason/context before current policy/target/currentness qualification (216–223); accepted identical retry returns the retained result after current Candidate changes. Changed target, reason/context or original identity conflicts. Header's post metadata_version and native METADATA task/generation establish accepted order without timestamp or random-UUID precedence. Child uniqueness is operation+native member key, with exact operation/Candidate FK.

Apply writes only current `candidates.pay_method,umbrella_id` (240). It does not invoke the legacy refresh, rewrite source money/rates/Contracts or alter protected Draft/Case/transfer/reissue money. Existing Candidate trigger supplies metadata/version/task once. Receipt and scope insert occur in that same transaction; failures roll back the current-fact change. No placeholder immutable header update is required.

Fresh context includes actual installation/actor/module epoch, current/target method+Umbrella, target VAT/enabled, financial and metadata revisions, Workbench/adoption revisions, current valuation policy and internal official-date operand. Preview does not display that pay date. Shared target quote1157 owns arithmetic; no Worker/browser amount or headroom arithmetic is introduced.

Current guarded Create1358 locks Candidate SHARE before Work/Case discovery and original admission; the new Candidate UPDATE serializes against its frozen target capture. The new Apply holds module → Candidate → control → Work → Case/component. It performs no Work acquisition after Case. Do not change original Create/financial lock order as a review workaround. Target Umbrella/policy SHARE guards and context revalidation precede the write; native metadata task keeps its existing lease/generation/cursor rules.

Contract-move comparison exclusion is correctly documented as absent native source; no fabricated relation/inferred Contract mismatch was introduced. Existing classifiers remain0426/1438/0559; EX08 retains its finite compatible/unresolved binding for later WB29/WB45/CM11 consumption. No item or future resolver completion is claimed.

## Reviewed snapshot / checks

Current raw SHA-256 values are appended below after saving this note. Parent/root alone may run authorized source gates/local compilation. This reviewer performed none. All application/queue/protected pay-channel journeys and A4 captures remain NOT_RUN.

## Additional current-context/classification findings

5. Context39–40 omits `workbench_revision.adoption_state='CURRENT'`. During UNSEEDED/SEEDING, maintained Candidate groups may be incomplete or absent; the reader can return zero affected members and Apply accept that incomplete scope. Require the actual existing adoption CURRENT fence, as current Create1358:562–565 does. This is a fixed state predicate, not a history census or new gate.
6. Automatic quote ELSE126 treats NULL/unknown tax treatment as taxable. Native approved_line.tax_treatment is nullable (core1324:202). The shared quote helper is arithmetic-only and does not certify classification (1157:8–9). Require positive approved TAXABLE authority before automatic ERNI conversion; unresolved/missing classification remains NEEDS_DECISION with NULL quote. Do not infer taxable policy from falling through the reimbursement branch.
7. **Accepted Create can change the native scope without changing this preview context.** Current guarded Create1358:552–559 captures the original target;567–570 accepts PREPARE and the descriptor. Its source does not bump Candidate financial/metadata or Workbench/adoption revision. Existing receive_command1404:38–54 advances `command_clock.last_sequence`. Context1624 currently does not include that native cut. Consequently a Create committed after Preview but before Apply can introduce protected Work/Case under an equal context; Apply231–238 compares its newly discovered scope only with its own locked reread, rather than the earlier Preview scope. Extend the exact raw context with the existing command clock last_sequence and retain native Candidate serialization/currentness validation. Check the final locked cut explicitly if the selected existing locking path requires it; do not add a new clock, digest, signing or protocol. The same operand closes cursor-page drift at that accepted-command boundary.
8. **New clock-before-Candidate acquisition in editable Apply inverts actual Create.** Latest Apply223 takes command_clock SHARE, then Candidate UPDATE224. Actual current private Create1358:187–194 takes Candidate SHARE, Work/revision/component SHARE205–225 and only then command_clock UPDATE231. Its outer public1358paye_guard_intake has no earlier clock lock. If Create holds Candidate SHARE and Apply holds clock SHARE, each waits for the other. Preserve the proven original Candidate-before-clock path or make an exact finite NOWAIT refusal at the conflict point; do not describe the intake as clock-before-Candidate or change existing Create ordering solely to accommodate EX08.
9. **Fresh native protection must exclude future quoting independently of projection timing.** Future1624:76–77 trusts maintained `i.accepted_run_id/is_selected`; Create1358 does not run a Workbench refresh. Classifier0426:158–163 sets those fields only when its owner later refreshes. Between accepted Create and that projection, a fresh scope can therefore contain both an F: automatic compatible quote and PW:/PC: protected identity for the same accepted Work/component. FC16 already discovers accepted Case descriptor identities immediately1509:18–23. Before quoting, exclude or protect those exact discovered native Work/component identities from the future CTE. The last-command cut closes change between calls but does not fix a fresh read inside this interval. Do not rewire Create/projection producers to solve this scope-only issue.

### Raw snapshot hashes

| File | SHA-256 |
| --- | --- |
| supabase/migrations/11102026_1624_banking_pay_next_pay_method_change_receipt.sql | D7D5203449912201DF726BFF02B81D00C219E75F9ACC90104150CCA32CC19BDB |
| supabase/repeatable/11102026_1624_banking_pay_next_pay_method_change_v1.sql | 4887564BFCB6D93B9897D610028A5E3B5AE60C22D082124DC638522BBB56AB69 |

These hashes identify the reviewed editable checkpoint. Later root edits require a narrow reread and appended final verdict/hash; the earlier defect history is retained.

## Narrow reread after root corrections

The original findings above are historical checkpoints. Current saved1624 source has the existing `last_command_sequence` in raw context30/34/39, adoption CURRENT39, positive TAXABLE qualification142, exact PC binding identity109, Work RECOVERY omission76 and native composite line/segment/transfer FKs. Its protected Case traversal now uses only maintained native CASE groups115–118, after the indexed1001-group overflow check54–55. This closes the unbounded all-Candidate-Case seed finding; native overall scope still refuses1001 members rather than truncating. No new history relation or protection money formula was introduced.

Apply takes Candidate/control before replay234–244, then existing clock SHARE NOWAIT248–250; contention is a closed context-unavailable refusal and cannot wait through the previously identified inverse. Exact accepted replay returns before this mutable clock/currentness qualification. Preview/context/scope are now VOLATILE **write-free** reads so post-wait/post-scope currentness sees fresh native state; the earlier STABLE description is historical. Existing private/public ACL boundaries remain unchanged.

**Remaining essential finding at this reread:** fresh Work protection is now checked directly at80–89, including exact native SUBSET selected line IDs. Fresh Case protection90 calls the existing helper with LIMIT1 before its outer component predicate, so only the first returned binding can exclude a future row. Later protected components can still be future quoted. Reuse the complete already bounded protected Case identities, or a complete bounded helper result with an honest overflow refusal, before the component predicate. This is the remaining component-specific form of finding9. No SQL/runtime acceptance is claimed.

Composite FK correction closes finding3's required provenance gap. Tightening unused null columns in private member CHECKs is optional hardening while editable; the only public writer already emits explicit typed nulls and table ACL prevents service/browser arbitrary inserts. This review does not require a new trigger or guard framework for those unused fields.

Editable reread hashes: migration `D7D5203449912201DF726BFF02B81D00C219E75F9ACC90104150CCA32CC19BDB`; repeatable `9020FE9508D958633EB8AA1EA83356B91523BDBE04119309FAC7CD465A1F222C`. **Source verdict remains corrections required solely for the remaining Case predicate; no item PASS.**

## Final current SQL closure

Final saved repeatable replaces the per-component LIMIT1 with one MATERIALIZED `case_bindings` read60–63 over the previously bounded native Candidate CASE groups. Both future94 and protected_case119 consume that same finite native result. Every pending accepted component is excluded before quoting; actual HOLD does not exclude a distinct native hold-subtracted available portion. Binding keys retain actual kind/native ID113. A1001-binding excess necessarily reaches the overall1001-result guard and refuses the whole scope136, so no truncated accepted scope or page is returned. Work's native ALL/SUBSET selected line exclusion84–93 preserves future unselected components and avoids double-classifying the accepted selection while projection catches up. Finding9 is closed.

Original Formula/Source/Case producers, accepted Create, native FC16 guard and money owners remain untouched. The new SQL changes only current Candidate channel/Umbrella, retains the exact decision+finite members and schedules the original METADATA task through the existing trigger. Raw context/replay, native currentness, clock NOWAIT, Work-before-Case preholds, ACL and exact request bounds reviewed above remain present. **Independent SQL source PASS; no essential unresolved finding.**

| Current file | SHA-256 |
| --- | --- |
| supabase/migrations/11102026_1624_banking_pay_next_pay_method_change_receipt.sql | D7D5203449912201DF726BFF02B81D00C219E75F9ACC90104150CCA32CC19BDB |
| supabase/repeatable/11102026_1624_banking_pay_next_pay_method_change_v1.sql | E05D8B9A751F83C6485A0E5D00826489D777483E0332A7FA50FCB6E18AAB2C4E |

## Exact Worker/route source review

Read the complete new `broker/src/bpay-next-pay-method-change.js` and exact index/fence changes plus their existing queue consumer. Both actual legacy GET/POST handlers retain original admin authentication and Candidate UUID qualification, then freshly read the native owner (`index.js:159343–159357`,159395,159670). Only explicit LEGACY falls through to original logic; unavailable/unknown/NEXT-disabled state refuses. The fence exemptions match exactly those two existing Candidate routes after original slash normalization (`bpay-next-legacy-fence.js:168–170`); NEXT cannot enter the legacy history refresh.

Adapter context/cursor/request/row/quote/receipt validators are closed, with native BIGINT counters as decimal strings, finite dates/money/bytes, exact actor/Candidate/target/context equality, ascending distinct page keys,1000 total members, exact next-cursor identity and category count conservation. Apply carries only the original operation/request, reason and raw context; no browser/Worker amount, bank data, pricing or all-page scope array enters the mutation. SQL is the sole change owner. Accepted identical replay result remains valid even after later current channel changes.

RPC transport selects only the two named fixed RPCs, validates the existing secret header without logging it, refuses redirect/invalid base URL, streams UTF-8 JSON up to64KiB and applies4s Preview/8s mutation deadlines. Request read is capped8KiB/4s. Promises are awaited or caught; no global request state, connection retention, platform binding/config/secret changes or unbounded response read is added. The narrow review uses the [current official Workers practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/) for bounded reads and asynchronous handling; no new platform API/type or configuration was introduced.

Only a validated accepted receipt sends `{id: task_id, kind: 'WORKBENCH'}` through the existing queue. Current queue allowlist and background CLAIM→STEP owner already accept this exact native identity. A missed wake does not convert a committed mutation into failure; existing durable task scanning owns recovery. Known finite SQL refusal codes return sanitized409/403; unavailable/malformed/timeout after an attempted mutation is explicit OUTCOME_UNKNOWN with the original operation UUID. No automatic financial retry or fresh UUID is introduced. Unknown outcome handling in the future Office caller remains its own review boundary.

**Independent Worker/route source PASS; no essential unresolved finding.** No execution/type checks/tests/fixtures/business actions, SQL compilation, queue journeys or A4 screens were performed by this reviewer. Those remain NOT_RUN here.

| Current Worker file | SHA-256 |
| --- | --- |
| broker/src/bpay-next-pay-method-change.js | 4A09461F35B62C1CC0C2C05B5D974E408FFD076E60199FA3AC7BEEE03E153432 |
| broker/src/bpay-next-legacy-fence.js | 0A7E0F87AF882E1D0D39A98498FED7814BBDAD77C444AC9B0F9058BC4C5F24B8 |
| broker/src/index.js | 96E6692E52FC85CEB6FB60646BD5E369865DD6E6975D15DD002AC77574C4F3B0 |

## Locked migration executable-owner / ACL gate supplement

Independently read the exact new diffs and surrounding implementation in `scripts/cloudtms-db-release-lib.mjs` and `scripts/bpay-next-source-gates.mjs`. Root reported the initial local load refused42501 because the new quoted owner/policy were not mapped; this reviewer did not perform or observe a SQL load. The canonical migration is unchanged. A read-only LF-canonical hash calculation confirms `d7d5203449912201df726bff02b81d00c219e75f9acc90104150cca32cc19bdb`, with exactly one original owner literal and exactly one original policy literal.

Release mapper339–345 adds only this exact migration path and whole canonical hash, with fixed owner/policy literals changing audited logical `postgres` to `CURRENT_USER` in the executable copy. Original mode remains unchanged; provider mode still rejects every value other than CURRENT_USER112–116. Existing exact path resolution, canonical full-source hash387–389 and owner one-occurrence guards390–393 remain intact. Optional policy replacement394–398 adds the same one-occurrence checks against both original and generally mapped text. The policy pair is a constant in the private authority Map and is included in the existing exported executable-policy metadata369–373. Earlier authority entries receive no optional policy replacement. No migration bytes, canonical hashes/locks, arbitrary quoted SQL, unexpected role, source body, public/service grants or general provider-mapping rules are relaxed.

Gate allowance1741–1744 requires both the exact migration path/outer signature and equality to the **entire original ACL DO body** after the existing LF normalization/comment removal1524–1535. It admits exactly the two fixed private receipt/member names; seven fixed owner/RLS/revoke/policy/immutability statements; the existing immutable trigger function; and fixed suffixes. Changing a relation, role, trigger target, predicate, loop input or executable statement breaks that literal equality. The flag only qualifies DYNAMIC_SQL_UNCLASSIFIED1807 for that exact body. Other parts/statements and every other special-construct hazard still pass through the original scanner; no blanket SQL/file/history/callee waiver was added. Independent migration locking additionally preserves its complete canonical bytes.

**Supplement source PASS; no essential finding.** This is source reasoning and read-only hash/occurrence evidence, not an executed tamper test, source-gate run, compilation, database release or application acceptance. All such reviewer operations remain NOT_RUN.

| Supporting source | SHA-256 |
| --- | --- |
| scripts/cloudtms-db-release-lib.mjs | 22B3A5CD4AB5CA93F59F891C03187A6F5214E6F4BB99381E203700C52B413BBF |
| scripts/bpay-next-source-gates.mjs | 3F5491C0D2B746077C6E5A0C363AB65BBF3B487CE197D01F42693B9033762403 |
