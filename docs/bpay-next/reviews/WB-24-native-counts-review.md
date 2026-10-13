# WB-24 native complete-count support review

2026-10-11. **PASS — source review; no essential findings in the reviewed BE count support.** This review covers the unloaded0135 SQL successor, matching Worker/FE closed count fields and current final-definer registration. It does not certify the full FE selection reducer, app journeys, compilation, installation or deployment.

## Scope and sources

The full WB24 card `02_WORKBENCH.md:931–950`, R2-F06 `review/R2.md:120–124`, R2 WB24:236–238 and all WB24 checklist rows were read for the current investigation. D04 is the sole implementation deliverable. Card943 explicitly requires complete server `selectable_item_count` and `selectable_case_component_count` for CANDIDATES/CANDIDATE_GROUPS. The new fixed counter response is indispensable support for the existing screen; it creates no Worker route, local write protocol or duplicate shared-owner deliverable.

Reviewed current saved files:

- `supabase/repeatable/11102026_0135_banking_pay_next_selection_complete_counts_v1.sql`, all86lines.
- Original row helper and sole choice-unit helper in `10102026_2306_banking_pay_next_actor_selection_memory_v1.sql:83–130/761–835`; current0121 LEAF owner retained.
- `broker/src/bpay-next-workbench.js:400–404/438/567–576`; semantic diff against current HEAD ignores only line-ending conversion.
- FE `js/bpay-next-workbench.js:402–406/440/568–577`, for closed response compatibility only.
- `docs/bpay-next/final-definer-closures.json`, exact row-helper chain and adjacent choices/LEAF/common-page chains.

## Findings

**Original helper preserved.** Source comparison of the complete original2306 row function and0135 successor shows only two new count variables, two additional `count(*) filter(...)` aggregates/INTO targets, and two fixed output fields for CANDIDATES/CANDIDATE_GROUPS. Candidate narrowing, original Client/channel scope, Work/Case/native component filtering, default/selected/new-changed flags, eligibility/payable0 rule, all native selection_basis operands, size guard and existing helper invocation remain unchanged. The new file defines only the existing exact row signature, keeps STABLE SECURITY INVOKER and search_path, then applies the original private ownership/ACL posture.

**Complete native count semantics.** SQL0135:22–28 obtains all five counts from the same single existing `selection_memory_choices_v1(v_scope,p_baseline,null)` relation. Work split is `work_id is not null`; Case split is `case_component_id is not null`. Therefore the split applies to exactly the same current eligible scope as existing choice_member_count. It does not count the visible page, presentation groups, nominal money or all raw segment LEAFs. Original choices2306:88–95 groups native units by Candidate/Work/revision/approved_line/component_key/direction or native Case component; this grouping is unchanged. A frontend must consume returned per-line complete counts rather than assume one unit for every line. The final LEAF stays0121 and all money-bearing eligibility/projection logic remains its current owner.

**Output shape is scoped correctly.** SQL0135:77–81 adds split integer-string fields only for CANDIDATES/CANDIDATE_GROUPS. GROUP_LINES and EXPORT_ROWS retain their exact prior shape; the null-native GROUP_LINES early return remains compatible. Zero eligible membership produces split0/0, selected0, defaultfalse and eligiblefalse. Candidate payable0 still cannot become selection-eligible merely because deductions/Case units exist. The split is independent of current remembered selection; choice_selected_count remains the selected baseline counter used with local bounded overrides.

**Closed Worker and FE contract matches.** Worker404 and FE406 require both new fields only on CANDIDATES/CANDIDATE_GROUPS. Exact row shaping rejects missing/extra fields. Their existing integer classifier accepts canonical nonnegative decimal integer strings through signed BIGINT max. Worker571 and FE572 require Work split + Case split = choice_member_count using BigInt. Existing selected≤total, default-selected equivalence, zero-membership eligibility/new-changed and exact native basis validation remain intact. GROUP_LINES/EXPORT_ROWS are not accidentally required to carry the new fields. The semantic Worker diff is only these two additions; other displayed changes are CRLF normalization.

**Single registered current owner.** Registry row-helper chain now has finalRoot/finalPhysical/finalClosures0135; allowedPhysical contains original2306 and0135, with original2306 the predecessor. The original choice helper remains final2306, LEAF final0121, and common kind-page reader final0113. No second independent member-count framework is registered or added. Current common reader invokes the same row helper for these native rows; its paging/context/intake semantics are unchanged.

**No money/source/authority rewires.** The only new computation is membership counting. Original monetary basis operands are copied unchanged, not reinterpreted or calculated. This support does not edit source producers, Rule B capture, preparation, allocation kernels, locks, live balances, immutable accepted membership, journal retries, public commands or memory exclusions. SQL84–85 keeps owner postgres and revokes all function privileges from public/anon/authenticated/service_role; no private relation or function gains browser/service-role access. It introduces no table or migration.

## Evidence limits

0135 was supplied as unloaded. This reviewer performed only saved-source reads/comparison and wrote this Markdown. No tests, DB calls, fixtures, hosted service, commit, deployment or product write occurred. Root must perform the required source/sole PostgreSQL compile-contract-coupling checks and record the actual outcome. Full WB24 FE behavior has a separate reviewer. Joined app journeys remain NOT_RUN and installation is not inferred from this PASS.

## Snapshot hashes

| Reviewed source | SHA-256 |
| --- | --- |
|0135 complete-count SQL|`5f8d2e1cc3ad80460c840aac028cb76a499f3de2a7496b721bd8c7369ed9ba85`|
|BE broker workbench|`034a74748451a237ba9a193ab74fb336e293fbef1ed9712acb2b7a5d693c6985`|
|Final-definer registry|`1310fd8e89adcccd56c05899f97821bf80fb056dd1f88df97ded80280ac2459b`|
|FE workbench (closed-field review only)|`9977c5daadab83493915a0d19a14aaf04e6c9615963bc304f28dc0720b358c5e`|
