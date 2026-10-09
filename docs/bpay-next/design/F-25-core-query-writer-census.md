# F-25 core Query writer census

Saved 10 October 2026; requested F-25 build lane retains the 20261009 identities. Source-only inspection of the current saved BUILD-BE tree. Root alone writes product. No SQL, runtime, tests, fixtures, browser, hosted read, or product mutation was performed.

## Result

The first three gate relations have **14 material SQL statements in eight current physical routines**: six true public Query owners and two nested helpers. There is **no additional unclassified material writer** in the managed `supabase` source scan. Current action/reconciliation/delivery-only incident statements do not change the canonical gate tuple and must return before ownership assertion/staging. The final Source/Candidate outer lifetimes still need the already identified producer entry/finish work; this census is not proof that those product successors are implemented or installed.

Scan covered explicit INSERT/UPDATE/DELETE/MERGE on these three relation names in `.sql`/`.inc` under `supabase`, including quoted/unqualified spellings. The only additional verification-script statements are existing source fixtures; they are excluded from product owners and were not run. No current DELETE/MERGE writer was found. Historical definitions are excluded by exact final registry or the later physical same-signature definition, rather than counted as separate owners.

## Canonical fields and identity

| Relation | Primary key / genuine scope | Material captured tuple | Excluded fields |
|---|---|---|---|
| `private.weekly_source_manual_reviews` | `id`; OPEN uniqueness `(source_group_id,work_event_id)`; explicit Candidate/Client/Contract/work_date | `source_group_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,work_date,state` | Office reason, actor/timestamps alone. Original open/resolve command and Final witness remain genuine cause/completion evidence. |
| `public.weekly_discrepancy_incidents` | `id`; episode `(source_group_id,work_event_id,episode_number)`; OPEN uniqueness `(source_group_id,work_event_id)` | `candidate_id,client_id,work_event_id,source_cycle_id,source_group_id,state,current_comparison_revision_id` | `candidate_action_state,manager_potential_state,manager_action_state,reconciliation_state,waiting_source_state`, resolved timestamps and resolution prose/kind alone. `issue_family` is not an incident column. |
| `public.weekly_issue_comparison_revisions` | Immutable `id`; `(incident_id,revision_number)`; unique `material_comparison_fingerprint` | `incident_id,revision_number,contract_id,issue_family,material_comparison_fingerprint` plus append identity. Original fingerprint binds comparison operands; preserve genuine referenced rows. | Created timestamp alone; no new full-row/finance/prose snapshot. Existing append-only guard/ACL still forbids modifying/deleting old comparison evidence. |

Fields above follow `F-25-query-transaction-protocol.md:70–72`. Do not widen the incident tuple to outreach flags merely because a function is called Query. Initial incident INSERT (null current pointer), immutable comparison INSERT, then incident pointer UPDATE are three genuine separate event ordinals; never coalesce them into a fabricated single transition.

## Material routines and exact writer statements

Physical abbreviations:

- **M** = `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql`
- **Q** = `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql`
- **C** = `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql`
- **D** = `supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql`

| Current routine/signature | Exact statements | Classification and true owner |
|---|---|---|
| `public.weekly_source_manual_review_open_v1(jsonb)` M:144 | M:218 INSERT manual review OPEN | True `MANUAL_OPEN`. Pure command/hash replay precedes entry; original command receipt M:233–239 completes before owner finish. Already-open unchanged review emits no fake gate event. |
| `public.weekly_source_manual_review_resolve_v1(jsonb)` M:325 | M:499 UPDATE review state RESOLVED | Direct `MANUAL_RESOLVE`; also nested under Local source completion. Original resolve evidence/immutable command receipt completes before actual outer finish. |
| `private.weekly_source_manual_reviews_finalised_v1(uuid,uuid)` M:249 | M:310 UPDATE review state RESOLVED | Nested stage-only helper; true `SOURCE_FINAL` / `SOURCE_CORRECT_FINAL` owner, actual CURRENT CAS required M:268–277. It is not an independent synthetic manual RESOLVE. |
| `public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)` Q:642 | Q:698 UPDATE incident state RESOLVED | True `SYSTEM_HOURS_ACCEPT`. Incident selection is original requested IDs; final owner is after original discrepancy events/outreach/audit, not per incident loop. |
| `public.weekly_source_manager_review_respond_atomic_v1(jsonb)` Q:988 | Q:1210 UPDATE incident state RESOLVED | True `MANAGER_RESPONSE`. Only resolving branch changes gate; Q:1229/1234 do not. Exact immutable response replay/key/hash stays before admission. |
| `public.weekly_source_candidate_response_submit_atomic_v1(jsonb)` Q:2473 | Q:2709 UPDATE incident state RESOLVED | True `CANDIDATE_RESPONSE` when direct; nested under `CANDIDATE_APP_SUBMIT`. Q:2731 alone is not material. App true root owns complete materialise/compare/response lifetime. |
| `public.weekly_source_query_sync_atomic_v1(jsonb)` Q:4691 | Q:4895 incident INSERT; Q:4968 comparison INSERT; Q:4987 incident pointer UPDATE; Q:5066 incident state RESOLVED | True `QUERY_SYNC` when direct. Actual projection/work-event/Contract scope and each observed transition retained. Existing resolved-decision/unchanged comparison branches emit no fake versions. |
| `private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz)` C:1224 | C:1755 incident INSERT; C:1805 comparison INSERT; C:1828 incident pointer UPDATE; C:1867 incident state RESOLVED | Nested material compare helper. True app-submit/materialise/self-submit or Source upload→prefinal-recheck root. It cannot acquire a second clock or finish a parent lifetime. |

The six direct Query owner kinds already exist in 2352/2357. The two private material helpers need no invented additional financial command. They stage under an exact inherited admitted owner. If the implementation deliberately enters a nested kind, it must use an actual finite allowed parent edge and restore the stack; bare xid8 coincidence is insufficient.

## Non-material current incident writers

| Current routine | Statements | Fields changed / disposition |
|---|---|---|
| `public.weekly_source_timesheet_submission_complete_atomic_v1` | Q:521 | candidate_action_state NOT_REQUIRED; skip before stage/owner assertion. |
| `public.weekly_source_timesheet_submission_complete_atomic_v1` | Q:542 | manager_action_state DUE; skip before stage/owner assertion. |
| `public.weekly_source_manager_review_respond_atomic_v1` | Q:1229 | reconciliation_state,manager_action_state,waiting_source_state; skip before stage/owner assertion. |
| `public.weekly_source_manager_review_respond_atomic_v1` | Q:1234 | reconciliation_state,manager_action_state,waiting_source_state; skip before stage/owner assertion. |
| `public.weekly_source_query_scheduler_tick_v1` | Q:2448 | manager_action_state DUE; skip before stage/owner assertion. |
| `public.weekly_source_candidate_response_submit_atomic_v1` | Q:2731 | candidate_action_state,manager_potential_state,manager_action_state,waiting_source_state/reconciliation operational branch; skip before stage/owner assertion. |
| `public.weekly_source_query_send_manager_now_atomic_v1` | Q:3060 | candidate_action_state NOT_REQUIRED; skip before stage/owner assertion. |
| `public.weekly_source_query_send_manager_now_atomic_v1` | Q:3108 | manager_action_state DUE; skip before stage/owner assertion. |
| `private.weekly_source_query_manager_generation_v1` | Q:3793 | manager_action_state,waiting_source_state,reconciliation_state; skip before stage/owner assertion. |
| `private.weekly_source_query_candidate_generation_v1` | Q:4322 | candidate_action_state ASKED; skip before stage/owner assertion. |
| `private.weekly_source_delivery_aggregate_command_v1` | D:1309 | manager_action_state SENT; skip before stage/owner assertion. |

In particular Q:3793 `private.weekly_source_query_manager_generation_v1(uuid,text,timestamptz,boolean)` resets only operational flags; it does **not** change `incident.state` or pointer. Q:4322 `private.weekly_source_query_candidate_generation_v1(uuid,uuid,uuid,uuid,text,uuid,timestamptz)` asks the Candidate without changing the gate tuple. Their callers (send-now, ask-now, scheduler, cohort restart and contact rebuild) are not missing material owners for these first three relations. Qualification writers on the other sixteen relations need their own census.

**Important historical dispatch exclusion:** Q:1713 belongs to the old dispatch-result body at Q:1634. The same signature is replaced later in Q:5229 and is finally registered to D:240 via `28092026_1234_stage2_hosted_final_authority_reassert_v1.sql`. Do not port Q:1713 as a current writer. The actual current delivery target aggregate D:1220 writes only `manager_action_state='SENT'` at D:1309. Registry names that aggregate's final physical D and final root1234.

## Actual Candidate/root/incident FK closure before the agency clock

The 2357 stage function performs no business discovery. It accepts `(owner_relation,owner_row_id,candidate_id,root_timesheet_id,old_present,new_present,old_tuple,new_tuple)` and adds event/version/ownership rows. Its real Candidate/root FK inserts and 2352 optional tail-target FK inserts still take referential locks. Acquire/stage their exact existing referenced targets in the original business phase; a tail registration is not proof a lock was held.

| Mutation | Candidate authority | Root authority | Original referenced targets and preclock rule |
|---|---|---|---|
| Manual OPEN INSERT | Actual resolved source JSON M:221–224, from `weekly_source_manual_review_source_v2`/row-resolution authority; verify Candidate matches work-event and Contract. | No `root_timesheet_id` column exists on manual review. Default event scalar root is NULL unless the true outer already has one exact canonical root from current approved source identity. A family found by Contract/Candidate/week is optional and must be unambiguous; do not invent a root by latest row. | Original INSERT references source group, cycle, upload row, row resolution, work event, Candidate, Client, Contract, opening user. M:202–204 existing OPEN lock precedes INSERT; M:211–215 optional family count rejects >1. Stage Candidate FK during preclock business phase. |
| Manual RESOLVE / Final nested UPDATE | Existing locked review `candidate_id`, Client, Contract, work_date/work_event; M:396 exactreview or M:283–287 contained Final match. | No review root; NULL is honest absent scope. For protected resolution, genuine family `root_timesheet_id` used by existing Local/C1/NEXT receipt checks M:477–479 may be reused only after exact reviewed family identity validation. | UPDATE resolves user FK in addition to review state; retain original actor/reference closure. Final nested helper matches current Final/source cycle/client manifest/contained row and different opening upload M:278–307 before real state transition; original Final true root must retain that completion witness. |
| Incident INSERT in Sync / Candidate compare | Explicit `v_event.candidate_id` Q:4900 or `v_generation.candidate_id` C:1760, matched by canonical work event + Client/Contract/source group. | No root on incident. NULL until actual parent has exact canonical root; do not look up a new Timesheet after Source clock. | Original FK targets source group, work event, Candidate, Client, cycle. Actual work event lock Q:4881/C:1740; existing OPEN incident lock Q:4886–4889/C:1745–1749. Stage knows Candidate from original INSERT, not a trigger-side SELECT. |
| Comparison INSERT | The exact parent incident's Candidate and scope, inherited from owner-local locked record; comparison itself has no Candidate column. | `candidate_timesheet_id` is a genuine optional reference but may identify a Candidate Timesheet rather than prove a canonical financial root. Only normalize/certify through existing original root authority **before** clock; otherwise keep scalar root NULL and retain actual comparison/timesheet identity via its original fingerprint/evidence. | Original INSERT FKs: incident, projection publication, comparison upload, optional Final, candidate Timesheet, source upload row, work-event source link, Contract. All original values Q:4968–4986 / C:1805–1827 must be resolved/preheld before a postclock tail; incident ID must already be allocated/inserted by this true owner. Neither review nor incident is a Timesheet ID. |
| Incident current pointer UPDATE | Existing incident Candidate; owner verifies source work event/Client and comparison Contract. | Same actual parent canonical root if already certified, otherwise NULL. | New comparison `id` is original FK target and must already be inserted/owned. OLD/NEW incident pointer tuples each refer to real immutable revisions. Incident ownership/candidate scope is captured from original locked owner record, not a late new incident lookup. |
| Incident RESOLVED updates | Original locked incident Q:687/Q:1116/Q:2615/Q:5064/C:1865. | Same absent/certified-root rule. | Current comparison remains exact; original selected episode/scope/fingerprint check remains. These UPDATEs do not create a new incidental root. Resolution completion retains original event/request witness; do not snapshot live counters. |

Schema authority: manual migration `02102026_1600_weekly_source_manual_review.sql:3–29`; incident/comparison migration `15092026_1534_weekly_source_plan6_schema.sql:1416–1473`. `private.weekly_source_pay_query_scope_v1(uuid)` in `05102026_0057_weekly_source_pay_query_scope_v1.sql:9–80` starts from an already supplied canonical root and verifies original Contract/Candidate/week/family; it is not permission to infer a root from an unrelated review UUID. Stage's nullable Candidate/root fields are not permission to drop a known Candidate. Manual/incident mutations have an exact known Candidate and should pass it.

**Preclock branch handling:** ordinary standalone core writers stage events before their sole metadata clock at genuine outer finish. Nested Final/upload/local Source writers that reach these mutations after original Source command receive must either move that original receive to the complete business tail or certify each actual existing Candidate/root/incident/comparison/other FK target and finite row/operation before first clock, then use the reserved original producer ordinal range. Never solve missing scope with a row-hook SELECT FOR UPDATE, late module/admission, second clock or `SET CONSTRAINTS ALL`. If the exact postclock incident/comparison IDs cannot be known/preallocated and closed before original Source receive, refuse that unsupported branch honestly; do not invent prior rows.

## Final-definition and scan exclusions

- Registry `docs/bpay-next/final-definer-closures.json` selects both manual public signatures at M, excluding old `02102026_1601_weekly_source_manual_review_v1.sql:79/211`.
- Manual finalised helper and material Query/compare routines have one physical definition in the bounded saved repeatable source inventory. The registry has no competing newer material physical definition for these signatures. Existing managed includers/reassertors do not create a different body just because they reinstall the same source.
- Dispatch/aggregate final physical is D, exact final root1234. Q has two dispatch definitions; old mutation1713 is excluded.
- Existing `supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql:1297/1306/1314` are verification fixture statements, excluded and not run/copied.
- No product writer executes UPDATE/DELETE against comparison append evidence in this scope. Adding a hook never authorizes such mutations.

## Exact current per-statement inventory

| Physical path | Line | Owner routine | SQL operation | Relation | Classification |
|---|---:|---|---|---|---|
| `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql` | 218 | `public.weekly_source_manual_review_open_v1` | INSERT INTO | `private.weekly_source_manual_reviews` | material |
| `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql` | 310 | `private.weekly_source_manual_reviews_finalised_v1` | UPDATE | `private.weekly_source_manual_reviews` | material |
| `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql` | 499 | `public.weekly_source_manual_review_resolve_v1` | UPDATE | `private.weekly_source_manual_reviews` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 521 | `public.weekly_source_timesheet_submission_complete_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 542 | `public.weekly_source_timesheet_submission_complete_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 698 | `public.weekly_source_query_accept_system_hours_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 1210 | `public.weekly_source_manager_review_respond_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 1229 | `public.weekly_source_manager_review_respond_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 1234 | `public.weekly_source_manager_review_respond_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 2448 | `public.weekly_source_query_scheduler_tick_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 2709 | `public.weekly_source_candidate_response_submit_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 2731 | `public.weekly_source_candidate_response_submit_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 3060 | `public.weekly_source_query_send_manager_now_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 3108 | `public.weekly_source_query_send_manager_now_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 3793 | `private.weekly_source_query_manager_generation_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 4322 | `private.weekly_source_query_candidate_generation_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 4895 | `public.weekly_source_query_sync_atomic_v1` | INSERT INTO | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 4968 | `public.weekly_source_query_sync_atomic_v1` | INSERT INTO | `public.weekly_issue_comparison_revisions` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 4987 | `public.weekly_source_query_sync_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 5066 | `public.weekly_source_query_sync_atomic_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 1755 | `private.weekly_source_candidate_submission_compare_sync_v1` | INSERT INTO | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 1805 | `private.weekly_source_candidate_submission_compare_sync_v1` | INSERT INTO | `public.weekly_issue_comparison_revisions` | material |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 1828 | `private.weekly_source_candidate_submission_compare_sync_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 1867 | `private.weekly_source_candidate_submission_compare_sync_v1` | UPDATE | `public.weekly_discrepancy_incidents` | material |
| `supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql` | 1309 | `private.weekly_source_delivery_aggregate_command_v1` | UPDATE | `public.weekly_discrepancy_incidents` | irrelevant |

Current physical count: **25** statements = **14 material** + **11 irrelevant**. The excluded two historical manual statements, one superseded dispatch statement and three verification statements are recorded above, not counted as current product writers.

## Raw source provenance

| Saved path | Raw bytes | SHA-256 |
|---|---:|---|
| `supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql` | 38249 | `1e9f16241ffa5e9249a7e8bf796693b81b633c2268cd1864147223451628e8d6` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 268078 | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 149249 | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| `supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql` | 67803 | `2623374220b3c7ebb14699428a280bf8bbbd1f87dce5371072fe9e1cf35d62e6` |
| `docs/bpay-next/final-definer-closures.json` | 710491 | `7b252755f20f0dc42d59eb0f145b0d86533ba077ac11d85e27d5ba1621b31f1d` |
| `supabase/migrations/02102026_1600_weekly_source_manual_review.sql` | 2231 | `cadac3950ea836a35005eecf1f0b064bdf935d90d6f8b74f2a3377b7ae0afce4` |
| `supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql` | 229113 | `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d` |
| `supabase/repeatable/05102026_0057_weekly_source_pay_query_scope_v1.sql` | 4718 | `18317d4152fa8e8df4c17ac7895046453a3e26d534b10c45b1fafb50b91a983d` |
| `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` | 19936 | `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1` |
| `supabase/repeatable/09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` | 15973 | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |

2352/2357 are current saved uninstalled product drafts, inspected only to bind this census to their actual signatures/FKs/owner enum. This document does not promote those drafts or comparison packs to installed authority. Re-read changed files immediately before root implements any successor.
