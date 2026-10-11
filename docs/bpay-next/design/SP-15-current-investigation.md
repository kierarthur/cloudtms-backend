# SP-15 current investigation — sequence 113

11 October 2026. Independent read-only source investigation at the saved BUILD-BE/BUILD-FE worktrees. No product/checklist changes, SQL, business calls, tests, fixtures, rendering, hosted actions or commits were performed. This is an implementation investigation; joined application acceptance remains NOT_RUN.

## Authority and narrow result

Read the complete approved card `05_SOURCE_AND_PROTECTED_PAY.md:515–542`, every SP-15 checklist row at `IMPLEMENTATION_CHECKLIST.md:3331–3355`, the master chain `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:72,197,310,330,338`, the current SP-12 census and its Candidate boundary review. The card is not design-first. D01 requires existing targeted refresh after actual gate-input writes; D02 records its exact interface/commit and updates the census; D03/D05 reuse genuine Source routes, receipts and retry controls; D04 adds only the two owned messages and their real Timesheet link. D06 is superseded context, not a bespoke fixture/proof programme.

The current workbench interface is **private.bpay_next_workbench_refresh_work_v1(uuid) RETURNS void**, final physical `supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql:318–351`. Its classifier is now **private.bpay_next_workbench_refresh_work_core_v1(uuid)** in `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql`; do not reinstall the old 1252/1410/1438 classifier. The wrapper requires NEXT, takes Candidate control then Work locks, retains the existing bounded ORIGIN continuation, and refreshes linked Cases only through that existing seam. The current companion **private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[])** is also 1438; its existing envelope is 100 identities.

The existing **private.weekly_source_pay_query_admit_v2()** in `05102026_0044_bpay_next_source_pay_query_admission_v2.sql:10–22` remains the one operational try-key. Each fresh writer inherits/takes it before its original first business lock; SP-15 does not move first acquisition into a deep refresh helper. Replay returns before fresh writes/hooks. The existing original Rule-B cut, immutable selected Work/segment/query data, row kernel, Source calculations and producer ownership remain unchanged. F-25's parked producer/version/latch/certificate/resolver implementation is outside this task.

## Exact current writer seams

The final-definer registry, not the old card filename, selects these parents. A static scan found **no direct Banking refresh call in the Source query, Candidate comparator, Local decision, NEXT protected receipt or WAIT bodies below**. Inherited genuine publication refresh is distinguished separately.

| Exact signature / current physical parent | Actual admitted write and smallest hook seam |
| --- | --- |
| public.weekly_source_manual_review_open_v1(jsonb) — 05102026_0428_weekly_source_manual_review_commands_v2.sql:144 | Original replay 167–187; admission177 before command/family/review locks178/200/204. Actual new OPEN review218–229; refresh only that changed event's native Work after the insert/audit, before final return240. Already-open/no-write and exact command replay do not manufacture another mutation. |
| public.weekly_source_manual_review_resolve_v1(jsonb) — same0428:325 | Admission371 before command/review locks. Original accepted Source hash / lane-qualified protected receipt checks stay intact. Actual resolution499 and immutable command result precede final return; refresh the exact resolved event after evidence/state is complete. |
| private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) — same0428:249 | Existing admitted final-authority and genuine CURRENT CAS qualification267–307. Each actually contained later-report review resolves310–316. Add the exact event hook after its actual resolution; preserve original-upload/noncontained/provisional/replay exclusions. Both real Final owners already call this helper: finalisation1534:2736 and correct_final_source1534:2039. A successor here covers both calls without duplicating the outer engines. |
| public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) — 15092026_1534_weekly_source_query_delivery_v1.sql:642 | Existing admission684 precedes incident687. Actual incident resolution698–704 and immutable resolution events712–731. Hook only affected event identities after their full writes, retain original requested-set conservation734–736. |
| public.weekly_source_manager_review_respond_atomic_v1(jsonb) — same1534:988 | Existing admission1082 precedes original advisory1088. Actual response event1190–1196 and material resolution1210–1227 / waiting-control updates1228 onward need the affected event refreshed after complete business facts. Preserve the Candidate-correct exception1197–1209. Pure outreach bookkeeping is not a new gate producer. |
| public.weekly_source_candidate_response_submit_atomic_v1(jsonb) — same1534:2473 | Existing admission2573 before operation advisory2576. Original immutable Candidate response2694–2707; CANDIDATE_WRONG resolves2709–2727, other actual relevant incident controls remain original. Refresh exact affected native event after the complete accepted branch; preserve semantic request-set/hash/replay and Candidate session/identity checks. |
| public.weekly_source_query_sync_atomic_v1(jsonb) — same1534:4691 | Existing admission4771 before cycle4772. Actual incident insert4895, comparison/current pointer4968/4987 and genuine omitted-event resolution5066–5089. Cover both changed-present and changed-resolved event paths, not just the new-row branch. Unchanged comparisons/resolved-decision matches do not need a write hook. |
| private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone) — 15092026_2203_weekly_source_candidate_app_contract_v1.sql:1224 | Actual material incident/comparison insertion1755–1827, current pointer1828 and omitted-event resolution1867 onward. This inherited path must appear in the finite census. Either put only the post-write refresh here, with original admitted callers retained, or cover every original material caller with exact affected identities. Do not miss it by reviewing only Candidate response1534. |
| public.weekly_exceptional_pay_publish_next_v1(jsonb) — 04102026_0345_banking_pay_next_protected_source_v1.sql:429 | Admission545 precedes current root559. Existing WB05 staged-pair publication907–911 already reaches common projection. It precedes family/generation completion917–925 and native NEXT receipt926–930; query evidence can differ after those writes. Required final hook belongs after completed accepted evidence, using v_stage.work_id. No financial publisher/formula rewrite. SP09 later merges tax treatment onto this successor. |
| public.weekly_exceptional_pay_complete_local_v1(jsonb) — 04102026_1257_weekly_source_local_protected_decision_v1.sql:42 | Admission125 before family144. Actual completion evidence438–447 then nested manual resolution452–460; APPROVE/AMEND retain PROTECTED_PAY even pending. A new0428 resolution hook already covers that resolution; cover any additional material WAIT/receipt branch not represented by a resolution, after original complete evidence. Avoid refreshing before the accepted receipt exists. |
| public.weekly_exceptional_pay_wait_atomic_v1(jsonb) — 10102026_0821_banking_pay_next_protected_wait_permission_v1.sql:4 | Actual Office permission68/admission83 precede family87. WAIT event262–268, family lifecycle/control269–272 and run completion280–283; refresh afterward for the actual bound Work. Retain original complete replay and permissions. No financial calculation or new WAIT command. |

The Candidate comparator's true outer entries are the current SP12 successors **public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone)**, **public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamp with time zone)** and **public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamp with time zone)** in `10102026_0853_weekly_source_pay_query_entry_admission_v1.sql:8,310,405`; admission92/356/510 remains before original first locking context/advisory. Materialise calls comparator943; the existing prefinal recheck2203:2018 calls the same comparator from publication's already-admitted outer. No new public Candidate function or MyTMS release is needed.

Resolve affected Work through the **actual native Source family/booking/root and event Candidate/Contract/week tuple**, then the existing Work identity; no UUID guesses or whole-Candidate/all-agency refresh. Native Work has exact original_timesheet_id and booking_id uniqueness (`03102026_1324_banking_pay_next_core.sql:111–129`), with Candidate/week indexed994–995. Work absence before genuine publication is an honest no-op, not authority to seed new Work. Deduplicate genuine affected Work within an operation and preserve Candidate→Work ordering of the existing wrapper. Do not collect an unbounded agency array or add a new continuation/proof framework merely for these hooks. Existing Source iteration may have many admitted events; hook only that original operation's actual changed event/family membership and respect the wrapper's native bounded continuation.

## Already-built refresh and semantic boundaries

WB02 withdrawal is already current1332:419–427 (financial revision followed by exact Work refresh), and ordinary withdrawal1332:352–360. WB05 common publisher1346 marks Processing before its original clock and terminal APPLIED replacement refreshes426–435; staged-pair1403 retains finite Contract/Client preholds and existing publication order. These are reuse evidence, not new SP15 writer rewires. Source OPEN/RESOLVE and post-receipt/control refresh remain separate necessary facts.

The Source gate remains the original five fields only. Display reason/actor/time/query identity come from Source-owned **private.weekly_source_pay_query_open_display_v1**, current `10102026_0956_weekly_source_pay_query_open_display_v1.sql:73–86,117–119`, when detail opens; never extend the gate with display fields or fetch on a tick. It reads actual manual review office_reason, opened_by_user_id, opened_at_utc and current evidence qualification. Its emitted UTC instant is displayed in UK DD/MM/YYYY hh:mm:ss through the existing formatter.

Owner directions override the card's older joined-acceptance wording where necessary: a query opened after original Create does not rewrite/block that captured Draft; E2 release of specified pre-confirm failures belongs the existing DR05 system-release mechanics rather than an invented held REVIEW state. SP14's named shift-only exclusion remains current2223; do not turn one incident into a blanket Candidate Timesheet hold, revive missing-Timesheet hold, or alter recovery. WB41 retains its separately owned unauthorised-WAIT recovery rule. This is a documented authority reconciliation, not permission to broaden SP15.

## Candidate STOP-CHECK

The start sheet §1/§2 (lines5,34) explicitly preapproves card-required frozen-boundary changes and requires their logs; no second chat approval is needed. B2.22 still requires five protected-file byte checks **and semantic preservation**. SP12-boundary-review.md:13–36 identifies the precise source entries and scope. Recheck saved protected-boundary-lock.json and all five declared files before a Candidate successor; preserve public signatures/configuration/ACL, session/environment/account/workflow/generation/actor checks, request fingerprints and same-key collisions, original current comparison, immutable signatures and original Source/TSFIN/invoice facts. Do not modify the lock to excuse a change. Actual MyTMS retry consumer remains SP36, and Source push approval remains SP28.

## Worker and Office scope

Existing `broker/src/weekly-source/routes.js:18–29` routes OPEN_MANUAL_REVIEW, RESOLVE_MANUAL_REVIEW and QUERY_SYNC to the genuine existing RPCs. Keep authenticated actor injection, permissions, original query/protected orchestration and closed busy mapping. No new Worker route/command is required. The OPEN0428 fresh body writes review/audit/command facts, not money/invoice; exact replay returns retained result before fresh admission. Local1257:451–460 resolves APPROVE/AMEND as PROTECTED_PAY with the original accepted approval/generation IDs, including pending receipt; no Finish query action.

Current BUILD-FE `js/weekly-source/workspace-actions.js:922–960` generates one RESOLVE command_id927 outside the retry handler and repeatedly posts the same payload943 with actor/frame protection. OPEN is the existing `openManualReview`1047–1077; its first pending payload1068 retains command_id and reason and is posted1069 for subsequent explicit retry. Upload row action1014 hides Send to Pay Queries when pay_query_open===true. Preserve actual Source workspace red-row/remedy behavior; source evidence is not proof of deployed/joined acceptance.

Current `js/bpay-next-office.js:202` STATUS already types review_issue_code/review_issue_work_id, but its worker table1988 still collapses nonnull codes to “Review required,” and selected worker details1989 onward do not render the two owned messages or link. Add the sole closed-code table entries:
- SOURCE_PAY_QUERY_OPEN: “Pay Query open on this Timesheet — resolve it in Weekly Source Office Checks”.
- SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE: “Pay Query evidence could not be confirmed — check the Timesheet's queries”.

DR15 later extends that same table; do not build its whole Review screen now. Owner one-sentence copy directions apply to any resulting inline errors. Open Timesheet must use the **native review_issue_work_id** and its exact physical Timesheet identity through existing native read/opener, refusing missing/ambiguous evidence. Do not pass Work UUID as Timesheet UUID. Existing qualified Timesheet opener `js/main.js:7185–7204` is a Workbench issue caller using physical identity, actor/NEXT/frame context and rejects accepted_run_id; reuse its actual opener/guards with the separately qualified review subject rather than weakening the accepted-run guard. No new money/read protocol or technical identifier display.

## Screen references and later acceptance

The read-only design index maps:
- **SCR142** (SP15/WB07/SP14) to root `png/SCR-08b_issue_detail_pay_query_dark.png`, `png/T04b_action_required_dark.png` and matching root specs. These show Pay query open and actual who/when/Source navigation.
- **SCR45 / SCR45b** (DR15/SP14/SP15), `batches/B4/png/SCR-45_review_reasons_dark.png`, phone counterpart and `SCR-45b_reason_wording_dark.png`; SCR44 supplied variants are shared later Draft Review context, not a new screen mandate at SP15.

Use the existing native screen/Source workspace and actual owned data; supplied designs add no fields or future DR15/Source workflow. Any UI change requires the normal source/actual-screen policy review, with differences explained; no render/spec edits were made here.

Future real acceptance retains original card checks: paid£100 unaffected/no invoice/debt; query row reason/actor/UK time; admitted new£20 blocked then Ready after remedy; only genuine later Final containing the same event clears; original command lost-reply replay; Candidate response and protected WAIT receipt update only affected NEXT Work; both-order OPEN/PIN busy/replay/eventual progress. All remain NOT_RUN in this investigation. D06's superseded bespoke programme must not be resurrected.

## Current saved source fingerprints

| File | SHA-256 |
| --- | --- |
| final-definer-closures.json | bba34606fe384dc8f28d649a575cb5145905fc1521a6ca97d045f4692879084d |
| SP-12-census.md | fa08495510178cfc00b15874e74b37c87e79748f6ea4e12c5bd69bf3182ae71a |
| 0428 manual commands | 1e9f16241ffa5e9249a7e8bf796693b81b633c2268cd1864147223451628e8d6 |
| 1534 query delivery | dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d |
| 2203 Candidate contract | 2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6 |
| 0853 Candidate admission successors | abc874b2d4ed90529ad591ea08a0a5131a6bc32c86109911631c8516e18b2e94 |
| 0345 protected NEXT | 0ea79ae265cffb2370926e1728f483b8846e7539ffcac428f5d5fa557a56ac44 |
| 1257 Local decision | 2a4f0d619ec2800f3d5a880459d0c0421f41c890bb6706750eb0464cab24fb03 |
| 0821 WAIT successor | 47f7f1befc74f5e5fc8e891d1e8b5b199ac3467ff047ef4555226be89a96f6a0 |
| 1438 current refresh wrapper | b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5 |
| weekly-source/routes.js | 3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11 |

No installed runtime or hosted version is inferred from these saved bytes. Final builder must save SP-15-refresh-hook.md with actual successor signatures/physical file/commit, exact gating and admission/lock rows, update every touched current registry chain and append actual SP12 census rows. Do not claim ITEM/App-tested completion from this investigation.

### Final static boundary checkpoint

All five files declared in the current protected-boundary-lock.json were freshly read and SHA-256 matched their declared hashes. This is saved-file evidence only, not an executed security verifier or operational permission. No protected byte was changed. Fresh BUILD-FE hashes: Office `64f8eafd29c1eb002a8f445860282f61eaf9c314f1c292a2dd862203aca2798d`; workspace-actions `66345020ef21fdb2b71db996b8d9fbff100f1bbc9d4694dc2659b60cab5b416a`; import-workspace `7d4621851bf5c5e5b14c0dbd2f149291a16f34ae050778a952cfc203855d9bb1`. Root's concurrent PF08 completion may legitimately advance Office bytes; re-read immediately before SP15 product editing.

## Bounded identity-helper and hook amendment

11 October 2026, follow-up read-only design check. **Recommended narrow support: private.bpay_next_source_query_work_id_v1(p_contract_id uuid,p_event_id uuid) RETURNS uuid, STABLE, owner-only.** This is an identity mapper only; its changed-event callers perform the actual NEXT refresh. No Work creation, Source financial publication, money calculation, producer protocol, new command/table or admission key is added. NEXT gating belongs the existing original writer (or the helper's plain module read); non-NEXT returns NULL/no hook. Keep immutable replay before fresh hooks.

### Exact native mapping

- Read **public.weekly_work_events** by event PK: id, candidate_id, client_id, work_date. The schema1534:535–552 has no Contract/week or Banking Work ID. Read **public.contracts** by p_contract_id PK and require native Contract Candidate/Client equal event Candidate/Client with IS DISTINCT FROM/null-safe checks. Contract comes from the writer's original accepted comparison/review/family/membership, never a browser guess.
- Resolve the existing **public.contract_weeks** primary row: contract_id=p_contract_id, additional_seq=0, is_adjustment IS FALSE, **week_ending_date BETWEEN event.work_date AND event.work_date+6**. This uses that event's own stored native week, including old omitted incidents; current Sync cycle finalisation_week_ending is not their week. Select at most two to detect overlapping native primary weeks rather than choosing latest. Saved current contract records unique **uq_contract_week(contract_id,week_ending_date,additional_seq)** and existing **idx_cw_contract_we(contract_id,week_ending_date)** / **idx_cw_timesheet(timesheet_id)**; no new point index is needed.
- Read the primary row's **timesheet_id** by PK. Use its exact **raw booking_id**, with native Contract/week/WEEKLY-HOURS/nonadjustment identity consistent. The existing Source root identity owner **private.weekly_source_resolve_root_identity_v1(uuid)** (17092026_0200:456) provides raw/trim collision and multiple-current qualification if needed. Do not choose a root using MAX, newest version or history. Root identity is distinct from authorisation eligibility.
- Resolve **private.bpay_next_work** by **booking_id = root.booking_id**, its unique key (03102026_1324:127–128), then require work_kind='SOURCE' and exact event Candidate/p_contract_id/primary week. It returns at most one actual Work. The Work's **original_timesheet_id** is the original member of that same raw booking family; it must **not** equal the current primary CW.timesheet_id after legitimate physical rotation. Both original_timesheet_id and booking_id are unique at1324:127–128.
- Do not match **work_revision.source_event_id** to p_event_id. That unique column is a Source approval/publication event (1324:138–141), not the durable shift event. Do not require an exceptional target family for an ordinary Source Work: 0057:70–73 explicitly permits ordinary Source publication with no exceptional family.
- A positively resolved root/booking with no Banking Work is a genuine **no-Work no-op**, return NULL; imported/not-yet-published events need not have Work. A missing/ambiguous primary/root when a corresponding native Work already exists must not be described as proved no-Work or silently mapped to an arbitrary Candidate Work. Retain existing native integrity qualification/refusal if that case occurs; no guessed fallback. No candidate/all-agency inventory is needed.
- **Do not call pay_query_scope_v1 as a positive hook eligibility test.** Its revoked/noncurrent/authorisation conditions0057:29–41 can return NULL precisely when existing Work needs an unavailable/blocked refresh. Map identity, then leave eligibility, accepted H/R, original PIN and OR47 to the unchanged current classifier2223.

Root's proposed helper is smaller than a combined refresh helper and fits the explicit card. It reads only existing native identities and returns no financial/display/certificate envelope. All private owners/ACL/final chain/callee additions must be exact.

### Changed-branch native operands

| Original writer/branch | p_event_id | p_contract_id |
| --- | --- | --- |
| OPEN0428 new review | (v_source->>'work_event_id')::uuid | (v_source->>'contract_id')::uuid; original resolver132–136 supplies the exact date/week |
| RESOLVE0428 actual resolution | v_review.work_event_id | v_review.contract_id |
| later-Final0428 cleared row | v_review.work_event_id | v_review.contract_id |
| accept-hours1534 changed incident | v_incident.work_event_id | v_comparison.contract_id; original current comparison696–697 |
| manager1534 material response | v_incident.work_event_id | v_comparison.contract_id |
| Candidate response1534 material response | v_incident.work_event_id | v_comparison.contract_id; accepted current comparison2619–2620 |
| query_sync1534 changed-present row | v_event.id | v_contract_id; native source link/resolution validates4857–4868 |
| query_sync1534 omitted-event resolution | v_incident.work_event_id | its exact original comparison.contract_id from omission join5009–5010: select that scalar or point-read its current comparison; do not reuse the unrelated last v_contract_id |
| Candidate comparator2203 changed/omitted row | v_work_event.id / v_incident.work_event_id | v_membership.contract_id, accepted Candidate/Client/Contract/week1355–1360 |
| protected NEXT0345 completed receipt | direct **v_stage.work_id** already owned | no event-to-root round trip needed; refresh after native receipt/family/run completion |
| Local1257 / WAIT0821 material completion | v_approval.work_event_id / v_event_id | v_family.contract_id; its exact root/raw booking/week is already qualified |

Protected family schema supplies unique(agency_id,candidate_id,contract_id,week_ending_date), unique btrim(root_family_booking_id), and root PK index at plan6_schema2083–2126. Local/WAIT may directly point-resolve that already-owned raw booking to exact Work rather than rereading the event. Preserve their original family/root tuple, no late fresh financial authority.

### Bounded collector versus scalar refresh

**Prefer the root's bounded Work-ID collector and existing refresh_many_v1(work_ids,'{}'::uuid[])**, over scalar refresh per incident. Add only a newly resolved nonnull Work absent from the current local batch; when it reaches100, flush after those branches' complete actual business writes and reset, then flush the final remainder before return. Deduplicate/sort at the existing owner boundary. This retains at most100 UUIDs, not an agency/Candidate/event inventory, changes no original Source iteration or request limit, and permits legitimate original Final/query_sync operations with more than100 events/Works. A Work repeated in another flushed batch is safe recomputation; card does not require global exactly-once refresh or an unbounded dedup set.

Existing wrapper1438:360–366 enforces100 input identities/16KiB and no null IDs. It sorts **Candidate control→Work→related Case** at374–381, uses its own bounded linked-Case lookup368–373 and native ORIGIN queue387–395 if the related projection exceeds100; no new continuation is needed. Passing a direct one-Work array is equally bounded for OPEN/RESOLVE/WAIT; gathering100 Work UUIDs is not permission to gather100 Candidates' entire Work arrays.

Per-loop refresh with a bounded already-seen array100 is financially correct if each call follows complete original business writes, but causes avoidable repeated Work replacement after the seen window is reset and acquires locks in incident iteration order. The collector reuses sorted native owner order per batch and is the smaller predictable implementation. Global ordering across an arbitrarily large original Source operation is not newly claimed: original Source admission try-key remains held and serialises participating Source writers before their Source rows, while each original Banking operation retains its existing Candidate scope/order. Do not introduce a new lock service/protocol.

Source writers originally lock Source command/incident/event/family rows; refresh adds only their admitted post-write Candidate→Work→Case projection seam. Protected NEXT0345/common publisher already holds its actual Candidate/Work and original clock before later completed receipt facts; do not take new public root/family locks after that clock or re-run the publisher. Use its known exact Work and existing wrapper. Final source review must inspect those precise late-call preimages/held locks; this amendment does not claim runtime contention proof.

The mapper/collector leave current2223 classification, original selected input/query pin and OR47 exclusion unchanged. No schema/index/money/protocol deliverable beyond the explicit targeted hook is identified.


